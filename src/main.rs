extern crate abomonation;
extern crate differential_dataflow;
extern crate rdkafka;
extern crate timely;
extern crate timely_communication;

#[macro_use] extern crate abomonation_derive;
#[macro_use] extern crate error_chain;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::{ClientConfig, TopicPartitionList};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::rc::Rc;
use std::str::FromStr;
use std::time::Duration;
use timely::Data;
use timely::dataflow::operators::*;
use timely::dataflow::operators::generic::source;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::{InputHandle, Stream};
use timely::progress::Timestamp;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely_communication::Allocate;

error_chain! {
    types {
        KtError, KtErrorKind, KtResultExt, KtResult;
    }

    foreign_links {
        Kafka(KafkaError);
    }

    errors {
        UnknownTopic(topic: String) {
            description("Unknown topic"),
            display("Unknown topic: {:?}", topic),
        }
        TooManyCaps(desc: String) {
            description("Worker has too many capabilities"),
            display("{}", desc),
        }
    }
}

struct PartitionConsumer {
    consumer: BaseConsumer,
}

impl PartitionConsumer {
    fn new<S: Into<String>>(config: &ClientConfig, topic: S, partition: i32) -> KtResult<PartitionConsumer> {
        let topic = topic.into();
        let consumer = config.create::<BaseConsumer>()
            .map_err(|e| format!("Error creating consumer: {}", e))?;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(&topic, partition);
        consumer.assign(&tpl)
            .map_err(|e| format!("Error assigning topic partition to consumer: {}", e))?;

        Ok(PartitionConsumer {
            consumer: consumer,
        })
    }

    fn poll(&self) -> KtResult<Vec<BorrowedMessage>> {
        let mut rcvd_messages = Vec::new();

        loop {
            match self.consumer.poll(Duration::from_millis(100)) {
                Some(Ok(message)) => {
                    rcvd_messages.push(message);
                },
                Some(Err(KafkaError::PartitionEOF(..))) => {
                    break;
                },
                Some(Err(e)) => {
                    return Err(KtError::from(e));
                },
                None => {
                    break;
                }
            }
        }

        Ok(rcvd_messages)
    }
}

struct ConsumerBuilder {
    config: ClientConfig,
    topic: String,
    partition: i32,
}

impl ConsumerBuilder {
    fn new<S: Into<String>>(config: ClientConfig, topic: S, partition: i32) -> ConsumerBuilder {
        ConsumerBuilder {
            config: config,
            topic: topic.into(),
            partition: partition,
        }
    }

    fn build_for_worker(&self, index: i32, peers: i32) -> bool {
        self.partition % peers == index
    }

    fn build_consumer(&self) -> KtResult<PartitionConsumer> {
        PartitionConsumer::new(&self.config, self.topic.clone(), self.partition)
    }
}

struct KafkaPartitionSource<'a, A: Allocate, T: Timestamp, D: Data> {
    // topic: String,
    partition: i32,
    stream: Stream<Child<'a, Root<A>, T>, D>,
    cap: Rc<RefCell<Option<Capability<Product<RootTimestamp, T>>>>>,
    offsets: Rc<RefCell<Option<BTreeSet<i64>>>>,
    consumer: Rc<RefCell<Option<PartitionConsumer>>>,
    offset_handle: InputHandle<T, (i32, i64)>,
    offset_stream: Stream<Child<'a, Root<A>, T>, (i32, i64)>,
}

impl<'a, A, T, D> KafkaPartitionSource<'a, A, T, D>
    where A: Allocate,
          T: Timestamp,
          D: Data,
{
    fn new<L>(scope: &mut Child<'a, Root<A>, T>, builder: ConsumerBuilder, logic: L) -> KafkaPartitionSource<'a, A, T, D>
        where L: 'static + Fn(&BorrowedMessage) -> Option<(Product<RootTimestamp, T>, D)>,
    {
        let cap = Rc::new(RefCell::new(None));
        let offsets = Rc::new(RefCell::new(None));
        let consumer = Rc::new(RefCell::new(None));

        let worker_id = scope.index() as i32;
        let num_workers = scope.peers() as i32;

        let stream = source(scope, "KafkaPartitionSource", |src_cap| {
            let cap = cap.clone();
            let offsets = offsets.clone();
            let consumer = consumer.clone();

            if builder.build_for_worker(worker_id, num_workers) {
                *cap.borrow_mut() = Some(src_cap);
                *offsets.borrow_mut() = Some(BTreeSet::new());
                *consumer.borrow_mut() = Some(builder.build_consumer().expect("Could not build consumer"));
            }

            move |output| {
                if let (Some(ref cap), Some(ref mut offsets), Some(ref consumer)) =
                    (cap.borrow().as_ref(), offsets.borrow_mut().as_mut(), consumer.borrow().as_ref())
                {
                    let mut old_ts = cap.time().clone();

                    let messages = match consumer.poll() {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("Worker {}/{}: Error polling Kafka for messages: {}", worker_id + 1, num_workers, e);
                            Vec::new()
                        }
                    };

                    let mut processed = BTreeMap::new();
                    for message in messages {
                        match logic(&message) {
                            Some((timestamp, processed_message)) => {
                                processed.entry(timestamp)
                                    .or_insert_with(|| Vec::new())
                                    .push((message, processed_message));
                            },
                            None => {
                                println!("Dropping message with offset {}", message.offset());
                            }
                        }
                    }

                    for (block_ts, block) in processed {
                        if block_ts >= old_ts {
                            let block_cap = cap.delayed(&block_ts);
                            let mut session = output.session(&block_cap);
                            for (orig, processed) in block {
                                offsets.insert(orig.offset());
                                session.give(processed);
                            }
                        } else {
                            println!("Dropping a block of {} messages with a timestamp of {:?}, which is behind the current timestamp of {:?}", block.len(), block_ts, old_ts);
                        }
                    }
                }
            }
        });

        let (offset_input_handle, offset_stream) = scope.new_input::<(i32, i64)>();

        KafkaPartitionSource {
            partition: builder.partition,
            stream: stream,
            cap: cap,
            offsets: offsets,
            consumer: consumer,
            offset_handle: offset_input_handle,
            offset_stream: offset_stream,
        }
    }
}

struct KafkaTopicSource<'a, A: Allocate, T: Timestamp, D: Data> {
    stream: Stream<Child<'a, Root<A>, T>, D>,
    cap: Rc<RefCell<Option<Capability<Product<RootTimestamp, T>>>>>,
    offset_handle: InputHandle<T, (i32, i64)>,
    _offset_stream: Stream<Child<'a, Root<A>, T>, (i32, i64)>,
}

impl<'a, A, T, D> KafkaTopicSource<'a, A, T, D>
    where A: Allocate,
          T: Timestamp + From<u64>,
          T::Summary: From<u64>,
          D: Data,
{
    fn new<L>(scope: &mut Child<'a, Root<A>, T>, config: ClientConfig, topic: &str, logic: L) -> KtResult<KafkaTopicSource<'a, A, T, D>>
        where L: 'static + Clone + Fn(&BorrowedMessage) -> Option<(Product<RootTimestamp, T>, D)>
    {
        let worker_id = scope.index();
        let partition_ids = Self::partition_ids(&config, topic)
            .expect("Could not fetch partition ids");

        let mut partition_sources = Vec::new();
        for partition_id in partition_ids {
            let builder = ConsumerBuilder::new(config.clone(), topic, partition_id);
            let source = KafkaPartitionSource::new(scope, builder, logic.clone());
            partition_sources.push(source);
        }

        let streams = partition_sources.iter().map(|s| s.stream.clone()).collect::<Vec<_>>();
        let offset_streams = partition_sources.iter().map(|s| s.offset_stream.clone()).collect::<Vec<_>>();

        let active_sources = partition_sources.into_iter().filter(|s| s.cap.borrow().is_some()).collect::<Vec<_>>();

        let num_active = active_sources.len();
        if num_active > 1 {
            Err(KtError::from(KtErrorKind::TooManyCaps(format!(
                "Worker {}/{} has too many capabilities: {}. Do you need more worker threads?",
                scope.index() + 1, scope.peers(), num_active,
            ))))
        } else {
            let active_source = active_sources.into_iter().next().unwrap();
            let this_partition = active_source.partition;
            let offsets = active_source.offsets;
            let consumer = active_source.consumer;
            Ok(KafkaTopicSource {
                stream: scope.concatenate(streams),
                cap: active_source.cap.clone(),
                offset_handle: active_source.offset_handle,
                _offset_stream: scope.concatenate(offset_streams)
                    .broadcast()
                    // .inspect(move |(partition, offset)| {
                    //     println!("Worker {}: ({}, {})", worker_id, partition, offset);
                    // })
                    .filter(move |(partition, _)| *partition == this_partition)
                    .inspect_batch(move |time, psnos| {
                        if let (Some(ref mut offsets), Some(ref _consumer)) = (offsets.borrow_mut().as_mut(), consumer.borrow().as_ref()) {
                            for (partition, offset) in psnos {
                                println!(
                                    "Worker {}: {:?}: ({}, {}) this_partition = {} offsets = {:?} retiring",
                                    worker_id, time,
                                    partition, offset, this_partition,
                                    offsets,
                                );
                                offsets.remove(offset);
                            }
                        }
                    }),
            })
        }
    }

    fn partition_ids(config: &ClientConfig, topic: &str) -> KtResult<Vec<i32>> {
        let consumer = config.create::<BaseConsumer>()?;
        let metadata = consumer.fetch_metadata(None, Duration::from_millis(100))?;

        metadata.topics().iter().find(|t| t.name() == topic)
            .map(|topic| {
                topic.partitions().iter().map(|p| p.id()).collect::<Vec<_>>()
            })
            .ok_or(KtError::from(KtErrorKind::UnknownTopic(topic.to_string())))
    }
}

#[derive(Abomonation, Clone, Debug)]
struct DummyData {
    data: u64,
    partition: i32,
    offset: i64,
}

fn main() {
    let topic = "random_numbers";
    let mut client_config = ClientConfig::new();
    client_config
        .set("api.version.request", "false")
        .set("broker.version.fallback", "0.9.0.1")
        .set("auto.offset.reset", "latest")
        .set("group.id", "timely_kafka_test")
        .set("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
        .set("enable.auto.commit", "false");

    timely::execute_from_args(env::args(), move |worker| {
        let client_config = client_config.clone();
        let worker_id = worker.index();
        // let num_workers = worker.peers();

        worker.dataflow(move |scope| {
            let client_config = client_config.clone();
            let source = KafkaTopicSource::new(scope, client_config, topic, |message| {
                message.payload()
                    .map(|v| String::from_utf8_lossy(v))
                    .and_then(|s| u64::from_str(&s).ok())
                    .map(|n| (
                        RootTimestamp::new(n),
                        DummyData {
                            data: n,
                            partition: message.partition(),
                            offset: message.offset(),
                        },
                    ))
            }).expect("Unable to create source");
            let cap_holder = source.cap.clone();

            let mut retrier = source.offset_handle;

            source.stream
                .inspect_batch(move |ts, _| {
                    // Advance with allowed latness.
                    let opt_cap = cap_holder.borrow().clone();
                    if let Some(cap) = opt_cap {
                        let ts2 = cap.time().clone();
                        let cap_time = ts2.inner;
                        let batch_time = ts.inner;

                        let new_time = if batch_time > 5 {
                            ::std::cmp::max(batch_time - 5, cap_time)
                        } else {
                            cap_time
                        };

                        if new_time != cap_time {
                            let mut new_ts = ts.clone();
                            new_ts.inner = new_time;
                            let new_cap = cap.delayed(&new_ts);
                            *cap_holder.borrow_mut() = Some(new_cap);
                        }
                    }
                })
                .exchange(|x| x.data as u64 % 3)
                .inspect_batch(move |t, x| {
                    println!("Worker {}: {:?}: {:?}", worker_id, t, x);
                })
                .inspect_batch(move |t, xs| {
                    let epoch = *retrier.epoch();
                    retrier.advance_to(::std::cmp::max(t.inner, epoch));
                    for x in xs {
                        retrier.send((x.partition, x.offset));
                    }
                })
                ;
        });
    }).unwrap();
}
