#!/usr/bin/env ruby

require "phobos"

class FooProducer
  include Phobos::Producer
end

Phobos.configure({
    logger: {
      file: false,
      level: "info",
      ruby_kafka: {
        level: "warn",
      },
    },
    kafka: {
      client_id: "foo-producer",
      seed_brokers: [
        "127.0.0.1:9092",
        "127.0.0.1:9093",
        "127.0.0.1:9094",
      ],
    },
    producer: {},
  })
srand(Time.now.to_i)

PARTITION_TO_KEY = [ "7", "2", "1" ]
done_by_partition = [ 0, 0, 0 ]

loop do
  0.upto(2) do |p|
    messages = 0.upto(rand(5)).map do |n|
      done_by_partition[p] += 1
      {
        topic: "random_numbers",
        payload: (done_by_partition[p] - 1).to_s,
        key: PARTITION_TO_KEY[p],
      }
    end
    FooProducer.producer.publish_list(messages)
  end

  sleep 10
end
