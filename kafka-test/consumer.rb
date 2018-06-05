#!/usr/bin/env ruby

require "kafka"

client = Kafka::new([
    "127.0.0.1:9092",
    "127.0.0.1:9093",
    "127.0.0.1:9094",
  ])

