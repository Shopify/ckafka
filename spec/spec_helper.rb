$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'ckafka'
require 'poseidon'
require 'json'

module KafkaIntegrationTestHelper
  def assert_kafka_offset_difference(value, topic: "test.1", partition: 0, wait: 0.5, total_attempts: 20, &block)
    offset_before = consumer(topic: topic, partition: partition).next_offset
    yield

    attempts = 0
    begin
      sleep(wait)
      offset_after = consumer(topic: topic, partition: partition).next_offset
      attempts += 1
    end while attempts <= total_attempts && offset_after - offset_before < value

    expect(value).to eq(offset_after - offset_before)
  end

  def latest_kafka_event
    JSON.parse(consumer(offset: -1).fetch.first.value, symbolize_names: true)
  end

  # Do not cache the instance to make sure it receives the latest offset again.
  # The topic metadata API doesn't expose it, so we have to resert to this.
  def consumer(offset: :latest_offset, topic: "test.1", partition: 0)
    Poseidon::PartitionConsumer.consumer_for_partition('kafka-shopify-test-consumer', ['localhost:9092'], topic, partition, offset)
  end
end
