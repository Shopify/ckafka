require 'spec_helper'
require 'json'

describe Ckafka do
  include KafkaIntegrationTestHelper

  describe '#produce' do
    before :each do
      Ckafka.init
      Ckafka.add_broker('localhost:9092')
    end

    after :each do
      Ckafka.close
    end

    it 'should produce multiple events' do
      assert_kafka_offset_difference(10, topic: 'test.1', partition: 0) do
        10.times { |i| Ckafka.produce('test.1', 'my_key', JSON.generate(foo: i)) }
      end
    end

    it 'should produce an event correctly' do
      assert_kafka_offset_difference(1, topic: 'test.1', partition: 0) do
        Ckafka.produce('test.1', 'my_key', JSON.generate(foo: :bar))
      end
      expect(latest_kafka_event[:foo]).to eq 'bar'
    end
  end
end
