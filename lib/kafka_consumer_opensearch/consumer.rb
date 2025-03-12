# frozen_string_literal: true

module KafkaConsumerOpenSearch
  class Consumer

    TOPIC = 'wikimedia.recentchange'
    INT32_MAX = (2**31 - 1).freeze

    def initialize(config)
      @config = config
      # Reference: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
      @kafka_config = {
        'bootstrap.servers' => config.bootstrap_server,
        'group.id' => 'opensearch-consumer'
      }
    end

    def consume!
      consumer.subscribe(TOPIC)

      consumer.each do |message|
        puts "Message received: #{message}"
      end
    end

    def open_search_client
      @open_search_client ||= OpenSearchClient.new(config)
    end

    def self.consume!
      new(KafkaConsumerOpenSearch.config).consume!
    end

    private

    attr_reader :config, :kafka_config

    def consumer
      @consumer ||= Rdkafka::Config.new(kafka_config).consumer
    end
  end
end
