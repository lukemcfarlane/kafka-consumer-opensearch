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

      puts "Subscribed to #{TOPIC}"

      consumer.each do |message|
        payload = JSON.parse(message.payload)
        payload.delete('log_params') # workaround for OpenSearch mapping issue
        id = payload.dig('meta', 'id')

        puts "Received message: #{id}"

        res = open_search_client.index(
          index: OpenSearchClient::INDEX_NAME,
          body: payload,
          id: id,
        )
        puts "#{res['result']} open search document with id: #{res['_id']}"
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
