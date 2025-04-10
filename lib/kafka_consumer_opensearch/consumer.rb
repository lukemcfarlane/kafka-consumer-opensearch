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
        'group.id' => 'opensearch-consumer',
        'enable.auto.commit' => false,
      }
    end

    def consume!
      # rdkafka-ruby documentation: https://karafka.io/docs/code/rdkafka-ruby/Rdkafka/Consumer.html

      consumer.subscribe(TOPIC)

      puts "Subscribed to #{TOPIC}"

      consumer.each_slice(500) do |messages|
        documents = []

        messages.each do |message|
          payload = JSON.parse(message.payload)
          payload.delete('log_params') # workaround for OpenSearch mapping issue
          id = payload.dig('meta', 'id')

          documents << { index: { _index: OpenSearchClient::INDEX_NAME, _id: id } }
          documents << payload
        end
        res = open_search_client.bulk(body: documents)

        results =  res['items'].map { |item| item.dig('index', 'result') }
        puts "Processed #{messages.count} messages #{results.tally}"

        consumer.commit
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
