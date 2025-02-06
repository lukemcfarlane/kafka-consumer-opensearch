# frozen_string_literal: true

module KafkaConsumerOpenSearch
  class Consumer

    TOPIC = 'wikimedia.recentchange'

    def initialize(config)
      @config = config
    end

    def consume!
      OpenSearchClient.new(config)
    end

    def self.consume!
      new(KafkaConsumerOpenSearch.config).consume!
    end

    private

    attr_reader :config, :kafka_config
  end
end
