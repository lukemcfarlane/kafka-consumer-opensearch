# frozen_string_literal: true

module KafkaConsumerOpenSearch
  class << self
    def config(&block)
      @config ||= Configuration.new
      yield @config if block_given?
      @config
    end
  end
end
