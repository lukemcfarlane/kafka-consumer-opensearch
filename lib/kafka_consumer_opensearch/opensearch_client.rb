# frozen_string_literal: true

require 'opensearch'

class OpenSearchClient
  def initialize(config)
    @config = config

    puts "OpenSearch client initialized: #{client.cluster.health}"
  end

  private

  attr_reader :config

  def client
    @client ||= OpenSearch::Client.new(
      url: config.opensearch_url,
      retry_on_failure: 5,
      request_timeout: 120,
      log: true
    )
  end
end
