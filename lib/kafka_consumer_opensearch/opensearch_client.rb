# frozen_string_literal: true

require 'opensearch'

class OpenSearchClient
  INDEX_NAME = 'wikimedia'

  def initialize(config)
    @config = config

    puts "OpenSearch client initialized: #{client.cluster.health}"

    create_index unless index_exists?
  end

  private

  attr_reader :config

  def index_exists?
    client.indices.exists?(index: INDEX_NAME)
  end

  def create_index
    response = client.indices.create(
      index: 'wikimedia',
      body: {
        settings: {
          index: {
            number_of_shards: 4
          }
        }
      }
    )
    puts response
  end

  def client
    @client ||= OpenSearch::Client.new(
      url: config.opensearch_url,
      retry_on_failure: 5,
      request_timeout: 120,
      # log: true,
      transport_options: { ssl: { verify: false } },
      user: 'admin',
      password: ENV.fetch('OPENSEARCH_INITIAL_ADMIN_PASSWORD'),
    )
  end
end
