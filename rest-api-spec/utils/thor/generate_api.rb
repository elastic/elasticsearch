require 'thor'

require 'pathname'
require 'active_support/core_ext/hash/deep_merge'
require 'active_support/inflector/methods'
require 'rest_client'
require 'json'
require 'pry'

module Elasticsearch

  module API
    module Utils
      # controller.registerHandler(RestRequest.Method.GET, "/_cluster/health", this);
      PATTERN_REST = /.*controller.registerHandler\(.*(?<method>GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH)\s*,\s*"(?<url>.*)"\s*,\s*.+\);/
      # request.param("index"), request.paramAsBoolean("docs", indicesStatsRequest.docs()), etc
      PATTERN_URL_PARAMS = /request.param.*\("(?<param>[a-z_]+)".*/
      # controller.registerHandler(GET, "/{index}/_refresh", this)
      PATTERN_URL_PARTS  = /\{(?<part>[a-zA-Z0-9\_\-]+)\}/
      # request.hasContent()
      PATTERN_HAS_BODY   = /request\.hasContent()/

      # Parses the Elasticsearch source code and returns a Hash of REST API information/specs.
      #
      # Example:
      #
      #     {
      #       "cluster.health" => [
      #         { "method" => "GET",
      #           "path"   => "/_cluster/health",
      #           "parts"  => ["index"],
      #           "params" => ["index", "local",  ... ],
      #           "body"   => false
      #     }
      #
      def __parse_java_source(path)
        path  += '/' unless path =~ /\/$/ # Add trailing slash if missing
        prefix = "src/main/java/org/elasticsearch/rest/action"

        java_rest_files = Dir["#{path}#{prefix}/**/*.java"]

        map = {}

        java_rest_files.sort.each do |file|
          content = File.read(file)
          parts   = file.gsub(path+prefix, '').split('/')
          name    = parts[0, parts.size-1].reject { |p| p =~ /^\s*$/ }.join('.')

          # Remove the `admin` namespace
          name.gsub! /admin\./, ''

          # Extract params
          url_params = content.scan(PATTERN_URL_PARAMS).map { |n| n.first }.sort

          # Extract parts
          url_parts = content.scan(PATTERN_URL_PARTS).map { |n| n.first }.sort

          # Extract if body allowed
          has_body  = !!content.match(PATTERN_HAS_BODY)

          # Extract HTTP method and path
          content.scan(PATTERN_REST) do |method, path|
            (map[name] ||= []) << { 'method' => method,
                                    'path'   => path,
                                    'parts'  => url_parts,
                                    'params' => url_params,
                                    'body'   => has_body }
          end

        end

        map
      end

      extend self
    end

    # Contains a generator which will parse the Elasticsearch *.java source files,
    # extract information about REST API endpoints (URLs, HTTP methods, URL parameters, etc),
    # and create a skeleton of the JSON API specification file for each endpoint.
    #
    # Usage:
    #
    #     $ thor help api:generate:spec
    #
    # Example:
    #
    # time thor api:generate:spec \
    #             --force \
    #             --verbose \
    #             --crawl \
    #             --elasticsearch=/path/to/elasticsearch/source/code
    #
    # Features:
    #
    # * Extract the API name from the source filename (eg. `admin/cluster/health/RestClusterHealthAction.java` -> `cluster.health`)
    # * Extract the URLs from the `registerHandler` statements
    # * Extract the URL parts (eg. `{index}`) from the URLs
    # * Extract the URL parameters (eg. `{timeout}`) from the `request.param("ABC")` statements
    # * Detect whether HTTP body is allowed for the API from `request.hasContent()` statements
    # * Search the <http://elasticsearch.org> website to get proper documentation URLs
    # * Assemble the JSON format for the API spec
    #
    class JsonGenerator < Thor
      namespace 'api:spec'

      include Thor::Actions

      __root = Pathname( File.expand_path('../../..', __FILE__) )

      # Usage: thor help api:generate:spec
      #
      desc "generate", "Generate JSON API spec files from Elasticsearch source code"
      method_option :force,     type: :boolean, default: false,            desc: 'Overwrite the output'
      method_option :verbose,   type: :boolean, default: false,            desc: 'Output more information'
      method_option :output,    default: __root.join('tmp/out'),           desc: 'Path to output directory'
      method_option :elasticsearch, default: __root.join('tmp/elasticsearch'), desc: 'Path to directory with Elasticsearch source code'
      method_option :crawl,     type: :boolean, default: false,            desc: 'Extract URLs from Elasticsearch website'

      def generate
        self.class.source_root File.expand_path('../', __FILE__)

        @output = Pathname(options[:output])

        rest_actions = Utils.__parse_java_source(options[:elasticsearch].to_s)

        if rest_actions.empty?
          say_status 'ERROR', 'Cannot find Elasticsearch source in ' + options[:elasticsearch].to_s, :red
          exit(1)
        end

        rest_actions.each do |name, info|
          doc_url  = ""
          parts    = info.reduce([]) { |sum, n| sum |= n['parts']; sum }.reduce({}) { |sum, n| sum[n] = {}; sum }
          params   = info.reduce([]) { |sum, n| sum |= n['params']; sum }.reduce({}) { |sum, n| sum[n] = {}; sum }

          if options[:crawl]
            begin
              response = RestClient.get "http://search.elasticsearch.org/elastic-search-website/guide/_search?q=#{URI.escape(name.gsub(/\./, ' '))}"
              hits = JSON.load(response)['hits']['hits']
              if hit = hits.first
                if hit['_score'] > 0.2
                  doc_title = hit['fields']['title']
                  doc_url   = "http://elasticsearch.org" + hit['fields']['url']
                end
              end
            rescue Exception => e
              puts "[!] ERROR: #{e.inspect}"
            end
          end

          spec     = {
            name => {
              'documentation' => doc_url,

              'methods' => info.map { |n| n['method'] }.uniq,

              'url' => {
                'path'   => info.first['path'],
                'paths'  => info.map { |n| n['path'] }.uniq,
                'parts'  => parts,
                'params' => params
              },

              'body' => info.first['body'] ? {} : nil
            }
          }

          json = JSON.pretty_generate(spec, indent: '  ', array_nl: '', object_nl: "\n", space: ' ', space_before: ' ')

          # Fix JSON array formatting
          json.gsub!(/\[\s+/, '[')
          json.gsub!(/, {2,}"/, ', "')

          create_file @output.join( "#{name}.json" ), json + "\n"

          if options[:verbose]
            lines = json.split("\n")
            say_status 'JSON',
                       lines.first  + "\n" + lines[1, lines.size].map { |l| ' '*14 + l }.join("\n")
          end
        end
      end

      private

    end
  end
end
