### Purpose

This plugin provides empty REST and transport endpoints for bulk indexing and search. It is used to avoid accidental server-side bottlenecks in client-side benchmarking.

### Build Instructions

Build the plugin with `gradle :client:client-benchmark-noop-api-plugin:assemble` from the Elasticsearch root project directory.

### Installation Instructions

After, the binary has been built, install it with `bin/elasticsearch-plugin install file:///full/path/to/noop-plugin.zip`.

### Usage

The plugin provides two REST endpoints:

* `/_noop_bulk` and all variations that the bulk endpoint provides (except that all no op endpoints are called `_noop_bulk` instead of `_bulk`)
* `_noop_search` and all variations that the search endpoint provides (except that all no op endpoints are called `_noop_search` instead of `_search`)

The corresponding transport actions are:

* `org.elasticsearch.plugin.noop.action.bulk.TransportNoopBulkAction`
* `org.elasticsearch.plugin.noop.action.search.TransportNoopSearchAction`