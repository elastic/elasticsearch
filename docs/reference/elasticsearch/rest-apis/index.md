---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rest-apis.html
  - https://www.elastic.co/guide/en/serverless/current/elasticsearch-differences.html
applies_to:
  stack: ga
  serverless: ga
---

# REST APIs

Elasticsearch exposes REST APIs that are used by the UI components and can be called directly to configure and access Elasticsearch features.

For API reference information, go to [{{es}} API]({{es-apis}}) and [{{es-serverless}} API]({{es-serverless-apis}}).

This section includes:

- [API conventions](/reference/elasticsearch/rest-apis/api-conventions.md)
- [Common options](/reference/elasticsearch/rest-apis/common-options.md)
- [Compatibility](/reference/elasticsearch/rest-apis/compatibility.md)
- [Examples](/reference/elasticsearch/rest-apis/api-examples.md)

## API endpoints

### [Behavioral analytics](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-analytics)

```{applies_to}
stack: deprecated
```

The behavioral analytics APIs enable you to create and manage analytics collections and retrieve information about analytics collections. Behavioral Analytics is an analytics event collection platform. You can use it to analyze your users' searching and clicking behavior. Leverage this information to improve the relevance of your search results and identify gaps in your content.

| API | Description |
| --- | ----------- |
| [Get Collections](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-get-behavioral-analytics) | Lists all behavioral analytics collections. |
| [Create Collection](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-put-behavioral-analytics) | Creates a new behavioral analytics collection. |
| [Delete Collection](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-delete-behavioral-analytics) | Deletes a behavioral analytics collection. |
| [Create Event](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-post-behavioral-analytics-event) | Sends a behavioral analytics event to a collection. |

### [Compact and aligned text (CAT)](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-cat)

The compact and aligned text (CAT) APIs return human-readable text as a response, instead of a JSON object. The CAT APIs aim are intended only for human consumption using the Kibana console or command line. They are not intended for use by applications. For application consumption, it's recommend to use a corresponding JSON API.

| API | Description |
| --- | ----------- |
| [Get aliases](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-aliases) | Returns index aliases. |
| [Get allocation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-allocation) | Provides a snapshot of shard allocation across nodes. |
| [Get component templates](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-component-templates) | Returns information about component templates. |
| [Get count](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-count) | Returns document count for specified indices. |
| [Get fielddata](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-fielddata) | Shows fielddata memory usage by field. |
| [Get health](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-health) | Returns cluster health status. |
| [Get help](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-help) | Shows help for CAT APIs. |
| [Get index information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-indices) | Returns index statistics. |
| [Get master](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-master) | Returns information about the elected master node. |
| [Get ml data frame analytics](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-ml-data-frame-analytics) | Returns data frame analytics jobs. |
| [Get ml datafeeds](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-ml-datafeeds) | Returns information about datafeeds. |
| [Get ml jobs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-ml-jobs) | Returns anomaly detection jobs. |
| [Get ml trained models](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-ml-trained-models) | Returns trained machine learning models. |
| [Get nodeattrs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-nodeattrs) | Returns custom node attributes. |
| [Get node information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-nodes) | Returns cluster node info and statistics. |
| [Get pending tasks](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-pending-tasks) | Returns cluster pending tasks. |
| [Get plugins](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-plugins) | Returns information about installed plugins. |
| [Get recovery](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-recovery) | Returns shard recovery information. |
| [Get repositories](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-repositories) | Returns snapshot repository information. |
| [Get segments](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-segments) | Returns low-level segment information. |
| [Get shard information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-shards) | Returns shard allocation across nodes. |
| [Get snapshots](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-snapshots) | Returns snapshot information. |
| [Get tasks](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-tasks) | Returns information about running tasks. |
| [Get templates](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-templates) | Returns index template information. |
| [Get thread pool](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-thread-pool) | Returns thread pool statistics. |
| [Get transforms](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-transforms) | Returns transform information. |

### [Cluster](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-cluster)

The cluster APIs enable you to retrieve information about your infrastructure on cluster, node, or shard level. You can manage cluster settings and voting configuration exceptions, collect node statistics and retrieve node information.

| API | Description |
| --- | ----------- |
| [Get cluster health](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-health) | Returns health status of the cluster. |
| [Get cluster info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-info) | Returns basic information about the cluster. |
| [Reroute cluster](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute) | Manually reassigns shard allocations. |
| [Get cluster state](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-state) | Retrieves the current cluster state. |
| [Explain shard allocation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-allocation-explain) | Get explanations for shard allocations in the cluster. |
| [Update cluster settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) | Updates persistent or transient cluster settings. |
| [Get cluster stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-stats) | Returns cluster-wide statistics, including node, index, and shard metrics. |
| [Get cluster pending tasks](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-pending-tasks) | Lists cluster-level tasks that are pending execution. |
| [Get cluster settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-get-settings) | Retrieves the current cluster-wide settings, including persistent and transient settings. |
| [Get cluster remote info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-remote-info) | Returns information about configured remote clusters for cross-cluster search and replication. |
| [Update cluster voting config exclusions](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-post-voting-config-exclusions) | Update the cluster voting config exclusions by node IDs or node names. |
| [Delete voting config exclusions](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-delete-voting-config-exclusions) | Clears voting configuration exclusions, allowing previously excluded nodes to participate in master elections. |
| [Exclude nodes from voting](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-post-voting-config-exclusions) | Excludes nodes from voting in master elections. |
| [Clear voting config exclusions](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-delete-voting-config-exclusions) | Clears voting config exclusions. |

### [Cluster - Health](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-health_report)

The cluster - health API provides you a report with the health status of an Elasticsearch cluster.

| API | Description |
| --- | ----------- |
| [Get cluster health report](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-health-report) | Returns health status of the cluster, including index-level details. |

### [Connector](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector)

The connector and sync jobs APIs provide a convenient way to create and manage Elastic connectors and sync jobs in an internal index.

| API | Description |
| --- | ----------- |
| [Get connector](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-get) | Retrieves a connector configuration. |
| [Put connector](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-put) | Creates or updates a connector configuration. |
| [Delete connector](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-delete) | Deletes a connector configuration. |
| [Start connector sync job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-post) | Starts a sync job for a connector. |
| [Get connector sync job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-get) | Retrieves sync job details for a connector. |
| [Get all connectors](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-list) | Retrieves a list of all connector configurations. |
| [Get all connector sync jobs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-list) | Retrieves a list of all connector sync jobs. |
| [Delete connector sync job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-delete) | Deletes a connector sync job. |

The connector and sync jobs APIs provide a convenient way to create and manage Elastic connectors and sync jobs in an internal index.

| API | Description |
| --- | ----------- |
| [Get connector](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-get) | Retrieves a connector configuration. |
| [Put connector](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-put) | Creates or updates a connector configuration. |
| [Delete connector](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-delete) | Deletes a connector configuration. |
| [Start connector sync job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-post) | Starts a sync job for a connector. |
| [Get connector sync job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-get) | Retrieves sync job details for a connector. |

### [Cross-cluster replication (CCR)](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-ccr)

The cross-cluster replication (CCR) APIs enable you to run cross-cluster replication operations, such as creating and managing follower indices or auto-follow patterns. With CCR, you can replicate indices across clusters to continue handling search requests in the event of a datacenter outage, prevent search volume from impacting indexing throughput, and reduce search latency by processing search requests in geo-proximity to the user.

| API | Description |
| --- | ----------- |
| [Create or update auto-follow pattern](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-put-auto-follow-pattern) | Creates or updates an auto-follow pattern. |
| [Delete auto-follow pattern](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-delete-auto-follow-pattern) | Deletes an auto-follow pattern. |
| [Get auto-follow pattern](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-get-auto-follow-pattern) | Retrieves auto-follow pattern configuration. |
| [Pause auto-follow pattern](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-pause-auto-follow-pattern) | Pauses an auto-follow pattern. |
| [Resume auto-follow pattern](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-resume-auto-follow-pattern) | Resumes a paused auto-follow pattern. |
| [Forget follower](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-forget-follower) | Removes follower retention leases from leader index. |
| [Create follower](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-follow) | Creates a follower index. |
| [Get follower](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-follow-info) | Retrieves information about follower indices. |
| [Get follower stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-follow-stats) | Retrieves stats about follower indices. |
| [Pause follower](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-pause-follow) | Pauses replication of a follower index. |
| [Resume follower](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-resume-follow) | Resumes replication of a paused follower index. |
| [Unfollow index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-unfollow) | Converts a follower index into a regular index. |
| [Get CCR stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-stats) | Retrieves overall CCR statistics for the cluster. |

### [Data stream](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-data-stream)

The data stream APIs enable you to create and manage data streams and data stream lifecycles. A data stream lets you store append-only time series data across multiple indices while giving you a single named resource for requests. Data streams are well-suited for logs, events, metrics, and other continuously generated data.

| API | Description |
| --- | ----------- |
| [Create data stream](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create-data-stream) | Creates a new data stream. |
| [Delete data stream](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-data-stream) | Deletes an existing data stream. |
| [Get data stream](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-data-stream) | Retrieves one or more data streams. |
| [Modify data stream](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-modify-data-stream) | Updates the backing index configuration for a data stream. |
| [Promote data stream write index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-promote-data-stream) | Promotes a backing index to be the write index. |
| [Data streams stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-data-streams-stats) | Returns statistics about data streams. |
| [Migrate to data stream](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-migrate-to-data-stream) | Migrates an index or indices to a data stream. |

### [Document](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-document)

The document APIs enable you to create and manage documents in an {{es}} index.

| API | Description |
| --- | ----------- |
| [Index document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) | Indexes a document into a specific index. |
| [Get document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get) | Retrieves a document by ID. |
| [Delete document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete) | Deletes a document by ID. |
| [Update document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) | Updates a document using a script or partial doc. |
| [Bulk](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) | Performs multiple indexing or delete operations in a single API call. |
| [Multi-get document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-mget) | Retrieves multiple documents by ID in one request. |
| [Update documents by query](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query) | Updates documents that match a query. |
| [Delete documents by query](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete-by-query) | Deletes documents that match a query. |
| [Get term vectors](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-termvectors) | Retrieves term vectors for a document. |
| [Multi-termvectors](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-mtermvectors) | Retrieves term vectors for multiple documents. |
| [Reindex](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex) | Copies documents from one index to another. |
| [Reindex Rethrottle](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex-rethrottle) | Changes the throttle for a running reindex task. |
| [Explain](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-explain) | Explains how a document matches (or doesn't match) a query. |
| [Get source](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get-source) | Retrieves the source of a document by ID. |
| [Exists](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-exists) | Checks if a document exists by ID. |

### [Enrich](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-enrich)

The enrich APIs enable you to manage enrich policies. An enrich policy is a set of configuration options used to add the right enrich data to the right incoming documents.

| API | Description |
| --- | ----------- |
| [Create or update enrich policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-enrich-put-policy) | Creates or updates an enrich policy. |
| [Get enrich policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-enrich-get-policy) | Retrieves enrich policy definitions. |
| [Delete enrich policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-enrich-delete-policy) | Deletes an enrich policy. |
| [Execute enrich policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-enrich-execute-policy) | Executes an enrich policy to create an enrich index. |
| [Get enrich stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-enrich-stats) | Returns enrich coordinator and policy execution statistics. |

### [EQL](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-eql)

The EQL APIs enable you to run EQL-related operations. Event Query Language (EQL) is a query language for event-based time series data, such as logs, metrics, and traces.

| API | Description |
| --- | ----------- |
| [Submit EQL search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-search) | Runs an EQL search. |
| [Get EQL search status](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-get) | Retrieves the status of an asynchronous EQL search. |
| [Get EQL search results](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-get) | Retrieves results of an asynchronous EQL search. |
| [Delete EQL search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-delete) | Cancels an asynchronous EQL search. |

### [ES|QL](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-esql)

The ES|QL APIs enable you to run ES|QL-related operations. The Elasticsearch Query Language (ES|QL) provides a powerful way to filter, transform, and analyze data stored in Elasticsearch, and in the future in other runtimes.

| API | Description |
| --- | ----------- |
| [ES|QL Query](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-query) | Executes an ES|QL query using a SQL-like syntax. |
| [ES|QL Async Submit](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query) | Submits an ES|QL query to run asynchronously. |
| [ES|QL Async Get](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query-get) | Retrieves results of an asynchronous ES|QL query. |
| [ES|QL Async Delete](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query-delete) | Cancels an asynchronous ES|QL query. |

### [Features](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-features)

The feature APIs enable you to introspect and manage features provided by {{es}} and {{es}} plugins.

| API | Description |
| --- | ----------- |
| [Get Features](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-features-get-features) | Lists all available features in the cluster. |
| [Reset Features](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-features-reset-features) | Resets internal state for system features. |

### [Fleet](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-fleet)

The Fleet APIs support Fleet’s use of Elasticsearch as a data store for internal agent and action data.

| API | Description |
| --- | ----------- |
| [Run Multiple Fleet Searches](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-fleet-msearch) | Runs several Fleet searches with a single API request. |
| [Run a Fleet Search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-fleet-search) | Runs a Fleet search. |
| [Get global checkpoints](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-fleet-global-checkpoints) | Get the current global checkpoints for an index. |

### [Graph explore](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-graph)

The graph explore APIs enable you to extract and summarize information about the documents and terms in an {{es}} data stream or index.

| API | Description |
| --- | ----------- |
| [Graph Explore](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-graph-explore) | Discovers relationships between indexed terms using relevance-based graph exploration. |

### [Index](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-indices)

The index APIs enable you to manage individual indices, index settings, aliases, mappings, and index templates.

| API | Description |
| --- | ----------- |
| [Create index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) | Creates a new index with optional settings and mappings. |
| [Delete index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete) | Deletes an existing index. |
| [Get index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get) | Retrieves information about one or more indices. |
| [Open index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-open) | Opens a closed index to make it available for operations. |
| [Close index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-close) | Closes an index to free up resources. |
| [Shrink index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shrink) | Shrinks an existing index into a new index with fewer primary shards. |
| [Split index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-split) | Splits an existing index into a new index with more primary shards. |
| [Clone index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-clone) | Clones an existing index into a new index. |
| [Check alias](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-exists-alias) | Manages index aliases. |
| [Update field mappings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) | Updates index mappings. |
| [Get field mappings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-mapping) | Retrieves index mappings. |
| [Get index settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-settings) | Retrieves settings for one or more indices. |
| [Update index settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings) | Updates index-level settings dynamically. |
| [Get index templates](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-template) | Retrieves legacy index templates. |
| [Put index template](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-template) | Creates or updates a legacy index template. |
| [Delete index template](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-template) | Deletes a legacy index template. |
| [Get composable index templates](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-index-template) | Retrieves composable index templates. |
| [Put composable index template](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-index-template) | Creates or updates a composable index template. |
| [Delete composable index template](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-index-template) | Deletes a composable index template. |
| [Get index alias](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-alias) | Retrieves index aliases. |
| [Delete index alias](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-alias) | Deletes index aliases. |
| [Refresh index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-refresh) | Refreshes one or more indices, making recent changes searchable. |
| [Flush index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-flush) | Performs a flush operation on one or more indices. |
| [Clear index cache](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-clear-cache) | Clears caches associated with one or more indices. |
| [Force merge index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-forcemerge) | Merges index segments to reduce their number and improve performance. |
| [Rollover index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-rollover) | Rolls over an alias to a new index when conditions are met. |
| [Resolve index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-resolve-index) | Resolves expressions to index names, aliases, and data streams. |
| [Simulate index template](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-simulate-index-template) | Simulates the application of a composable index template. |
| [Simulate template](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-simulate-template) | Simulates the application of a legacy index template. |
| [Get mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-mapping) | Retrieves mapping definitions for one or more indices. |
| [Put mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) | Updates mapping definitions for one or more indices. |
| [Reload search analyzers](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-reload-search-analyzers) | Reloads search analyzers for one or more indices. |
| [Shrink index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shrink) | Shrinks an existing index into a new index with fewer primary shards. |
| [Split index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-split) | Splits an existing index into a new index with more primary shards. |
| [Clone index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-clone) | Clones an existing index into a new index. |

### [Index lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ilm)

The index lifecycle management APIs enable you to set up policies to automatically manage the index lifecycle.

| API | Description |
| --- | ----------- |
| [Put Lifecycle Policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-put-lifecycle) | Creates or updates an ILM policy. |
| [Get Lifecycle Policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-get-lifecycle) | Retrieves one or more ILM policies. |
| [Delete Lifecycle Policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-delete-lifecycle) | Deletes an ILM policy. |
| [Explain Lifecycle](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-explain-lifecycle) | Shows the current lifecycle step for indices. |
| [Move to Step](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-move-to-step) | Manually moves an index to the next step in its lifecycle. |
| [Retry Lifecycle Step](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-retry) | Retries the current lifecycle step for failed indices. |
| [Start ILM](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-start) | Starts the ILM plugin. |
| [Stop ILM](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-stop) | Stops the ILM plugin. |
| [Get ILM Status](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-get-status) | Returns the status of the ILM plugin. |

### [Inference](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-inference)

The inference APIs enable you to create inference endpoints and integrate with machine learning models of different services - such as Amazon Bedrock, Anthropic, Azure AI Studio, Cohere, Google AI, Groq, Mistral, OpenAI, or HuggingFace.

| API | Description |
| --- | ----------- |
| [Put Inference Endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) | Creates an inference endpoint. |
| [Get Inference Endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-get) | Retrieves one or more inference endpoints. |
| [Delete Inference Endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-delete) | Deletes an inference endpoint. |
| [Infer](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-inference) | Runs inference using a deployed model. |

### [Info](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-info)

The info API provides basic build, version, and cluster information.

| API | Description |
| --- | ----------- |
| [Get cluster information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-info) | Returns basic information about the cluster. |

### [Ingest](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ingest)

The ingest APIs enable you to manage tasks and resources related to ingest pipelines and processors.

| API | Description |
| --- | ----------- |
| [Create or update pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-put-pipeline) | Creates or updates an ingest pipeline. |
| [Get pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-get-pipeline) | Retrieves one or more ingest pipelines. |
| [Delete pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-delete-pipeline) | Deletes an ingest pipeline. |
| [Simulate pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-simulate) | Simulates a document through an ingest pipeline. |
| [Get built-in grok patterns](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-processor-grok) | Returns a list of built-in grok patterns. |

### [Licensing](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-license)

The licensing APIs enable you to manage your licenses.

| API | Description |
| --- | ----------- |
| [Get license](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-license-get) | Retrieves the current license for the cluster. |
| [Update license](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-license-post) | Updates the license for the cluster. |
| [Delete license](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-license-delete) | Removes the current license. |
| [Start basic license](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-license-post-start-basic) | Starts a basic license. |
| [Start trial license](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-license-post-start-trial) | Starts a trial license. |
| [Get the trial status](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-license-get-trial-status) | Returns the status of the current trial license. |

### [Logstash](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-logstash)

The logstash APIs enable you to manage pipelines that are used by Logstash Central Management.

| API | Description |
| --- | ----------- |
| [Create or update Logstash pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-logstash-put-pipeline) | Creates or updates a Logstash pipeline. |
| [Get Logstash pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-logstash-get-pipeline) | Retrieves one or more Logstash pipelines. |
| [Delete Logstash pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-logstash-delete-pipeline) | Deletes a Logstash pipeline. |

### [Machine learning](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml)

The machine learning APIs enable you to retrieve information related to the {{stack}} {{ml}} features.

| API | Description |
| --- | ----------- |
| [Get machine learning memory stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-memory-stats) | Gets information about how machine learning jobs and trained models are using memory. |
| [Get machine learning info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-info) | Gets defaults and limits used by machine learning. |
| [Set upgrade mode](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-set-upgrade-mode) | Sets a cluster wide upgrade_mode setting that prepares machine learning indices for an upgrade. |
| [Get ML job stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-job-stats) | Retrieves usage statistics for ML jobs. |
| [Get ML calendar events](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-calendar-events) | Retrieves scheduled events for ML calendars. |
| [Get ML filters](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-filters) | Retrieves ML filters. |
| [Put ML filter](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-put-filter) | Creates or updates an ML filter. |
| [Delete ML filter](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-filter) | Deletes an ML filter. |
| [Get ML info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-info) | Gets overall ML info. |
| [Get ML model snapshots](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-model-snapshots) | Retrieves model snapshots for ML jobs. |
| [Revert ML model snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-revert-model-snapshot) | Reverts an ML job to a previous model snapshot. |
| [Delete expired ML data](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-expired-data) | Deletes expired ML results and model snapshots. |

### [Machine learning anomaly detection](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml-anomaly)

The machine learning anomaly detection APIs enable you to perform anomaly detection activities.


| API | Description |
| --- | ----------- |
| [Put Job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-put-job) | Creates an anomaly detection job. |
| [Get Job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-jobs) | Retrieves configuration info for anomaly detection jobs. |
| [Delete Job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-job) | Deletes an anomaly detection job. |
| [Open Job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-open-job) | Opens an existing anomaly detection job. |
| [Close anomaly detection jobs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-close-job) | Closes an anomaly detection job. |
| [Flush Job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-flush-job) | Forces any buffered data to be processed. |
| [Forecast Job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-forecast) | Generates forecasts for anomaly detection jobs. |
| [Get Buckets](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-buckets) | Retrieves bucket results from a job. |
| [Get Records](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-records) | Retrieves anomaly records for a job. |
| [Get calendar configuration info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-calendars) | Gets calendar configuration information. |
| [Create a calendar](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-put-calendar) | Create a calendar. |
| [Delete a calendar](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-calendar) | Delete a calendar. |
| [Delete events from a calendar](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-calendar) | Delete events from a calendar. |
| [Add anomaly detection job to calendar](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-put-calendar-job) | Add an anomaly detection job to a calendar. |
| [Delete anomaly detection jobs from calendar](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-calendar-job) | Deletes anomaly detection jobs from a calendar. |
| [Get datafeeds configuration info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-datafeeds) | Get configuration information for a datafeed. |
| [Create datafeed](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-put-datafeed) | Creates a datafeed. |
| [Delete a datafeed](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-datafeed) | Deletes a datafeed. |
| [Delete expired ML data](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-expired-data) | Delete all job results, model snapshots and forecast data that have exceeded their retention days period.  |
| [Delete expired ML data](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-expired-data) | Delete all job results, model snapshots and forecast data that have exceeded their retention days period.  |
| [Get filters](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-filters) | Get a single filter or all filters. |
| [Get anomaly detection job results for influencers](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-influencers) | Get anomaly detection job results for entities that contributed to or are to blame for anomalies. |
| [Get anomaly detection job stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-job-stats) | Get anomaly detection job stats. |
| [Get anomaly detection jobs configuration info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-jobs) | You can get information for multiple anomaly detection jobs in a single API request by using a group name, a comma-separated list of jobs, or a wildcard expression. |

### [Machine learning data frame analytics](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml-data-frame)

The machine learning data frame analytics APIs enable you to perform data frame analytics activities.

| API | Description |
| --- | ----------- |
| [Create a data frame analytics job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-put-data-frame-analytics) | Creates a data frame analytics job. |
| [Get data frame analytics job configuration info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-data-frame-analytics) | Retrieves configuration and results for analytics jobs. |
| [Delete a data frame analytics job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-data-frame-analytics) | Deletes a data frame analytics job. |
| [Start a data frame analytics job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-start-data-frame-analytics) | Starts a data frame analytics job. |
| [Stop data frame analytics jobs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-stop-data-frame-analytics) | Stops a running data frame analytics job. |

### [Machine learning trained models](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml-trained-model)

The machine learning trained models APIs enable you to perform model management operations.

| API | Description |
| --- | ----------- |
| [Put Trained Model](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-put-trained-model) | Uploads a trained model for inference. |
| [Get Trained Models](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-trained-models) | Retrieves configuration and stats for trained models. |
| [Delete Trained Model](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-delete-trained-model) | Deletes a trained model. |
| [Start Deployment](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-start-trained-model-deployment) | Starts a trained model deployment. |
| [Stop Deployment](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-stop-trained-model-deployment) | Stops a trained model deployment. |
| [Get Deployment Stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-get-trained-models-stats) | Retrieves stats for deployed models. |

### [Migration](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-migration)

The migration APIs power {{kib}}'s Upgrade Assistant feature.


| API | Description |
| --- | ----------- |
| [Deprecation Info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-deprecations) | Retrieves deprecation warnings for cluster and indices. |
| [Get Feature Upgrade Status](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-get-feature-upgrade-status) | Checks upgrade status of system features. |
| [Post Feature Upgrade](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-post-feature-upgrade) | Upgrades internal system features after a version upgrade. |

### [Query rules](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-query_rules)

Query rules enable you to configure per-query rules that are applied at query time to queries that match the specific rule. Query rules are organized into rulesets, collections of query rules that are matched against incoming queries. Query rules are applied using the rule query. If a query matches one or more rules in the ruleset, the query is re-written to apply the rules before searching. This allows pinning documents for only queries that match a specific term.

| API | Description |
| --- | ----------- |
| [Create or update query ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-put-ruleset) | Creates or updates a query ruleset. |
| [Get query ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-get-ruleset) | Retrieves one or more query rulesets. |
| [Delete query ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-delete-ruleset) | Deletes a query ruleset. |

### [Rollup](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-rollup)

The rollup APIs enable you to create, manage, and retrieve information about rollup jobs.

| API | Description |
| --- | ----------- |
| [Create or update rollup job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-put-job) | Creates or updates a rollup job for summarizing historical data. |
| [Get rollup jobs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-get-jobs) | Retrieves configuration for one or more rollup jobs. |
| [Delete rollup job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-delete-job) | Deletes a rollup job. |
| [Start rollup job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-start-job) | Starts a rollup job. |
| [Stop rollup job](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-stop-job) | Stops a running rollup job. |
| [Get rollup capabilities](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-get-rollup-caps) | Returns the capabilities of rollup jobs. |
| [Search rollup data](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rollup-rollup-search) | Searches rolled-up data using a rollup index. |

### [Script](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-script)

Use the script support APIs to get a list of supported script contexts and languages. Use the stored script APIs to manage stored scripts and search templates.


| API | Description |
| --- | ----------- |
| [Add or update stored script](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-put-script) | Adds or updates a stored script. |
| [Get stored script](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get-script) | Retrieves a stored script. |
| [Delete stored script](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete-script) | Deletes a stored script. |
| [Execute Painless script](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-scripts-painless-execute) | Executes a script using the Painless language. |
| [Get script contexts](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get-script-context) | Returns available script execution contexts. |
| [Get script languages](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get-script-languages) | Returns available scripting languages. |

### [Search](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-search)

The search APIs enable you to search and aggregate data stored in {{es}} indices and data streams.

| API | Description |
| --- | ----------- |
| [Search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) | Executes a search query on one or more indices. |
| [Multi search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-msearch) | Executes multiple search requests in a single API call. |
| [Search template](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-template) | Executes a search using a stored or inline template. |
| [Render search template](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-render-search-template) | Renders a search template with parameters. |
| [Explain search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-explain) | Explains how a document scores against a query. |
| [Get field capabilities](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-field-caps) | Returns the capabilities of fields across indices. |
| [Scroll search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-scroll) | Efficiently retrieves large numbers of results (pagination). |
| [Clear scroll](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-clear-scroll) | Clears search contexts for scroll requests. |

### [Search application](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-search_application)

The search application APIs enable you to manage tasks and resources related to Search Applications.

| API | Description |
| --- | ----------- |
| [Create or update search application](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-put) | Creates or updates a search application. |
| [Get search application](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-get) | Retrieves a search application by name. |
| [Delete search application](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-delete) | Deletes a search application. |
| [Search search application](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-application-search) | Executes a search using a search application. |

### [Searchable snapshots](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-searchable_snapshots)

The searchable snapshots APIs enable you to perform searchable snapshots operations.

| API | Description |
| --- | ----------- |
| [Mount searchable snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-searchable-snapshots-mount) | Mounts a snapshot as a searchable index. |
| [Clear searchable snapshot cache](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-searchable-snapshots-clear-cache) | Clears the cache of searchable snapshots. |
| [Get searchable snapshot stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-searchable-snapshots-stats) | Returns stats about searchable snapshots. |

### [Security](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-security)

The security APIs enable you to perform security activities, and add, update, retrieve, and remove application privileges, role mappings, and roles. You can also create and update API keys and create and invalidate bearer tokens.


| API | Description |
| --- | ----------- |
| [Create or update user](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-user) | Creates or updates a user in the native realm. |
| [Get user](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-user) | Retrieves one or more users. |
| [Delete user](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-delete-user) | Deletes a user from the native realm. |
| [Create or update role](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-role) | Creates or updates a role. |
| [Get role](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-role) | Retrieves one or more roles. |
| [Delete role](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-delete-role) | Deletes a role. |
| [Create API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key) | Creates an API key for access without basic auth. |
| [Invalidate API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-invalidate-api-key) | Invalidates one or more API keys. |
| [Authenticate](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-authenticate) | Retrieves information about the authenticated user. |

### [Snapshot and restore](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-snapshot)

The snapshot and restore APIs enable you to set up snapshot repositories, manage snapshot backups, and restore snapshots to a running cluster.

| API | Description |
| --- | ----------- |
| [Clean up snapshot repository](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-cleanup-repository) | Removes stale data from a repository. |
| [Clone snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-clone) | Clones indices from a snapshot into a new snapshot. |
| [Get snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-get) | Retrieves information about snapshots. |
| [Create snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-create) | Creates a snapshot of one or more indices. |
| [Delete snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-delete) | Deletes a snapshot from a repository. |
| [Get snapshot repository](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-get-repository) | Retrieves information about snapshot repositories. |
| [Create or update snapshot repository](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-create-repository) | Registers or updates a snapshot repository. |
| [Delete snapshot repository](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-delete-repository) | Deletes a snapshot repository. |
| [Restore snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-restore) | Restores a snapshot. |
| [Analyze snapshot repository](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-repository-analyze) | Analyzes a snapshot repository for correctness and performance. |
| [Verify snapshot repository](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-repository-verify-integrity) | Verifies access to a snapshot repository. |
| [Get snapshot status](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-status) | Gets the status of a snapshot. |

### [Snapshot lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-slm)

The snapshot lifecycle management APIs enable you to set up policies to automatically take snapshots and control how long they are retained.

| API | Description |
| --- | ----------- |
| [Get snapshot lifecycle policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-get-lifecycle) | Retrieves one or more snapshot lifecycle policies. |
| [Create or update snapshot lifecycle policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-put-lifecycle) | Creates or updates a snapshot lifecycle policy. |
| [Delete snapshot lifecycle policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-delete-lifecycle) | Deletes a snapshot lifecycle policy. |
| [Execute snapshot lifecycle policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-execute-lifecycle) | Triggers a snapshot lifecycle policy manually. |
| [Execute snapshot retention](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-execute-retention) | Manually apply the retention policy to force immediate removal of snapshots that are expired according to the snapshot lifecycle policy retention rules. |
| [Get snapshot lifecycle stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-get-stats) | Returns statistics about snapshot lifecycle executions. |
| [Get snapshot lifecycle status](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-get-status) | Returns the status of the snapshot lifecycle management feature. |
| [Start snapshot lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-start) | Starts the snapshot lifecycle management feature. |
| [Stop snapshot lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-stop) | Stops the snapshot lifecycle management feature. |

### [SQL](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-sql)

The SQL APIs enable you to run SQL queries on Elasticsearch indices and data streams.

| API | Description |
| --- | ----------- |
| [Clear SQL cursor](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-clear-cursor) | Clears the server-side cursor for an SQL search. |
| [Delete async SQL search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-delete-async) | Deletes an async SQL search. |
| [Get async SQL search results](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-get-async) | Retrieves results of an async SQL query. |
| [Get async SQL search status](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-get-async-status) | Gets the current status of an async SQL search or a stored synchronous SQL search. |
| [SQL query](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query) | Executes an SQL query. |
| [Translate SQL](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-translate) | Translates SQL into Elasticsearch DSL. |

### [Synonyms](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-synonyms)

The synonyms management APIs provide a convenient way to define and manage synonyms in an internal system index. Related synonyms can be grouped in a "synonyms set".

| API | Description |
| --- | ----------- |
| [Get synonym set](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym) | Retrieves a synonym set by ID. |
| [Create of update synonym set](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-put-synonym) | Creates or updates a synonym set. |
| [Delete synonym set](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/operation/operation-synonyms-delete-synonym) | Deletes a synonym set. |
| [Get synonym rule](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym-rule) | |
| [Get synonym sets](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonyms-sets) | Lists all synonym sets. |

### [Task management](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-tasks)

The task management APIs enable you to retrieve information about tasks or cancel tasks running in a cluster.

| API | Description |
| --- | ----------- |
| [Cancel a task](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-tasks-cancel) | Cancels a running task. |
| [Get task information](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-tasks-get) |  |
| [Get all tasks](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-tasks-list) | Retrieves information about running tasks. |

### [Text structure](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-text_structure)

The text structure APIs enable you to find the structure of a text field in an {{es}} index.

| API | Description |
| --- | ----------- |
| [Find the structure of a text field](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-text-structure-find-field-structure) |  |
| [Find the structure of a text message](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-text-structure-find-message-structure) |  |
| [Find the structure of a text file](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-text-structure-find-structure) | Analyzes a text file and returns its structure. |
| [Test a Grok pattern](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-text-structure-test-grok-pattern) |  |

### [Transform](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-transform)

The transform APIs enable you to create and manage transforms.

| API | Description |
| --- | ----------- |
| [Get transforms](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-get-transform) | Retrieves configuration for one or more transforms. |
| [Create a transform](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-put-transform) | Creates or updates a transform job. |
| [Get transform stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-get-transform-stats) | Get usage information for transforms. |
| [Preview transform](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-preview-transform) | Previews the results of a transform job. |
| [Reset a transform](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-reset-transform) | Previews the results of a transform job. |
| [Delete transform](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-delete-transform) | Deletes a transform job. |
| [Schedule a transform](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-schedule-now-transform) | Previews the results of a transform job. |
| [Start transform](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-start-transform) | Starts a transform job. |
| [Stop transform](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-stop-transform) | Stops a running transform job. |
| [Update transform](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-update-transform) | Updates certain properties of a transform. |
| [Upgrade all transforms](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-upgrade-transforms) | Updates certain properties of a transform. |

### [Usage](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-xpack)

The usage API provides usage information about the installed X-Pack features.

| API | Description |
| --- | ----------- |
| [Get information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-xpack-info) | Gets information about build details, license status, and a list of features currently available under the installed license. |
| [Get usage information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-xpack-usage) | Get information about the features that are currently enabled and available under the current license. |


### [Watcher](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-watcher)

You can use Watcher to watch for changes or anomalies in your data and perform the necessary actions in response.

| API | Description |
| --- | ----------- |
| [Acknowledge a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-ack-watch) | Acknowledges a watch action. |
| [Activate a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-activate-watch) | Activates a watch. |
| [Deactivates a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-deactivate-watch) | Deactivates a watch. |
| [Get a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-get-watch) | Retrieves a watch by ID. |
| [Create or update a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-put-watch) | Creates or updates a watch. |
| [Delete a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-delete-watch) | Deletes a watch. |
| [Run a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-execute-watch) | Executes a watch manually. |
| [Get Watcher index settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-get-settings) | Get settings for the Watcher internal index |
| [Update Watcher index settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-update-settings) | Update settings for the Watcher internal index |
| [Query watches](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-query-watches) | Get all registered watches in a paginated manner and optionally filter watches by a query. |
| [Start the watch service](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-start) | Starts the Watcher service. |
| [Get Watcher statistics](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-stats) | Returns statistics about the Watcher service. |
| [Stop the watch service](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-stop) | Stops the Watcher service. |
