---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rest-apis.html
  - https://www.elastic.co/guide/en/serverless/current/elasticsearch-differences.html
---

# REST APIs

Elasticsearch exposes REST APIs that are used by the UI components and can be called directly to configure and access Elasticsearch features.

For API reference information, go to [Elasticsearch API](https://www.elastic.co/docs/api/doc/elasticsearch) and [Elasticsearch Serverless API](https://www.elastic.co/docs/api/doc/elasticsearch-serverless).

This section includes:

- [API conventions](/reference/elasticsearch/rest-apis/api-conventions.md)
- [Common options](/reference/elasticsearch/rest-apis/common-options.md)
- [Compatibility](/reference/elasticsearch/rest-apis/compatibility.md)
- [Examples](/reference/elasticsearch/rest-apis/api-examples.md)

## API endpoints

### [Autoscaling](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-autoscaling)

The autoscaling APIs enable you to create and manage autoscaling policies and retrieve information about autoscaling capacity. Autoscaling adjusts resources based on demand. A deployment can use autoscaling to scale resources as needed, ensuring sufficient capacity to meet workload requirements.

| API | Description |
| --- | ----------- |
| [Get Autoscaling Policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-autoscaling-get-autoscaling-policy) | Retrieves a specific autoscaling policy. |
| [Create or update an autoscaling policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-autoscaling-put-autoscaling-policy) | Creates or updates an autoscaling policy. |
| [Delete Autoscaling Policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-autoscaling-delete-autoscaling-policy) | Deletes an existing autoscaling policy. |
| [Get Autoscaling Capacity](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-autoscaling-get-autoscaling-capacity) | Estimates autoscaling capacity for current cluster state. |

### [Behavioral analytics](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-analytics)

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

### [Compact and aligned text (CAT)](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-cat)

The compact and aligned text (CAT) APIs return human-readable text as a response, instead of a JSON object. The CAT APIs aim are intended only for human consumption using the Kibana console or command line. They are not intended for use by applications. For application consumption, it's recommend to use a corresponding JSON API.


| API | Description |
| --- | ----------- |
| [Cat aliases](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-aliases) | Get the cluster's index aliases, including filter and routing information in a compact, human-readable format. |
| [Cat allocation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-allocation) | Provides a snapshot of the number of shards allocated to each data node and their disk space in a compact, human-readable format. |
| [Cat Indices](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-indices) | Lists index stats in a compact, human-readable format. |
| [Cat Nodes](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cat.nodes) | Shows node-level metrics like CPU, memory, and roles. |
| [Cat Shards](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cat.shards) | Displays shard allocation across nodes. |
| [Cat Health](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cat.health) | Provides a snapshot of cluster health. |

### [Cluster](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-cluster)

The cluster APIs enable you to retrieve information about your infrastructure on cluster, node, or shard level. You can manage cluster settings and voting configuration exceptions, collect node statistics and retrieve node information.

| API | Description |
| --- | ----------- |
| [Cluster Health](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.health) | Returns health status of the cluster. |
| [Cluster Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.stats) | Summarizes cluster-wide statistics. |
| [Cluster Reroute](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.reroute) | Manually reassigns shard allocations. |
| [Cluster State](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.state) | Retrieves the current cluster state. |
| [Update Settings](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.put_settings) | Updates persistent or transient cluster settings. |

### [Cluster - Health](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-health_report)

The cluster - health API provides you a report with the health status of an Elasticsearch cluster.


| API | Description |
| --- | ----------- |
| [Cluster Health](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.health) | Returns health status of the cluster, including index-level details. |
| [Cluster Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.stats) | Provides statistics about the entire cluster. |
| [Cluster State](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.state) | Returns the current state of the cluster, including metadata and routing. |
| [Pending Tasks](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.pending_tasks) | Lists cluster-level tasks that are queued for execution. |

### [Connector](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-connector)

The connector and sync jobs APIs provide a convenient way to create and manage Elastic connectors and sync jobs in an internal index.

| API | Description |
| --- | ----------- |
| [Get Connector](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.get_connector) | Retrieves a connector configuration. |
| [Put Connector](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.put_connector) | Creates or updates a connector configuration. |
| [Delete Connector](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.delete_connector) | Deletes a connector configuration. |
| [Post Connector Sync Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.post_connector_sync_job) | Starts a sync job for a connector. |
| [Get Connector Sync Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.get_connector_sync_job) | Retrieves sync job details for a connector. |

### [Cross-cluster replication (CCR)](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ccr)

The cross-cluster replication (CCR) APIs enable you to run cross-cluster replication operations, such as creating and managing follower indices or auto-follow patterns. With CCR, you can replicate indices across clusters to continue handling search requests in the event of a datacenter outage, prevent search volume from impacting indexing throughput, and reduce search latency by processing search requests in geo-proximity to the user.

| API | Description |
| --- | ----------- |
| [Follow Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ccr.follow) | Creates a follower index that replicates a leader index from a remote cluster. |
| [Pause Follow](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ccr.pause_follow) | Pauses replication of a follower index. |
| [Resume Follow](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ccr.resume_follow) | Resumes replication of a paused follower index. |
| [Unfollow Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ccr.unfollow) | Converts a follower index into a regular index. |
| [Get Follow Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ccr.follow_stats) | Retrieves stats about follower indices. |
| [Get Auto-follow Patterns](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ccr.get_auto_follow_pattern) | Lists auto-follow patterns for remote clusters. |
| [Put Auto-follow Pattern](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ccr.put_auto_follow_pattern) | Creates or updates an auto-follow pattern. |
| [Delete Auto-follow Pattern](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ccr.delete_auto_follow_pattern) | Deletes an auto-follow pattern. |

### [Data stream](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-data-stream)

The data stream APIs enable you to create and manage data streams and data stream lifecycles. A data stream lets you store append-only time series data across multiple indices while giving you a single named resource for requests. Data streams are well-suited for logs, events, metrics, and other continuously generated data.

| API | Description |
| --- | ----------- |
| [Create Data Stream](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.create_data_stream) | Creates a new data stream. |
| [Delete Data Stream](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.delete_data_stream) | Deletes an existing data stream. |
| [Get Data Stream](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.get_data_stream) | Retrieves one or more data streams. |
| [Modify Data Stream](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.modify_data_stream) | Updates the backing index configuration for a data stream. |
| [Promote Data Stream Write Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.promote_data_stream_write_index) | Promotes a backing index to be the write index. |
| [Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.stats) | Returns statistics about data streams. |

### [Document](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-document)

The document APIs enable you to create and manage documents in an {{es}} index.

| API | Description |
| --- | ----------- |
| [Index Document](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.index) | Indexes a document into a specific index. |
| [Get Document](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.get) | Retrieves a document by ID. |
| [Delete Document](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.delete) | Deletes a document by ID. |
| [Update Document](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.update) | Updates a document using a script or partial doc. |
| [Bulk](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.bulk) | Performs multiple indexing or delete operations in a single API call. |
| [Mget](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.mget) | Retrieves multiple documents by ID in one request. |

### [Enrich](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-enrich)

The enrich APIs enable you to manage enrich policies. An enrich policy is a set of configuration options used to add the right enrich data to the right incoming documents.

| API | Description |
| --- | ----------- |
| [Put Enrich Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/enrich.put_policy) | Creates or updates an enrich policy. |
| [Get Enrich Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/enrich.get_policy) | Retrieves enrich policy definitions. |
| [Delete Enrich Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/enrich.delete_policy) | Deletes an enrich policy. |
| [Execute Enrich Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/enrich.execute_policy) | Executes an enrich policy to create an enrich index. |

### [EQL](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-eql)

The EQL APIs enable you to run EQL-related operations. Event Query Language (EQL) is a query language for event-based time series data, such as logs, metrics, and traces.

| API | Description |
| --- | ----------- |
| [EQL Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/eql.search) | Executes an EQL query to find event sequences in time-series data. |
| [Get EQL Search Status](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/eql.get_status) | Retrieves the status of an asynchronous EQL search. |
| [Get EQL Search Results](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/eql.get) | Retrieves results of an asynchronous EQL search. |
| [Delete EQL Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/eql.delete) | Cancels an asynchronous EQL search. |

### [ES|QL](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-esql)

The ES|QL APIs enable you to run ES|QL-related operations. The Elasticsearch Query Language (ES|QL) provides a powerful way to filter, transform, and analyze data stored in Elasticsearch, and in the future in other runtimes.

| API | Description |
| --- | ----------- |
| [ES|QL Query](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/esql.query) | Executes an ES|QL query using a SQL-like syntax. |
| [ES|QL Async Submit](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/esql.async_submit) | Submits an ES|QL query to run asynchronously. |
| [ES|QL Async Get](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/esql.async_get) | Retrieves results of an asynchronous ES|QL query. |
| [ES|QL Async Delete](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/esql.async_delete) | Cancels an asynchronous ES|QL query. |

### [Features](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-features)

The feature APIs enable you to introspect and manage features provided by {{es}} and {{es}} plugins.

| API | Description |
| --- | ----------- |
| [Get Features](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/features.get_features) | Lists all available features in the cluster. |
| [Reset Features](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/features.reset_features) | Resets internal state for system features. |

### [Fleet](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-fleet)

The Fleet APIs support Fleet’s use of Elasticsearch as a data store for internal agent and action data.

| API | Description |
| --- | ----------- |
| [Get Fleet Status](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/fleet.get_status) | Retrieves the current status of the Fleet server. |
| [Fleet Setup](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/fleet.setup) | Initializes Fleet and sets up required resources. |
| [Fleet Agents](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/fleet.agents) | Manages agents enrolled in Fleet. |
| [Fleet Policies](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/fleet.agent_policies) | Manages agent policies used by Fleet agents. |

### [Graph explore](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-graph)

The graph explore APIs enable you to extract and summarize information about the documents and terms in an {{es}} data stream or index.

| API | Description |
| --- | ----------- |
| [Graph Explore](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/graph.explore) | Discovers relationships between indexed terms using relevance-based graph exploration. |

### [Index](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-indices)

The index APIs enable you to manage individual indices, index settings, aliases, mappings, and index templates.

| API | Description |
| --- | ----------- |
| [Create Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/index.create) | Creates a new index with optional settings and mappings. |
| [Delete Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/index.delete) | Deletes an existing index. |
| [Get Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/index.get) | Retrieves information about one or more indices. |
| [Open Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/index.open) | Opens a closed index to make it available for operations. |
| [Close Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/index.close) | Closes an index to free up resources. |
| [Shrink Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/index.shrink) | Shrinks an existing index into a new index with fewer primary shards. |
| [Split Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/index.split) | Splits an existing index into a new index with more primary shards. |
| [Clone Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/index.clone) | Clones an existing index into a new index. |

### [Index lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ilm)

The index lifecycle management APIs enable you to set up policies to automatically manage the index lifecycle.

| API | Description |
| --- | ----------- |
| [Put Lifecycle Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ilm.put_lifecycle) | Creates or updates an ILM policy. |
| [Get Lifecycle Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ilm.get_lifecycle) | Retrieves one or more ILM policies. |
| [Delete Lifecycle Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ilm.delete_lifecycle) | Deletes an ILM policy. |
| [Explain Lifecycle](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ilm.explain_lifecycle) | Shows the current lifecycle step for indices. |
| [Move to Step](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ilm.move_to_step) | Manually moves an index to the next step in its lifecycle. |
| [Retry Lifecycle Step](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ilm.retry) | Retries the current lifecycle step for failed indices. |
| [Start ILM](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ilm.start) | Starts the ILM plugin. |
| [Stop ILM](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ilm.stop) | Stops the ILM plugin. |
| [Get ILM Status](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ilm.get_status) | Returns the status of the ILM plugin. |

### [Inference](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-inference)

The inference APIs enable you to create inference endpoints and integrate with machine learning models of different services - such as Amazon Bedrock, Anthropic, Azure AI Studio, Cohere, Google AI, Mistral, OpenAI, or HuggingFace.

| API | Description |
| --- | ----------- |
| [Put Inference Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/inference.put_model) | Uploads a new inference model. |
| [Get Inference Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/inference.get_model) | Retrieves one or more inference models. |
| [Delete Inference Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/inference.delete_model) | Deletes an inference model. |
| [Infer](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/inference.infer) | Runs inference using a deployed model. |

### [Info](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-info)

The info API provides basic build, version, and cluster information.

| API | Description |
| --- | ----------- |
| [Root Info](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/info.root) | Returns basic information about the cluster. |
| [Main Info](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/info.main) | Returns version and build information about Elasticsearch. |

### [Ingest](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ingest)

The ingest APIs enable you to manage tasks and resources related to ingest pipelines and processors.

| API | Description |
| --- | ----------- |
| [Put Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.put_pipeline) | Creates or updates an ingest pipeline. |
| [Get Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.get_pipeline) | Retrieves one or more ingest pipelines. |
| [Delete Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.delete_pipeline) | Deletes an ingest pipeline. |
| [Simulate Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.simulate) | Simulates a document through an ingest pipeline. |
| [Processor Grok](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.processor_grok) | Returns a list of built-in grok patterns. |

### [Licensing](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-license)

The licensing APIs enable you to manage your licenses.

| API | Description |
| --- | ----------- |
| [Get License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.get) | Retrieves the current license for the cluster. |
| [Put License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.put) | Updates the license for the cluster. |
| [Delete License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.delete) | Removes the current license. |
| [Start Basic License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.post_start_basic) | Starts a basic license. |
| [Start Trial License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.post_start_trial) | Starts a trial license. |

### [Logstash](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-logstash)

The logstash APIs enable you to manage pipelines that are used by Logstash Central Management.

| API | Description |
| --- | ----------- |
| [Put Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/logstash.put_pipeline) | Creates or updates a Logstash pipeline. |
| [Get Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/logstash.get_pipeline) | Retrieves one or more Logstash pipelines. |
| [Delete Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/logstash.delete_pipeline) | Deletes a Logstash pipeline. |

### [Machine learning](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml)

The machine learning APIs enable you to retrieve information related to the {{stack}} {{ml}} features.

### [Machine learning anomaly detection](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml-anomaly)

The machine learning anomaly detection APIs enbale you to perform anomaly detection activities.


| API | Description |
| --- | ----------- |
| [Put Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.put_job) | Creates an anomaly detection job. |
| [Get Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.get_jobs) | Retrieves configuration info for anomaly detection jobs. |
| [Delete Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.delete_job) | Deletes an anomaly detection job. |
| [Open Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.open_job) | Opens an existing anomaly detection job. |
| [Close Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.close_job) | Closes an anomaly detection job. |
| [Flush Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.flush_job) | Forces any buffered data to be processed. |
| [Forecast Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.forecast) | Generates forecasts for anomaly detection jobs. |
| [Get Buckets](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.get_buckets) | Retrieves bucket results from a job. |
| [Get Records](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.get_records) | Retrieves anomaly records for a job. |

### [Machine learning data frame analytics](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml-data-frame)

The machine learning data frame analytics APIs enbale you to perform data frame analytics activities.

| API | Description |
| --- | ----------- |
| [Put Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.put_data_frame_analytics) | Creates a data frame analytics job. |
| [Get Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.get_data_frame_analytics) | Retrieves configuration and results for analytics jobs. |
| [Delete Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.delete_data_frame_analytics) | Deletes a data frame analytics job. |
| [Start Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.start_data_frame_analytics) | Starts a data frame analytics job. |
| [Stop Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.stop_data_frame_analytics) | Stops a running data frame analytics job. |

### [Machine learning trained models](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml-trained-model)

The machine learning trained models APIs enable you to perform model management operations.

| API | Description |
| --- | ----------- |
| [Put Trained Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.put_trained_model) | Uploads a trained model for inference. |
| [Get Trained Models](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.get_trained_models) | Retrieves configuration and stats for trained models. |
| [Delete Trained Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.delete_trained_model) | Deletes a trained model. |
| [Start Deployment](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.start_trained_model_deployment) | Starts a trained model deployment. |
| [Stop Deployment](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.stop_trained_model_deployment) | Stops a trained model deployment. |
| [Get Deployment Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.get_trained_models_stats) | Retrieves stats for deployed models. |

### [Migration](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-migration)

The migration APIs power {{kib}}'s Upgrade Assistant feature.


| API | Description |
| --- | ----------- |
| [Deprecation Info](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/migration.deprecations) | Retrieves deprecation warnings for cluster and indices. |
| [Get Feature Upgrade Status](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/migration.get_feature_upgrade_status) | Checks upgrade status of system features. |
| [Post Feature Upgrade](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/migration.post_feature_upgrade) | Upgrades internal system features after a version upgrade. |

### [Node lifecycle](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-shutdown)

The node lifecycle APIs enable you to prepare nodes for temporary or permanent shutdown, monitor the shutdown status, and enable a previously shut-down node to resume normal operations.

| API | Description |
| --- | ----------- |
| [Put Voting Config Exclusions](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.post_voting_config_exclusions) | Excludes nodes from voting in master elections. |
| [Delete Voting Config Exclusions](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.delete_voting_config_exclusions) | Clears voting config exclusions. |

### [Query rules](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-query_rules)

Query rules enable you to configure per-query rules that are applied at query time to queries that match the specific rule. Query rules are organized into rulesets, collections of query rules that are matched against incoming queries. Query rules are applied using the rule query. If a query matches one or more rules in the ruleset, the query is re-written to apply the rules before searching. This allows pinning documents for only queries that match a specific term.

| API | Description |
| --- | ----------- |
| [Put Query Ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/query_ruleset.put_query_ruleset) | Creates or updates a query ruleset. |
| [Get Query Ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/query_ruleset.get_query_ruleset) | Retrieves one or more query rulesets. |
| [Delete Query Ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/query_ruleset.delete_query_ruleset) | Deletes a query ruleset. |

### [Rollup](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-rollup)

The rollup APIs enable you to create, manage, and retrieve infromation about rollup jobs.

| API | Description |
| --- | ----------- |
| [Put Rollup Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.put_job) | Creates a new rollup job for summarizing historical data. |
| [Get Rollup Jobs](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.get_jobs) | Retrieves configuration for one or more rollup jobs. |
| [Delete Rollup Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.delete_job) | Deletes a rollup job. |
| [Start Rollup Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.start_job) | Starts a rollup job. |
| [Stop Rollup Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.stop_job) | Stops a running rollup job. |
| [Rollup Capabilities](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.get_rollup_caps) | Returns the capabilities of rollup jobs. |
| [Search Rollup Data](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.rollup_search) | Searches rolled-up data using a rollup index. |

### [Script](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-script)

Use the script support APIs to get a list of supported script contexts and languages. Use the stored script APIs to manage stored scripts and search templates.


| API | Description |
| --- | ----------- |
| [Put Stored Script](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/script.put_script) | Adds or updates a stored script. |
| [Get Stored Script](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/script.get_script) | Retrieves a stored script. |
| [Delete Stored Script](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/script.delete_script) | Deletes a stored script. |
| [Execute Script](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/script.painless_execute) | Executes a script using the Painless language. |

### [Search](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-search)

The search APIs enable you to search and aggregate data stored in {{es}} indices and data streams.

| API | Description |
| --- | ----------- |
| [Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.search) | Executes a search query on one or more indices. |
| [Multi Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.msearch) | Executes multiple search requests in a single API call. |
| [Search Template](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.search_template) | Executes a search using a stored or inline template. |
| [Render Search Template](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.render_search_template) | Renders a search template with parameters. |
| [Explain](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.explain) | Explains how a document scores against a query. |
| [Validate Query](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.validate_query) | Validates a query without executing it. |
| [Field Capabilities](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.field_caps) | Returns the capabilities of fields across indices. |

### [Search application](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-search_application)

The search applcation APIs enable you to manage tasks and resources related to Search Applications.

| API | Description |
| --- | ----------- |
| [Put Search Application](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search_application.put_search_application) | Creates or updates a search application. |
| [Get Search Application](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search_application.get_search_application) | Retrieves a search application by name. |
| [Delete Search Application](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search_application.delete_search_application) | Deletes a search application. |
| [Search Application Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search_application.search) | Executes a search using a search application. |

### [Searchable snapshots](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-searchable_snapshots)

The searchable snapshots APIs enable you to perform searchable snapshots operations.

| API | Description |
| --- | ----------- |
| [Mount Snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/searchable_snapshots.mount) | Mounts a snapshot as a searchable index. |
| [Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/searchable_snapshots.stats) | Returns stats about searchable snapshots. |
| [Clear Cache](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/searchable_snapshots.clear_cache) | Clears the cache of searchable snapshots. |

### [Security](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-security)

The security APIs enable you to perform security activities, and add, update, retrieve, and remove application privileges, role mappings, and roles. You can also create and update API keys and create and invalidate bearer tokens.


| API | Description |
| --- | ----------- |
| [Create or Update User](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/security.put_user) | Creates or updates a user in the native realm. |
| [Get User](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/security.get_user) | Retrieves one or more users. |
| [Delete User](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/security.delete_user) | Deletes a user from the native realm. |
| [Create or Update Role](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/security.put_role) | Creates or updates a role. |
| [Get Role](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/security.get_role) | Retrieves one or more roles. |
| [Delete Role](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/security.delete_role) | Deletes a role. |
| [Create API Key](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/security.create_api_key) | Creates an API key for access without basic auth. |
| [Invalidate API Key](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/security.invalidate_api_key) | Invalidates one or more API keys. |
| [Authenticate](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/security.authenticate) | Retrieves information about the authenticated user. |

### [Snapshot and restore](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-snapshot)

The snapshot and restore APIs enable you to set up snapshot repositories, manage snapshot backups, and restore snapshots to a running cluster.

| API | Description |
| --- | ----------- |
| [Create Snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/snapshot.create) | Creates a snapshot of one or more indices. |
| [Get Snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/snapshot.get) | Retrieves information about snapshots. |
| [Delete Snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/snapshot.delete) | Deletes a snapshot from a repository. |
| [Restore Snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/snapshot.restore) | Restores a snapshot into the cluster. |
| [Create Repository](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/snapshot.create_repository) | Registers a snapshot repository. |
| [Get Repository](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/snapshot.get_repository) | Retrieves information about snapshot repositories. |
| [Delete Repository](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/snapshot.delete_repository) | Deletes a snapshot repository. |
| [Verify Repository](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/snapshot.verify_repository) | Verifies access to a snapshot repository. |

### [Snapshot lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-slm)

The snapshot lifecycle management APIs enable you to set up policies to automatically take snapshots and control how long they are retained.

| API | Description |
| --- | ----------- |
| [Get policy information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-get-lifecycle) | Retrieves one or more snapshot lifecycle policies. |
| [Create or update a policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-put-lifecycle) | Creates or updates a snapshot lifecycle policy. |
| [Delete a policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-delete-lifecycle) | Deletes a snapshot lifecycle policy. |
| [Run a policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-execute-lifecycle) | Triggers a snapshot lifecycle policy manually. |
| [Run a retention policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-execute-retention) | Manually apply the retention policy to force immediate removal of snapshots that are expired according to the snapshot lifecycle policy retention rules. |
| [Get snapshot lifecycle management statistics](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-get-stats) | Returns statistics about snapshot lifecycle executions. |
| [Get the snapshot lifecycle management status](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-get-status) | Returns the status of the snapshot lifecycle management feature. |
| [Start snapshot lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-start) | Starts the snapshot lifecycle management feature. |
| [Stop snapshot lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-slm-stop) | Stops the snapshot lifecycle management feature. |

### [SQL](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-sql)

The SQL APIs enable you to run SQL queries on Elasticsearch indices and data streams.

| API | Description |
| --- | ----------- |
| [Clear a SQL search cursor](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-clear-cursor) |  |
| [Delete an async SQL search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-delete-async) |  |
| [Get async SQL search results](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-get-async) | Retrieves results of an async SQL query. |
| [Get the async SQL search status](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-get-async-status) | Get the current status of an async SQL search or a stored synchronous SQL search. |
| [Get SQL search results](hhttps://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query) | Executes an SQL query. |
| [SQL Translate](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-translate) | Translates SQL into Elasticsearch DSL. |
| [Translate SQL into Elasticsearch queries](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/sql.async_query) | Submits an SQL query to run asynchronously. |

### [Synonyms](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-synonyms)

The synonyms management APIs provide a convenient way to define and manage synonyms in an internal system index. Related synonyms can be grouped in a "synonyms set".

| API | Description |
| --- | ----------- |
| [Get synonym set](hthttps://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym) | Retrieves a synonym set by ID. |
| [Create of update synonym set](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-put-synonym) | Creates or updates a synonym set. |
| [Delete synonym set](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/synonyms.delete_synonym) | Deletes a synonym set. |
| [Get synonym rule](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym-rule) | |
| [Get synonym sets](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/synonyms.get_synonyms) | Lists all synonym sets. |

### [Task management](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-tasks)

The task management APIs enable you to retrieve information about tasks or cancel tasks running in a cluster.

| API | Description |
| --- | ----------- |
| [Cancel a task](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-tasks-cancel) | Cancels a running task. |
| [Get task information](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-tasks-get) | Cancels a running task. |
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
| [Get transform stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-transform-get-transform-stats) |  |
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
| [Get information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-xpack-info) |  |
| [Get usage information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-xpack-usage) |  |


### [Watcher](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-watcher)

You can use Watcher to watch for changes or anomalies in your data and perform the necessary actions in response.

| API | Description |
| --- | ----------- |
| [Acknowledge a watch](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.ack_watch) | Acknowledges a watch action. |
| [Activate a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-activate-watch) | Activates a watch. |
| [Deactivates a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-deactivate-watch) | Activates a watch. |
| [Get a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-get-watch) | Retrieves a watch by ID. |
| [Create or update a watch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-put-watch) | Creates or updates a watch. |
| [Delete a watch](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.delete_watch) | Deletes a watch. |
| [Run a watch](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.execute_watch) | Executes a watch manually. |
| [Get Watcher index settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-get-settings) |  |
| [Update Watcher index settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-update-settings) |  |
| [Query watches](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-query-watches) |  |
| [Start the watch service](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.start) | Starts the Watcher service. |
| [Get Watcher statistics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.stats) | Returns statistics about the Watcher service. |
| [Stop the watch service](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.stop) | Stops the Watcher service. |
