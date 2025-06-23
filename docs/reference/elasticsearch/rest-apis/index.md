---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rest-apis.html
  - https://www.elastic.co/guide/en/serverless/current/elasticsearch-differences.html
---

# REST APIs

Elasticsearch exposes REST APIs that are used by the UI components and can be called directly to configure and access Elasticsearch features.

## Autoscaling
[View Autoscaling APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-autoscaling)

| API | Description |
| --- | ----------- |
| [Get Autoscaling Capacity](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/autoscaling.get_autoscaling_capacity) | Estimates autoscaling capacity for current cluster state. |
| [Put Autoscaling Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/autoscaling.put_autoscaling_policy) | Creates or updates an autoscaling policy. |
| [Delete Autoscaling Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/autoscaling.delete_autoscaling_policy) | Deletes an existing autoscaling policy. |
| [Get Autoscaling Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/autoscaling.get_autoscaling_policy) | Retrieves a specific autoscaling policy. |

## Behavioral Analytics
[View Behavioral Analytics APIs](https://www.elastic.co/docs/api/doc/elasticsearch-serverless/group/endpoint-analytics)

| API | Description |
| --- | ----------- |
| [Get Collections](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/behavioral_analytics.get_behavioral_analytics) | Lists all behavioral analytics collections. |
| [Create Collection](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/behavioral_analytics.put_behavioral_analytics) | Creates a new behavioral analytics collection. |
| [Delete Collection](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/behavioral_analytics.delete_behavioral_analytics) | Deletes a behavioral analytics collection. |
| [Create Event](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/behavioral_analytics.post_behavioral_analytics_event) | Sends a behavioral analytics event to a collection. |

## Compact and Aligned Text (CAT)
[View CAT APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-cat)

| API | Description |
| --- | ----------- |
| [Cat Indices](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cat.indices) | Lists index stats in a compact, human-readable format. |
| [Cat Nodes](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cat.nodes) | Shows node-level metrics like CPU, memory, and roles. |
| [Cat Shards](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cat.shards) | Displays shard allocation across nodes. |
| [Cat Health](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cat.health) | Provides a snapshot of cluster health. |

## Cluster
[View Cluster APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-cluster)

| API | Description |
| --- | ----------- |
| [Cluster Health](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.health) | Returns health status of the cluster. |
| [Cluster Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.stats) | Summarizes cluster-wide statistics. |
| [Cluster Reroute](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.reroute) | Manually reassigns shard allocations. |
| [Cluster State](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.state) | Retrieves the current cluster state. |
| [Update Settings](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.put_settings) | Updates persistent or transient cluster settings. |

## Cluster - Health
[View Cluster Health APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-health_report)

| API | Description |
| --- | ----------- |
| [Cluster Health](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.health) | Returns health status of the cluster, including index-level details. |
| [Cluster Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.stats) | Provides statistics about the entire cluster. |
| [Cluster State](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.state) | Returns the current state of the cluster, including metadata and routing. |
| [Pending Tasks](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.pending_tasks) | Lists cluster-level tasks that are queued for execution. |

## Connector
[View Connector APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector)

| API | Description |
| --- | ----------- |
| [Get Connector](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.get_connector) | Retrieves a connector configuration. |
| [Put Connector](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.put_connector) | Creates or updates a connector configuration. |
| [Delete Connector](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.delete_connector) | Deletes a connector configuration. |
| [Post Connector Sync Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.post_connector_sync_job) | Starts a sync job for a connector. |
| [Get Connector Sync Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/connector.get_connector_sync_job) | Retrieves sync job details for a connector. |

## Cross-cluster Replication (CCR)
[View CCR APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-ccr)

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

## Data Stream
[View Data Stream APIs](https://www.elastic.co/docs/api/doc/elasticsearch-serverless/group/endpoint-data-stream)

| API | Description |
| --- | ----------- |
| [Create Data Stream](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.create_data_stream) | Creates a new data stream. |
| [Delete Data Stream](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.delete_data_stream) | Deletes an existing data stream. |
| [Get Data Stream](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.get_data_stream) | Retrieves one or more data streams. |
| [Modify Data Stream](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.modify_data_stream) | Updates the backing index configuration for a data stream. |
| [Promote Data Stream Write Index](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.promote_data_stream_write_index) | Promotes a backing index to be the write index. |
| [Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/data_stream.stats) | Returns statistics about data streams. |

## Document
[View Document APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-document)

| API | Description |
| --- | ----------- |
| [Index Document](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.index) | Indexes a document into a specific index. |
| [Get Document](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.get) | Retrieves a document by ID. |
| [Delete Document](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.delete) | Deletes a document by ID. |
| [Update Document](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.update) | Updates a document using a script or partial doc. |
| [Bulk](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.bulk) | Performs multiple indexing or delete operations in a single API call. |
| [Mget](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/document.mget) | Retrieves multiple documents by ID in one request. |

## Enrich
[View Enrich APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-enrich)

| API | Description |
| --- | ----------- |
| [Put Enrich Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/enrich.put_policy) | Creates or updates an enrich policy. |
| [Get Enrich Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/enrich.get_policy) | Retrieves enrich policy definitions. |
| [Delete Enrich Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/enrich.delete_policy) | Deletes an enrich policy. |
| [Execute Enrich Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/enrich.execute_policy) | Executes an enrich policy to create an enrich index. |

## EQL (Event Query Language)
[View EQL APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-eql)

| API | Description |
| --- | ----------- |
| [EQL Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/eql.search) | Executes an EQL query to find event sequences in time-series data. |
| [Get EQL Search Status](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/eql.get_status) | Retrieves the status of an asynchronous EQL search. |
| [Get EQL Search Results](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/eql.get) | Retrieves results of an asynchronous EQL search. |
| [Delete EQL Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/eql.delete) | Cancels an asynchronous EQL search. |

## ES|QL (Elasticsearch Query Language)
[View ES|QL APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-esql)

| API | Description |
| --- | ----------- |
| [ES|QL Query](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/esql.query) | Executes an ES|QL query using a SQL-like syntax. |
| [ES|QL Async Submit](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/esql.async_submit) | Submits an ES|QL query to run asynchronously. |
| [ES|QL Async Get](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/esql.async_get) | Retrieves results of an asynchronous ES|QL query. |
| [ES|QL Async Delete](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/esql.async_delete) | Cancels an asynchronous ES|QL query. |

## Features
[View Features APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-features)

| API | Description |
| --- | ----------- |
| [Get Features](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/features.get_features) | Lists all available features in the cluster. |
| [Reset Features](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/features.reset_features) | Resets internal state for system features. |

## Fleet
[View Fleet APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-fleet)

| API | Description |
| --- | ----------- |
| [Get Fleet Status](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/fleet.get_status) | Retrieves the current status of the Fleet server. |
| [Fleet Setup](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/fleet.setup) | Initializes Fleet and sets up required resources. |
| [Fleet Agents](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/fleet.agents) | Manages agents enrolled in Fleet. |
| [Fleet Policies](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/fleet.agent_policies) | Manages agent policies used by Fleet agents. |

## Graph Explore
[View Graph Explore API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-graph)

| API | Description |
| --- | ----------- |
| [Graph Explore](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/graph.explore) | Discovers relationships between indexed terms using relevance-based graph exploration. |

## Index
[View Index APIs](https://www.elastic.co/docs/api/doc/elasticsearch-serverless/group/endpoint-indices)

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

## Index Lifecycle Management (ILM)
[View ILM APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-ilm)

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

## Inference
[View Inference APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-inference)

| API | Description |
| --- | ----------- |
| [Put Inference Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/inference.put_model) | Uploads a new inference model. |
| [Get Inference Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/inference.get_model) | Retrieves one or more inference models. |
| [Delete Inference Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/inference.delete_model) | Deletes an inference model. |
| [Infer](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/inference.infer) | Runs inference using a deployed model. |

## Info
[View Info APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-info)

| API | Description |
| --- | ----------- |
| [Root Info](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/info.root) | Returns basic information about the cluster. |
| [Main Info](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/info.main) | Returns version and build information about Elasticsearch. |

## Ingest
[View Ingest APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-ingest)

| API | Description |
| --- | ----------- |
| [Put Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.put_pipeline) | Creates or updates an ingest pipeline. |
| [Get Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.get_pipeline) | Retrieves one or more ingest pipelines. |
| [Delete Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.delete_pipeline) | Deletes an ingest pipeline. |
| [Simulate Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.simulate) | Simulates a document through an ingest pipeline. |
| [Processor Grok](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ingest.processor_grok) | Returns a list of built-in grok patterns. |

## Licensing
[View Licensing APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-license)

| API | Description |
| --- | ----------- |
| [Get License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.get) | Retrieves the current license for the cluster. |
| [Put License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.put) | Updates the license for the cluster. |
| [Delete License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.delete) | Removes the current license. |
| [Start Basic License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.post_start_basic) | Starts a basic license. |
| [Start Trial License](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/license.post_start_trial) | Starts a trial license. |

## Logstash
[View Logstash APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-logstash)

| API | Description |
| --- | ----------- |
| [Put Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/logstash.put_pipeline) | Creates or updates a Logstash pipeline. |
| [Get Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/logstash.get_pipeline) | Retrieves one or more Logstash pipelines. |
| [Delete Pipeline](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/logstash.delete_pipeline) | Deletes a Logstash pipeline. |

## Machine Learning - Anomaly Detection
[View Anomaly Detection APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-ml-anomaly)

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

## Machine Learning - Data Frame Analytics
[View Data Frame Analytics APIs](https://www.elastic.co/docs/api/doc/elasticsearch-serverless/group/endpoint-ml-data-frame)

| API | Description |
| --- | ----------- |
| [Put Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.put_data_frame_analytics) | Creates a data frame analytics job. |
| [Get Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.get_data_frame_analytics) | Retrieves configuration and results for analytics jobs. |
| [Delete Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.delete_data_frame_analytics) | Deletes a data frame analytics job. |
| [Start Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.start_data_frame_analytics) | Starts a data frame analytics job. |
| [Stop Data Frame Analytics](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.stop_data_frame_analytics) | Stops a running data frame analytics job. |

## Machine Learning - Trained Models
[View Trained Model APIs](https://www.elastic.co/docs/api/doc/elasticsearch-serverless/group/endpoint-ml-trained-model)

| API | Description |
| --- | ----------- |
| [Put Trained Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.put_trained_model) | Uploads a trained model for inference. |
| [Get Trained Models](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.get_trained_models) | Retrieves configuration and stats for trained models. |
| [Delete Trained Model](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.delete_trained_model) | Deletes a trained model. |
| [Start Deployment](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.start_trained_model_deployment) | Starts a trained model deployment. |
| [Stop Deployment](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.stop_trained_model_deployment) | Stops a trained model deployment. |
| [Get Deployment Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/ml.get_trained_models_stats) | Retrieves stats for deployed models. |


## Migration
[View Migration APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-migration)

| API | Description |
| --- | ----------- |
| [Deprecation Info](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/migration.deprecations) | Retrieves deprecation warnings for cluster and indices. |
| [Get Feature Upgrade Status](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/migration.get_feature_upgrade_status) | Checks upgrade status of system features. |
| [Post Feature Upgrade](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/migration.post_feature_upgrade) | Upgrades internal system features after a version upgrade. |

## Node Lifecycle
[View Node Lifecycle APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-node_lifecycle)

| API | Description |
| --- | ----------- |
| [Put Voting Config Exclusions](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.post_voting_config_exclusions) | Excludes nodes from voting in master elections. |
| [Delete Voting Config Exclusions](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/cluster.delete_voting_config_exclusions) | Clears voting config exclusions. |

## Query Rules
[View Query Rules APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-query_ruleset)

| API | Description |
| --- | ----------- |
| [Put Query Ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/query_ruleset.put_query_ruleset) | Creates or updates a query ruleset. |
| [Get Query Ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/query_ruleset.get_query_ruleset) | Retrieves one or more query rulesets. |
| [Delete Query Ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/query_ruleset.delete_query_ruleset) | Deletes a query ruleset. |

## Rollup
[View Rollup APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-rollup)

| API | Description |
| --- | ----------- |
| [Put Rollup Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.put_job) | Creates a new rollup job for summarizing historical data. |
| [Get Rollup Jobs](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.get_jobs) | Retrieves configuration for one or more rollup jobs. |
| [Delete Rollup Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.delete_job) | Deletes a rollup job. |
| [Start Rollup Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.start_job) | Starts a rollup job. |
| [Stop Rollup Job](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.stop_job) | Stops a running rollup job. |
| [Rollup Capabilities](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.get_rollup_caps) | Returns the capabilities of rollup jobs. |
| [Search Rollup Data](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/rollup.rollup_search) | Searches rolled-up data using a rollup index. |

## Script
[View Script APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-script)

| API | Description |
| --- | ----------- |
| [Put Stored Script](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/script.put_script) | Adds or updates a stored script. |
| [Get Stored Script](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/script.get_script) | Retrieves a stored script. |
| [Delete Stored Script](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/script.delete_script) | Deletes a stored script. |
| [Execute Script](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/script.painless_execute) | Executes a script using the Painless language. |

## Search
[View Search APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-search)

| API | Description |
| --- | ----------- |
| [Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.search) | Executes a search query on one or more indices. |
| [Multi Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.msearch) | Executes multiple search requests in a single API call. |
| [Search Template](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.search_template) | Executes a search using a stored or inline template. |
| [Render Search Template](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.render_search_template) | Renders a search template with parameters. |
| [Explain](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.explain) | Explains how a document scores against a query. |
| [Validate Query](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.validate_query) | Validates a query without executing it. |
| [Field Capabilities](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search.field_caps) | Returns the capabilities of fields across indices. |

## Search Application
[View Search Application APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-search_application)

| API | Description |
| --- | ----------- |
| [Put Search Application](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search_application.put_search_application) | Creates or updates a search application. |
| [Get Search Application](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search_application.get_search_application) | Retrieves a search application by name. |
| [Delete Search Application](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search_application.delete_search_application) | Deletes a search application. |
| [Search Application Search](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/search_application.search) | Executes a search using a search application. |

## Searchable Snapshots
[View Searchable Snapshots APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-searchable_snapshots)

| API | Description |
| --- | ----------- |
| [Mount Snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/searchable_snapshots.mount) | Mounts a snapshot as a searchable index. |
| [Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/searchable_snapshots.stats) | Returns stats about searchable snapshots. |
| [Clear Cache](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/searchable_snapshots.clear_cache) | Clears the cache of searchable snapshots. |

## Security
[View Security APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-security)

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

## Snapshot and Restore
[View Snapshot APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-snapshot)

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

## Snapshot Lifecycle Management (SLM)
[View SLM APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-slm)

| API | Description |
| --- | ----------- |
| [Put SLM Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/slm.put_lifecycle) | Creates or updates a snapshot lifecycle policy. |
| [Get SLM Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/slm.get_lifecycle) | Retrieves one or more snapshot lifecycle policies. |
| [Delete SLM Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/slm.delete_lifecycle) | Deletes a snapshot lifecycle policy. |
| [Execute SLM Policy](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/slm.execute_lifecycle) | Triggers a snapshot lifecycle policy manually. |
| [Get SLM Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/slm.get_stats) | Returns statistics about snapshot lifecycle executions. |
| [Get SLM Status](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/slm.get_status) | Returns the status of the snapshot lifecycle management feature. |
| [Start SLM](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/slm.start) | Starts the snapshot lifecycle management feature. |
| [Stop SLM](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/slm.stop) | Stops the snapshot lifecycle management feature. |

## SQL
[View SQL APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-sql)

| API | Description |
| --- | ----------- |
| [SQL Query](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/sql.query) | Executes an SQL query. |
| [SQL Translate](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/sql.translate) | Translates SQL into Elasticsearch DSL. |
| [SQL Async Query](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/sql.async_query) | Submits an SQL query to run asynchronously. |
| [SQL Async Get](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/sql.get_async) | Retrieves results of an async SQL query. |
| [SQL Async Delete](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/sql.delete_async) | Cancels an async SQL query. |

## Synonyms
[View Synonyms APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-synonyms)

| API | Description |
| --- | ----------- |
| [Put Synonym Set](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/synonyms.put_synonym) | Creates or updates a synonym set. |
| [Get Synonym Set](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/synonyms.get_synonym) | Retrieves a synonym set by ID. |
| [Delete Synonym Set](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/synonyms.delete_synonym) | Deletes a synonym set. |
| [List Synonym Sets](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/synonyms.get_synonyms) | Lists all synonym sets. |

## Task Management
[View Task Management APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks)

| API | Description |
| --- | ----------- |
| [List Tasks](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/tasks.list) | Retrieves information about running tasks. |
| [Cancel Task](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/tasks.cancel) | Cancels a running task. |

## Text Structure
[View Text Structure APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-text_structure)

| API | Description |
| --- | ----------- |
| [Find Structure](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/text_structure.find_structure) | Analyzes a text file and returns its structure. |

## Transform
[View Transform APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-transform)

| API | Description |
| --- | ----------- |
| [Put Transform](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/transform.put_transform) | Creates or updates a transform job. |
| [Get Transform](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/transform.get_transform) | Retrieves configuration for one or more transforms. |
| [Delete Transform](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/transform.delete_transform) | Deletes a transform job. |
| [Start Transform](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/transform.start_transform) | Starts a transform job. |
| [Stop Transform](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/transform.stop_transform) | Stops a running transform job. |
| [Preview Transform](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/transform.preview_transform) | Previews the results of a transform job. |

## Usage Watcher
[View Watcher APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-watcher)

| API | Description |
| --- | ----------- |
| [Put Watch](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.put_watch) | Creates or updates a watch. |
| [Get Watch](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.get_watch) | Retrieves a watch by ID. |
| [Delete Watch](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.delete_watch) | Deletes a watch. |
| [Execute Watch](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.execute_watch) | Executes a watch manually. |
| [Ack Watch](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.ack_watch) | Acknowledges a watch action. |
| [Watcher Stats](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.stats) | Returns statistics about the Watcher service. |
| [Start Watcher](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.start) | Starts the Watcher service. |
| [Stop Watcher](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/watcher.stop) | Stops the Watcher service. |



This section includes:

- [API conventions](/reference/elasticsearch/rest-apis/api-conventions.md)
- [Common options](/reference/elasticsearch/rest-apis/common-options.md)
- [Compatibility](/reference/elasticsearch/rest-apis/compatibility.md)
- [Examples](/reference/elasticsearch/rest-apis/api-examples.md)
