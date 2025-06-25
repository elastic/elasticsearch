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

### [Behavioral analytics](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-analytics)

The behavioral analytics APIs enable you to create and manage analytics collections and retrieve information about analytics collections. Behavioral Analytics is an analytics event collection platform. You can use it to analyze your users' searching and clicking behavior. Leverage this information to improve the relevance of your search results and identify gaps in your content.

| API | Description |
| --- | ----------- |
| [Get Collections](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/behavioral_analytics.get_behavioral_analytics) | Lists all behavioral analytics collections. |
| [Create Collection](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/behavioral_analytics.put_behavioral_analytics) | Creates a new behavioral analytics collection. |
| [Delete Collection](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/behavioral_analytics.delete_behavioral_analytics) | Deletes a behavioral analytics collection. |
| [Create Event](https://www.elastic.co/docs/api/doc/elasticsearch/endpoint/behavioral_analytics.post_behavioral_analytics_event) | Sends a behavioral analytics event to a collection. |

### [Compact and aligned text (CAT)](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-cat)

The compact and aligned text (CAT) APIs return human-readable text as a response, instead of a JSON object. The CAT APIs aim are intended only for human consumption using the Kibana console or command line. They are not intended for use by applications. For application consumption, it's recommend to use a corresponding JSON API.

### [Cluster](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-cluster)

The cluster APIs enable you to retrieve information about your infrastructure on cluster, node, or shard level. You can manage cluster settings and voting configuration exceptions, collect node statistics and retrieve node information.

### [Cluster - Health](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-health_report)

The cluster - health API provides you a report with the health status of an Elasticsearch cluster.

### [Connector](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-connector)

The connector and sync jobs APIs provide a convenient way to create and manage Elastic connectors and sync jobs in an internal index.

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

### [Document](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-document)

The document APIs enable you to create and manage documents in an {{es}} index.

### [Enrich](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-enrich)

The enrich APIs enable you to manage enrich policies. An enrich policy is a set of configuration options used to add the right enrich data to the right incoming documents.

### [EQL](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-eql)

The EQL APIs enable you to run EQL-related operations. Event Query Language (EQL) is a query language for event-based time series data, such as logs, metrics, and traces.

### [ES|QL](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-esql)

The ES|QL APIs enable you to run ES|QL-related operations. The Elasticsearch Query Language (ES|QL) provides a powerful way to filter, transform, and analyze data stored in Elasticsearch, and in the future in other runtimes.

### [Features](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-features)

The feature APIs enable you to introspect and manage features provided by {{es}} and {{es}} plugins.

### [Fleet](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-fleet)

The Fleet APIs support Fleet’s use of Elasticsearch as a data store for internal agent and action data.

### [Graph explore](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-graph)

The graph explore APIs enable you to extract and summarize information about the documents and terms in an {{es}} data stream or index.

### [Index](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-indices)

The index APIs enable you to manage individual indices, index settings, aliases, mappings, and index templates.

### [Index lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ilm)

The index lifecycle management APIs enable you to set up policies to automatically manage the index lifecycle.

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

### [Ingest](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ingest)

The ingest APIs enable you to manage tasks and resources related to ingest pipelines and processors.

### [Licensing](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-license)

The licensing APIs enable you to manage your licenses.

### [Logstash](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-logstash)

The logstash APIs enable you to manage pipelines that are used by Logstash Central Management.

### [Machine learning](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml)

The machine learning APIs enable you to retrieve information related to the {{stack}} {{ml}} features.

### [Machine learning anomaly detection](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml-anomaly)

The machine learning anomaly detection APIs enbale you to perform anomaly detection activities.

### [Machine learning data frame analytics](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml-data-frame)

The machine learning data frame analytics APIs enbale you to perform data frame analytics activities.

### [Machine learning trained models](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-ml-trained-model)

The machine learning trained models APIs enable you to perform model management operations.

### [Migration](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-migration)

The migration APIs power {{kib}}'s Upgrade Assistant feature.

### [Node lifecycle](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-shutdown)

The node lifecycle APIs enable you to prepare nodes for temporary or permanent shutdown, monitor the shutdown status, and enable a previously shut-down node to resume normal operations.

### [Query rules](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-query_rules)

Query rules enable you to configure per-query rules that are applied at query time to queries that match the specific rule. Query rules are organized into rulesets, collections of query rules that are matched against incoming queries. Query rules are applied using the rule query. If a query matches one or more rules in the ruleset, the query is re-written to apply the rules before searching. This allows pinning documents for only queries that match a specific term.

### [Rollup](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-rollup)

The rollup APIs enable you to create, manage, and retrieve infromation about rollup jobs.

### [Script](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-script)

Use the script support APIs to get a list of supported script contexts and languages. Use the stored script APIs to manage stored scripts and search templates.

### [Search](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-search)

The search APIs enable you to search and aggregate data stored in {{es}} indices and data streams.

### [Search application](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-search_application)

The search applcation APIs enable you to manage tasks and resources related to Search Applications.

### [Searchable snapshots](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-searchable_snapshots)

The searchable snapshots APIs enable you to perform searchable snapshots operations.

### [Security](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-security)

The security APIs enable you to perform security activities, and add, update, retrieve, and remove application privileges, role mappings, and roles. You can also create and update API keys and create and invalidate bearer tokens.

### [Snapshot and restore](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-snapshot)

The snapshot and restore APIs enable you to set up snapshot repositories, manage snapshot backups, and restore snapshots to a running cluster.

### [Snapshot lifecycle management](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-slm)

The snapshot lifecycle management APIs enable you to set up policies to automatically take snapshots and control how long they are retained.

### [SQL](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-sql)

The SQL APIs enable you to run SQL queries on Elasticsearch indices and data streams.

### [Synonyms](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-synonyms)

The synonyms management APIs provide a convenient way to define and manage synonyms in an internal system index. Related synonyms can be grouped in a "synonyms set".

### [Task management](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-tasks)

The task management APIs enable you to retrieve information about tasks or cancel tasks running in a cluster.

### [Text structure](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-text_structure)

The text structure APIs enable you to find the structure of a text field in an {{es}} index.

### [Transform](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-transform)

The transform APIs enable you to create and manage transforms.

### [Usage](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-xpack)

The usage API provides usage information about the installed X-Pack features.

### [Watcher](https://www.elastic.co/docs/api/doc/elasticsearch/v9/group/endpoint-watcher)

You can use Watcher to watch for changes or anomalies in your data and perform the necessary actions in response.
