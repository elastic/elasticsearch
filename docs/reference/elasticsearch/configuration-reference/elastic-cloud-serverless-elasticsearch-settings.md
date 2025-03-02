---
navigation_title: "Serverless differences"
mapped_pages:
  - https://www.elastic.co/guide/en/serverless/current/elasticsearch-differences.html
---

# Differences from other {{es}} offerings [elasticsearch-differences]


[{{es-serverless}}](docs-content://solutions/search.md) handles all the infrastructure management for you, providing a fully managed {{es}} service.

If you’ve used {{es}} before, you’ll notice some differences in how you work with the service on {{serverless-full}}, because a number of APIs and settings are not required for serverless projects.

This guide helps you understand what’s different, what’s available, and how to work effectively when running {{es}} on {{serverless-full}}.


## Fully managed infrastructure [elasticsearch-differences-serverless-infrastructure-management]

{{es-serverless}} manages all infrastructure automatically, including:

* Cluster scaling and optimization
* Node management and allocation
* Shard distribution and replication
* Resource utilization and monitoring

This fully managed approach means many traditional {{es}} infrastructure APIs and settings are not available to end users, as detailed in the following sections.


## Index size guidelines [elasticsearch-differences-serverless-index-size]

To ensure optimal performance, follow these recommendations for sizing individual indices on {{es-serverless}}:

| Use case | Maximum index size | Project configuration |
| --- | --- | --- |
| Vector search | 150GB | Vector optimized |
| General search (non data-stream) | 300GB | General purpose |
| Other uses (non data-stream) | 600GB | General purpose |

For large datasets that exceed the recommended maximum size for a single index, consider splitting your data across smaller indices and using an alias to search them collectively.

These recommendations do not apply to indices using better binary quantization (BBQ). Refer to [vector quantization](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization) in the core {{es}} docs for more information.


## API availability [elasticsearch-differences-serverless-apis-availability]

Because {{es-serverless}} manages infrastructure automatically, certain APIs are not available, while others remain fully accessible.

::::{tip}
Refer to the [{{es-serverless}} API reference](https://www.elastic.co/docs/api/doc/elasticsearch-serverless) for a complete list of available APIs.

::::


The following categories of operations are unavailable:

Infrastructure operations
:   * All `_nodes/*` operations
* All `_cluster/*` operations
* Most `_cat/*` operations, except for index-related operations such as `/_cat/indices` and `/_cat/aliases`


Storage and backup
:   * All `_snapshot/*` operations
* Repository management operations


Index management
:   * `indices/close` operations
* `indices/open` operations
* Recovery and stats operations
* Force merge operations


When attempting to use an unavailable API, you’ll receive a clear error message:

```json
{
 "error": {
   "root_cause": [
     {
       "type": "api_not_available_exception",
       "reason": "Request for uri [/<API_ENDPOINT>] with method [<METHOD>] exists but is not available when running in serverless mode"
     }
   ],
   "status": 410
 }
}
```


## Settings availability [elasticsearch-differences-serverless-settings-availability]

In {{es-serverless}}, you can only configure [index-level settings](/reference/elasticsearch/index-settings/index.md). Cluster-level settings and node-level settings are not required by end users and the `elasticsearch.yml` file is fully managed by Elastic.

Available settings
:   **Index-level settings**: Settings that control how {{es}} documents are processed, stored, and searched are available to end users. These include:

    * Analysis configuration
    * Mapping parameters
    * Search/query settings
    * Indexing settings such as `refresh_interval`


Managed settings
:   **Infrastructure-related settings**: Settings that affect cluster resources or data distribution are not available to end users. These include:

    * Node configurations
    * Cluster topology
    * Shard allocation
    * Resource management



## Feature availability [elasticsearch-differences-serverless-feature-categories]

Some features that are available in Elastic Cloud Hosted and self-managed offerings are not available in {{es-serverless}}. These features have either been replaced by a new feature, are planned to be released in future, or are not applicable in the new serverless architecture.


### Replaced features [elasticsearch-differences-serverless-features-replaced]

These features have been replaced by a new feature and are therefore not available on {{es-serverless}}:

* **Index lifecycle management ({{ilm-init}})** is not available, in favor of [**data stream lifecycle**](docs-content://manage-data/data-store/index-basics.md).

    In an Elastic Cloud Hosted or self-managed environment, {{ilm-init}} lets you automatically transition indices through data tiers according to your performance needs and retention requirements. This allows you to balance hardware costs with performance. {{es-serverless}} eliminates this complexity by optimizing your cluster performance for you.

    Data stream lifecycle is an optimized lifecycle tool that lets you focus on the most common lifecycle management needs, without unnecessary hardware-centric concepts like data tiers.

* **Watcher** is not available, in favor of [**Alerts**](docs-content://explore-analyze/alerts-cases/alerts.md#rules-alerts).

    Kibana Alerts allows rich integrations across use cases like APM, metrics, security, and uptime. Prepackaged rule types simplify setup and hide the details of complex, domain-specific detections, while providing a consistent interface across Kibana.



### Planned features [elasticsearch-differences-serverless-feature-planned]

The following features are planned for future support in all {{serverless-full}} projects:

* Reindexing from remote clusters
* Cross-project search and replication
* Snapshot and restore
* Migrations from non-serverless deployments
* Audit logging
* Clone index API
* Traffic filtering and VPCs



### Unplanned features [elasticsearch-differences-serverless-feature-unavailable]

The following features are not available in {{es-serverless}} and are not planned for future support:

* [Custom plugins and bundles](docs-content://deploy-manage/deploy/elastic-cloud/upload-custom-plugins-bundles.md)
* {{es}} for Apache Hadoop
* [Scripted metric aggregations](/reference/data-analysis/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md)
* Managed web crawler: You can use the [self-managed web crawler](https://github.com/elastic/crawler) instead.
* Managed Search connectors: You can use [self-managed Search connectors](/reference/ingestion-tools/search-connectors/self-managed-connectors.md) instead.
