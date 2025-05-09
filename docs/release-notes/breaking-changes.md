---
navigation_title: "Breaking changes"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes.html
---

# Elasticsearch breaking changes [elasticsearch-breaking-changes]

Breaking changes can impact your Elastic applications, potentially disrupting normal operations. Before you upgrade, carefully review the Elasticsearch breaking changes and take the necessary steps to mitigate any issues.

If you are migrating from a version prior to version 9.0, you must first upgrade to the last 8.x version available. To learn how to upgrade, check out [Upgrade](docs-content://deploy-manage/upgrade.md).

% ## Next version [elasticsearch-nextversion-breaking-changes]

## 9.0.1 [elasticsearch-9.0.1-breaking-changes]

No breaking changes in this version.

## 9.0.0 [elasticsearch-900-breaking-changes]

Aggregations:
* Remove date histogram boolean support [#118484](https://github.com/elastic/elasticsearch/pull/118484)

Allocation:
* Increase minimum threshold in shard balancer [#115831](https://github.com/elastic/elasticsearch/pull/115831)
* Remove `cluster.routing.allocation.disk.watermark.enable_for_single_data_node` setting [#114207](https://github.com/elastic/elasticsearch/pull/114207)
* Remove cluster state from `/_cluster/reroute` response [#114231](https://github.com/elastic/elasticsearch/pull/114231) (issue: [#88978](https://github.com/elastic/elasticsearch/issues/88978))

Analysis:
* Snowball stemmers have been upgraded [#114146](https://github.com/elastic/elasticsearch/pull/114146)
* The 'german2' stemmer is now an alias for the 'german' snowball stemmer [#113614](https://github.com/elastic/elasticsearch/pull/113614)
* The 'persian' analyzer has stemmer by default [#113482](https://github.com/elastic/elasticsearch/pull/113482) (issue: [#113050](https://github.com/elastic/elasticsearch/issues/113050))
* The Korean dictionary for Nori has been updated [#114124](https://github.com/elastic/elasticsearch/pull/114124)

Authentication:
* Configuring a bind DN in an LDAP or Active Directory (AD) realm without a corresponding bind password
will prevent node from starting [#118366](https://github.com/elastic/elasticsearch/pull/118366)

Cluster Coordination:
* Remove unsupported legacy value for `discovery.type` [#112903](https://github.com/elastic/elasticsearch/pull/112903)

EQL:
* Set allow_partial_search_results=true by default [#120267](https://github.com/elastic/elasticsearch/pull/120267)

Extract&Transform:
* Restrict Connector APIs to manage/monitor_connector privileges [#119863](https://github.com/elastic/elasticsearch/pull/119863)

Highlighting:
* Remove support for deprecated `force_source` highlighting parameter [#116943](https://github.com/elastic/elasticsearch/pull/116943)

Indices APIs:
* Apply more strict parsing of actions in bulk API [#115923](https://github.com/elastic/elasticsearch/pull/115923)
* Remove deprecated local attribute from alias APIs [#115393](https://github.com/elastic/elasticsearch/pull/115393)
* Remove the ability to read frozen indices [#120108](https://github.com/elastic/elasticsearch/pull/120108)
* Remove unfreeze REST endpoint [#119227](https://github.com/elastic/elasticsearch/pull/119227)

Infra/Core:
* Change Elasticsearch timeouts to 429 response instead of 5xx [#116026](https://github.com/elastic/elasticsearch/pull/116026)
* Limit `ByteSizeUnit` to 2 decimals [#120142](https://github.com/elastic/elasticsearch/pull/120142)
* Remove `client.type` setting [#118192](https://github.com/elastic/elasticsearch/pull/118192) (issue: [#104574](https://github.com/elastic/elasticsearch/issues/104574))
* Remove any references to org.elasticsearch.core.RestApiVersion#V_7 [#118103](https://github.com/elastic/elasticsearch/pull/118103)

Infra/Logging:
* Change `deprecation.elasticsearch` keyword to `elasticsearch.deprecation` [#117933](https://github.com/elastic/elasticsearch/pull/117933) (issue: [#83251](https://github.com/elastic/elasticsearch/issues/83251))
* Rename deprecation index template [#125606](https://github.com/elastic/elasticsearch/pull/125606) (issue: [#125445](https://github.com/elastic/elasticsearch/issues/125445))

Infra/Metrics:
* Deprecated tracing.apm.* settings got removed. [#119926](https://github.com/elastic/elasticsearch/pull/119926)

Infra/REST API:
* Output a consistent format when generating error json [#90529](https://github.com/elastic/elasticsearch/pull/90529) (issue: [#89387](https://github.com/elastic/elasticsearch/issues/89387))

Ingest Node:
* Remove `ecs` option on `user_agent` processor [#116077](https://github.com/elastic/elasticsearch/pull/116077)
* Remove ignored fallback option on GeoIP processor [#116112](https://github.com/elastic/elasticsearch/pull/116112)

Logs:
* Conditionally enable logsdb by default for data streams matching with logs-*-* pattern. [#121049](https://github.com/elastic/elasticsearch/pull/121049) (issue: [#106489](https://github.com/elastic/elasticsearch/issues/106489))

Machine Learning:
* Disable machine learning on macOS x86_64 [#104125](https://github.com/elastic/elasticsearch/pull/104125)

Mapping:
* Remove support for type, fields, `copy_to` and boost in metadata field definition [#118825](https://github.com/elastic/elasticsearch/pull/118825)
* Turn `_source` meta fieldmapper's mode attribute into a no-op [#119072](https://github.com/elastic/elasticsearch/pull/119072) (issue: [#118596](https://github.com/elastic/elasticsearch/issues/118596))

Search:
* Adjust `random_score` default field to `_seq_no` field [#118671](https://github.com/elastic/elasticsearch/pull/118671)
* Change Semantic Text To Act Like A Normal Text Field [#120813](https://github.com/elastic/elasticsearch/pull/120813)
* Remove legacy params from range query [#116970](https://github.com/elastic/elasticsearch/pull/116970)

Snapshot/Restore:
* Remove deprecated `xpack.searchable.snapshot.allocate_on_rolling_restart` setting [#114202](https://github.com/elastic/elasticsearch/pull/114202)

TLS:
* Drop `TLS_RSA` cipher support for JDK 24 [#123600](https://github.com/elastic/elasticsearch/pull/123600)
* Remove TLSv1.1 from default protocols [#121731](https://github.com/elastic/elasticsearch/pull/121731)

Transform:
* Remove `data_frame_transforms` roles [#117519](https://github.com/elastic/elasticsearch/pull/117519)

Vector Search:
* Remove old `_knn_search` tech preview API in v9 [#118104](https://github.com/elastic/elasticsearch/pull/118104)

Watcher:
* Removing support for types field in watcher search [#120748](https://github.com/elastic/elasticsearch/pull/120748)


