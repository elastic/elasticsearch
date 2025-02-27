---
navigation_title: "Elasticsearch"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes.html
---

# Elasticsearch breaking changes [elasticsearch-breaking-changes]
Before you upgrade, carefully review the Elasticsearch breaking changes and take the necessary steps to mitigate any issues. 

To learn how to upgrade, check out <uprade docs>.

% ## Next version [elasticsearch-nextversion-breaking-changes]
% **Release date:** Month day, year

## 9.0.0 [elasticsearch-900-breaking-changes]
**Release date:** March 25, 2025

Allocation
:   * Increase minimum threshold in shard balancer [#115831](https://github.com/elastic/elasticsearch/pull/115831)
* Remove `cluster.routing.allocation.disk.watermark.enable_for_single_data_node` setting [#114207](https://github.com/elastic/elasticsearch/pull/114207)
* Remove cluster state from `/_cluster/reroute` response [#114231](https://github.com/elastic/elasticsearch/pull/114231) (issue: [#88978](https://github.com/elastic/elasticsearch/issues/88978))

Analysis
:   * Snowball stemmers have been upgraded [#114146](https://github.com/elastic/elasticsearch/pull/114146)
* The *german2* stemmer is now an alias for the *german* snowball stemmer [#113614](https://github.com/elastic/elasticsearch/pull/113614)
* The *persian* analyzer has stemmer by default [#113482](https://github.com/elastic/elasticsearch/pull/113482) (issue: [#113050](https://github.com/elastic/elasticsearch/issues/113050))
* The Korean dictionary for Nori has been updated [#114124](https://github.com/elastic/elasticsearch/pull/114124)


Cluster Coordination
:   * Remove unsupported legacy value for `discovery.type` [#112903](https://github.com/elastic/elasticsearch/pull/112903)


Highlighting
:   * Remove support for deprecated `force_source` highlighting parameter [#116943](https://github.com/elastic/elasticsearch/pull/116943)


Indices APIs
:   * Apply more strict parsing of actions in bulk API [#115923](https://github.com/elastic/elasticsearch/pull/115923)
* Remove deprecated local attribute from alias APIs [#115393](https://github.com/elastic/elasticsearch/pull/115393)


Infra/REST API
:   * Output a consistent format when generating error json [#90529](https://github.com/elastic/elasticsearch/pull/90529) (issue: [#89387](https://github.com/elastic/elasticsearch/issues/89387))


Ingest Node
:   * Remove `ecs` option on `user_agent` processor [#116077](https://github.com/elastic/elasticsearch/pull/116077)
* Remove ignored fallback option on GeoIP processor [#116112](https://github.com/elastic/elasticsearch/pull/116112)


Mapping
:   * Remove support for type, fields, `copy_to` and boost in metadata field definition [#116944](https://github.com/elastic/elasticsearch/pull/116944)


Search
:   * Remove legacy params from range query [#116970](https://github.com/elastic/elasticsearch/pull/116970)


Snapshot/Restore
:   * Remove deprecated `xpack.searchable.snapshot.allocate_on_rolling_restart` setting [#114202](https://github.com/elastic/elasticsearch/pull/114202)

