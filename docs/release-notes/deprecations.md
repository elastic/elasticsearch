---
navigation_title: "Elasticsearch"
---

# {{es}} deprecations [elasticsearch-deprecations]
Review the deprecated functionality for your {{es}} version. While deprecations have no immediate impact, we strongly encourage you update your implementation after you upgrade.

To learn how to upgrade, check out <uprade docs>.

To give you insight into what deprecated features you’re using, {{es}}:

* Returns a `Warn` HTTP header whenever you submit a request that uses deprecated functionality.
* [Logs deprecation warnings](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md#deprecation-logging) when deprecated functionality is used.
* [Provides a deprecation info API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-deprecations) that scans a cluster’s configuration and mappings for deprecated functionality.

% ## Next version
% **Release date:** Month day, year

## 9.0.0 [elasticsearch-900-deprecations]
**Release date:** March 25, 2025

Ingest Node
:   * Fix `_type` deprecation on simulate pipeline API [#116259](https://github.com/elastic/elasticsearch/pull/116259)


Machine Learning
:   * [Inference API] Deprecate elser service [#113216](https://github.com/elastic/elasticsearch/pull/113216)


Mapping
:   * Deprecate `_source.mode` in mappings [#116689](https://github.com/elastic/elasticsearch/pull/116689)
