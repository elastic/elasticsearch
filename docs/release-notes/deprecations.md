---
navigation_title: "Deprecations"
---

# {{es}} deprecations [elasticsearch-deprecations]

Over time, certain Elastic functionality becomes outdated and is replaced or removed. To help with the transition, Elastic deprecates functionality for a period before removal, giving you time to update your applications.

Review the deprecated functionality for Elasticsearch. While deprecations have no immediate impact, we strongly encourage you update your implementation after you upgrade. To learn how to upgrade, check out [Upgrade](docs-content://deploy-manage/upgrade.md).

To give you insight into what deprecated features you’re using, {{es}}:

* Returns a `Warn` HTTP header whenever you submit a request that uses deprecated functionality.
* [Logs deprecation warnings](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md#deprecation-logging) when deprecated functionality is used.
* [Provides a deprecation info API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-deprecations) that scans a cluster’s configuration and mappings for deprecated functionality.

% ## Next version [elasticsearch-nextversion-deprecations]

```{applies_to}
stack: coming 9.0.2
```
## 9.0.2 [elasticsearch-9.0.2-deprecations]

No deprecations in this version.

## 9.0.1 [elasticsearch-9.0.1-deprecations]

No deprecations in this version.

## 9.0.0 [elasticsearch-900-deprecations]

ES|QL:
* Drop support for brackets from METADATA syntax [#119846](https://github.com/elastic/elasticsearch/pull/119846) (issue: [#115401](https://github.com/elastic/elasticsearch/issues/115401))

Ingest Node:
* Fix `_type` deprecation on simulate pipeline API [#116259](https://github.com/elastic/elasticsearch/pull/116259)

Machine Learning:
* Add deprecation warning for flush API [#121667](https://github.com/elastic/elasticsearch/pull/121667) (issue: [#121506](https://github.com/elastic/elasticsearch/issues/121506))
* Removing index alias creation for deprecated transforms notification index [#117583](https://github.com/elastic/elasticsearch/pull/117583)
* [Inference API] Deprecate elser service [#113216](https://github.com/elastic/elasticsearch/pull/113216)

Rollup:
* Emit deprecation warning when executing one of the rollup APIs [#113131](https://github.com/elastic/elasticsearch/pull/113131)

Search:
* Deprecate Behavioral Analytics CRUD apis [#122960](https://github.com/elastic/elasticsearch/pull/122960)

Security:
* Deprecate certificate based remote cluster security model [#120806](https://github.com/elastic/elasticsearch/pull/120806)


