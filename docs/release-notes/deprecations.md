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

## 9.1.0 [elasticsearch-910-deprecations]

No deprecations in this version.
