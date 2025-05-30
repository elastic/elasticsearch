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
stack: coming 9.1.0
```
## 9.1.0 [elasticsearch-9.1.0-deprecations]

No deprecations in this version.

```{applies_to}
stack: coming 9.0.10
```
## 9.0.10 [elasticsearch-9.0.10-deprecations]

No deprecations in this version.

## 9.0.9 [elasticsearch-9.0.9-deprecations]

No deprecations in this version.

## 9.0.8 [elasticsearch-9.0.8-deprecations]

No deprecations in this version.

## 9.0.7 [elasticsearch-9.0.7-deprecations]

No deprecations in this version.

## 9.0.6 [elasticsearch-9.0.6-deprecations]

No deprecations in this version.

## 9.0.5 [elasticsearch-9.0.5-deprecations]

No deprecations in this version.

## 9.0.4 [elasticsearch-9.0.4-deprecations]

Search:
* Test changelog entry 4_0 [#4000](https://github.com/elastic/elasticsearch/pull/4000) (issue: [#4001](https://github.com/elastic/elasticsearch/issues/4001))
* Test changelog entry 4_1 [#4002](https://github.com/elastic/elasticsearch/pull/4002) (issues: [#4003](https://github.com/elastic/elasticsearch/issues/4003), [#4004](https://github.com/elastic/elasticsearch/issues/4004))



## 9.0.3 [elasticsearch-9.0.3-deprecations]

No deprecations in this version.

## 9.0.2 [elasticsearch-9.0.2-deprecations]

No deprecations in this version.

## 9.0.1 [elasticsearch-9.0.1-deprecations]

No deprecations in this version.

## 9.0.0 [elasticsearch-900-deprecations]

No deprecations in this version.
