---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-fielddata.html
applies_to:
  deployment:
    self:
---

# Field data cache settings [modules-fielddata]

The field data cache contains [field data](/reference/elasticsearch/mapping-reference/text.md#fielddata-mapping-param) and [global ordinals](/reference/elasticsearch/mapping-reference/eager-global-ordinals.md), which are both used to support aggregations on certain field types. Since these are on-heap data structures, it is important to monitor the cache’s use.

The entries in the cache are expensive to build, so the default behavior is to keep the cache loaded in memory. The default cache size is unlimited, causing the cache to grow until it reaches the limit set by the [field data circuit breaker](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#fielddata-circuit-breaker). This behavior can be configured.

If the cache size limit is set, the cache will begin clearing the least-recently-updated entries in the cache. This setting can automatically avoid the circuit breaker limit, at the cost of rebuilding the cache as needed.

If the circuit breaker limit is reached, further requests that increase the cache size will be prevented. In this case you should manually [clear the cache](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-clear-cache).

::::{tip}
You can monitor memory usage for field data as well as the field data circuit breaker using the [nodes stats API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-stats) or the [cat fielddata API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-fielddata).
::::


`indices.fielddata.cache.size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) The max size of the field data cache, eg `38%` of node heap space, or an absolute value, eg `12GB`. Defaults to unbounded. If you choose to set it, it should be smaller than [Field data circuit breaker](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#fielddata-circuit-breaker) limit.

