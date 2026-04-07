---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-request-cache-settings.html
navigation_title: Shard request cache
applies_to:
  deployment:
    self:
---
# Shard request cache settings [shard-request-cache-settings]

The following settings affect the behavior of the [shard request cache](/reference/elasticsearch/rest-apis/shard-request-cache.md).

## Cache settings [_cache_settings]

`indices.requests.cache.size`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) The maximum size of the cache, as a percentage of the heap. Default: `1%`.

`indices.requests.cache.expire`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) The TTL for cached results. Stale results are automatically invalidated when the index is refreshed, so you shouldn’t need to use this setting.
