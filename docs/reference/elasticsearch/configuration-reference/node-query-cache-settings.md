---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-cache.html
applies_to:
  deployment:
    self:
---

# Node query cache settings [query-cache]

The results of queries used in the filter context are cached in the node query cache for fast lookup. There is one query cache per node that is shared by all shards. The cache uses an LRU eviction policy: when the cache is full, the least recently used query results are evicted to make way for new data. You cannot inspect the contents of the query cache.

Term queries and queries used outside of a filter context are not eligible for caching.

By default, the cache holds a maximum of 10000 queries in up to 10% of the total heap space. To determine if a query is eligible for caching, {{es}} maintains a query history to track occurrences.

Caching is done on a per segment basis if a segment contains at least 10000 documents and the segment has at least 3% of the total documents of a shard. Because caching is per segment, merging segments can invalidate cached queries.

The following setting is *static* and must be configured on every data node in the cluster:

`indices.queries.cache.size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Controls the memory size for the filter cache. Accepts either a percentage value, like `5%`, or an exact value, like `512mb`. Defaults to `10%`.

## Query cache index settings [query-cache-index-settings]

The following setting is an *index* setting that can be configured on a per-index basis. Can only be set at index creation time or on a [closed index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-open):

`index.queries.cache.enabled`
:   ([Static](/reference/elasticsearch/index-settings/index.md)) Controls whether to enable query caching. Accepts `true` (default) or `false`.


