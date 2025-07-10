---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-request-cache.html
---
# The shard request cache [shard-request-cache]

When a search request is run against an index or against many indices, each involved shard runs the search locally and returns its local results to the coordinating node, which combines these shard-level results into a global result set.

The shard-level request cache module caches the local results on each shard. This allows frequently used (and potentially heavy) search requests to return results almost instantly. The requests cache is a very good fit for the logging use case, where only the most recent index is being actively updated — results from older indices will be served directly from the cache.

You can control the size and expiration of the cache at the node level using the [shard request cache settings](/reference/elasticsearch/configuration-reference/shard-request-cache-settings.md).

::::{important}
By default, the request cache will only cache the results of search requests where `size=0`, so it will not cache `hits` but it will cache `hits.total`, aggregations, and suggestions.

Most queries that use `now` cannot be cached. For more information about `now`, refer to [](/reference/elasticsearch/rest-apis/common-options.md#date-math).

Scripted queries that use the API calls which are non-deterministic, such as `Math.random()` or `new Date()` are not cached.
::::

## Cache invalidation [_cache_invalidation]

The cache is smart — it keeps the same *near real-time* promise as uncached search.

Cached results are invalidated automatically whenever the shard refreshes to pick up changes to the documents or when you update the mapping. In other words you will always get the same results from the cache as you would for an uncached search request.

The longer the refresh interval, the longer that cached entries will remain valid even if there are changes to the documents. If the cache is full, the least recently used cache keys will be evicted.

The cache can be expired manually with the [clear cache API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-clear-cache):

```console
POST /my-index-000001,my-index-000002/_cache/clear?request=true
```

## Enabling and disabling caching [_enabling_and_disabling_caching]

The cache is enabled by default, but can be disabled when creating a new index as follows:

```console
PUT /my-index-000001
{
  "settings": {
    "index.requests.cache.enable": false
  }
}
```

It can also be enabled or disabled dynamically on an existing index with the [update settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings):

```console
PUT /my-index-000001/_settings
{
  "index.requests.cache.enable": true
}
```

## Enabling and disabling caching per request [_enabling_and_disabling_caching_per_request]

The `request_cache` query parameter can be used to enable or disable caching on a per-request basis. If set, it overrides the index-level setting:

```console
GET /my-index-000001/_search?request_cache=true
{
  "size": 0,
  "aggs": {
    "popular_colors": {
      "terms": {
        "field": "colors"
      }
    }
  }
}
```

Requests where `size` is greater than `0` will not be cached even if the request cache is enabled in the index settings. To cache these requests you will need to use the query parameter.

## Cache key [_cache_key]

A hash of the whole JSON body is used as the cache key. This means that if the JSON changes — for instance if keys are output in a different order — then the cache key will not be recognised.

::::{tip}
Most JSON libraries support a canonical mode, which ensures that JSON keys are always emitted in the same order. This canonical mode can be used in the application to ensure that a request is always serialized in the same way.
::::

## Monitoring cache usage [_monitoring_cache_usage]

The size of the cache (in bytes) and the number of evictions can be viewed by index, with the [indices stats API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-stats):

```console
GET /_stats/request_cache?human
```

Alternatively, view them by node with the [nodes stats API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-stats):

```console
GET /_nodes/stats/indices/request_cache?human
```