---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
applies_to:
  stack: all
---

# Paginate search results [paginate-search-results]

By default, searches return the top 10 matching hits. To page through a larger set of results, you can use the [search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search)'s `from` and `size` parameters. The `from` parameter defines the number of hits to skip, defaulting to `0`. The `size` parameter is the maximum number of hits to return. Together, these two parameters define a page of results.

```console
GET /_search
{
  "from": 5,
  "size": 20,
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  }
}
```

Avoid using `from` and `size` to page too deeply or request too many results at once. Search requests usually span multiple shards. Each shard must load its requested hits and the hits for any previous pages into memory. For deep pages or large sets of results, these operations can significantly increase memory and CPU usage, resulting in degraded performance or node failures.

By default, you cannot use `from` and `size` to page through more than 10,000 hits. This limit is a safeguard set by the [`index.max_result_window`](/reference/elasticsearch/index-settings/index-modules.md#index-max-result-window) index setting. If you need to page through more than 10,000 hits, use the [`search_after`](#search-after) parameter instead.

::::{warning}
{{es}} uses Lucene’s internal doc IDs as tie-breakers. These internal doc IDs can be completely different across replicas of the same data. When paging search hits, you might occasionally see that documents with the same sort values are not ordered consistently.
::::



## Search after [search-after]

You can use the `search_after` parameter to retrieve the next page of hits using a set of [sort values](/reference/elasticsearch/rest-apis/sort-search-results.md) from the previous page.

Using `search_after` requires multiple search requests with the same `query` and `sort` values. The first step is to run an initial request. The following example sorts the results by two fields (`date` and `tie_breaker_id`):

```console
GET twitter/_search
{
    "query": {
        "match": {
            "title": "elasticsearch"
        }
    },
    "sort": [
        {"date": "asc"},
        {"tie_breaker_id": "asc"}      <1>
    ]
}
```
% TEST[continued]

1. A copy of the `_id` field with `doc_values` enabled


The search response includes an array of `sort` values for each hit:

```console-result
{
  "took" : 17,
  "timed_out" : false,
  "_shards" : ...,
  "hits" : {
    "total" : ...,
    "max_score" : null,
    "hits" : [
      ...
      {
        "_index" : "twitter",
        "_id" : "654322",
        "_score" : null,
        "_source" : ...,
        "sort" : [
          1463538855,
          "654322"
        ]
      },
      {
        "_index" : "twitter",
        "_id" : "654323",
        "_score" : null,
        "_source" : ...,
        "sort" : [                                <1>
          1463538857,
          "654323"
        ]
      }
    ]
  }
}
```
% TESTRESPONSE[skip: demo of where the sort values are]

1. Sort values for the last returned hit.


To retrieve the next page of results, repeat the request, take the `sort` values from the last hit, and insert those into the `search_after` array:

```console
GET twitter/_search
{
    "query": {
        "match": {
            "title": "elasticsearch"
        }
    },
    "search_after": [1463538857, "654323"],
    "sort": [
        {"date": "asc"},
        {"tie_breaker_id": "asc"}
    ]
}
```
% TEST[continued]

Repeat this process by updating the `search_after` array every time you retrieve a new page of results. If a [refresh](docs-content://manage-data/data-store/near-real-time-search.md) occurs between these requests, the order of your results may change, causing inconsistent results across pages. To prevent this, you can create a [point in time (PIT)](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-open-point-in-time) to preserve the current index state over your searches.

```console
POST /my-index-000001/_pit?keep_alive=1m
```
% TEST[setup:my_index]

The API returns a PIT ID.

```console-result
{
  "id": "46ToAwMDaWR5BXV1aWQyKwZub2RlXzMAAAAAAAAAACoBYwADaWR4BXV1aWQxAgZub2RlXzEAAAAAAAAAAAEBYQADaWR5BXV1aWQyKgZub2RlXzIAAAAAAAAAAAwBYgACBXV1aWQyAAAFdXVpZDEAAQltYXRjaF9hbGw_gAAAAA==",
  "_shards": ...
}
```
% TESTRESPONSE[s/"id": "46ToAwMDaWR5BXV1aWQyKwZub2RlXzMAAAAAAAAAACoBYwADaWR4BXV1aWQxAgZub2RlXzEAAAAAAAAAAAEBYQADaWR5BXV1aWQyKgZub2RlXzIAAAAAAAAAAAwBYgACBXV1aWQyAAAFdXVpZDEAAQltYXRjaF9hbGw_gAAAAA=="/"id": $body.id/]
% TESTRESPONSE[s/"_shards": \.\.\./"_shards": "$body._shards"/]

To get the first page of results, submit a search request with a `sort` argument. If using a PIT, specify the PIT ID in the `pit.id` parameter and omit the target data stream or index from the request path.

::::{important}
All PIT search requests add an implicit sort tiebreaker field called `_shard_doc`, which can also be provided explicitly. If you cannot use a PIT, we recommend that you include a tiebreaker field in your `sort`. This tiebreaker field should contain a unique value for each document. If you don’t include a tiebreaker field, your paged results could miss or duplicate hits.
::::


::::{note}
Search after requests have optimizations that make them faster when the sort order is `_shard_doc` and total hits are not tracked. If you want to iterate over all documents regardless of the order, this is the most efficient option.
::::


::::{important}
If the `sort` field is a [`date`](/reference/elasticsearch/mapping-reference/date.md) in some target data streams or indices but a [`date_nanos`](/reference/elasticsearch/mapping-reference/date_nanos.md) field in other targets, use the `numeric_type` parameter to convert the values to a single resolution and the `format` parameter to specify a [date format](/reference/elasticsearch/mapping-reference/mapping-date-format.md) for the `sort` field. Otherwise, {{es}} won’t interpret the search after parameter correctly in each request.
::::


```console
GET /_search
{
  "size": 10000,
  "query": {
    "match" : {
      "user.id" : "elkbee"
    }
  },
  "pit": {
    "id":  "46ToAwMDaWR5BXV1aWQyKwZub2RlXzMAAAAAAAAAACoBYwADaWR4BXV1aWQxAgZub2RlXzEAAAAAAAAAAAEBYQADaWR5BXV1aWQyKgZub2RlXzIAAAAAAAAAAAwBYgACBXV1aWQyAAAFdXVpZDEAAQltYXRjaF9hbGw_gAAAAA==", <1>
    "keep_alive": "1m"
  },
  "sort": [ <2>
    {"@timestamp": {"order": "asc", "format": "strict_date_optional_time_nanos", "numeric_type" : "date_nanos" }}
  ]
}
```
% TEST[catch:unavailable]

1. PIT ID for the search.
2. Sorts hits for the search with an implicit tiebreak on `_shard_doc` ascending.


The search response includes an array of `sort` values for each hit. If you used a PIT, a tiebreaker is included as the last `sort` values for each hit. This tiebreaker called `_shard_doc` is added automatically on every search requests that use a PIT. The `_shard_doc` value is the combination of the shard index within the PIT and the Lucene’s internal doc ID, it is unique per document and constant within a PIT. You can also add the tiebreaker explicitly in the search request to customize the order:

```console
GET /_search
{
  "size": 10000,
  "query": {
    "match" : {
      "user.id" : "elkbee"
    }
  },
  "pit": {
    "id":  "46ToAwMDaWR5BXV1aWQyKwZub2RlXzMAAAAAAAAAACoBYwADaWR4BXV1aWQxAgZub2RlXzEAAAAAAAAAAAEBYQADaWR5BXV1aWQyKgZub2RlXzIAAAAAAAAAAAwBYgACBXV1aWQyAAAFdXVpZDEAAQltYXRjaF9hbGw_gAAAAA==", <1>
    "keep_alive": "1m"
  },
  "sort": [ <2>
    {"@timestamp": {"order": "asc", "format": "strict_date_optional_time_nanos"}},
    {"_shard_doc": "desc"}
  ]
}
```
% TEST[catch:unavailable]

1. PIT ID for the search.
2. Sorts hits for the search with an explicit tiebreak on `_shard_doc` descending.


```console-result
{
  "pit_id" : "46ToAwMDaWR5BXV1aWQyKwZub2RlXzMAAAAAAAAAACoBYwADaWR4BXV1aWQxAgZub2RlXzEAAAAAAAAAAAEBYQADaWR5BXV1aWQyKgZub2RlXzIAAAAAAAAAAAwBYgACBXV1aWQyAAAFdXVpZDEAAQltYXRjaF9hbGw_gAAAAA==", <1>
  "took" : 17,
  "timed_out" : false,
  "_shards" : ...,
  "hits" : {
    "total" : ...,
    "max_score" : null,
    "hits" : [
      ...
      {
        "_index" : "my-index-000001",
        "_id" : "FaslK3QBySSL_rrj9zM5",
        "_score" : null,
        "_source" : ...,
        "sort" : [                                <2>
          "2021-05-20T05:30:04.832Z",
          4294967298                              <3>
        ]
      }
    ]
  }
}
```
% TESTRESPONSE[skip: unable to access PIT ID]

1. Updated `id` for the point in time.
2. Sort values for the last returned hit.
3. The tiebreaker value, unique per document within the `pit_id`.


To get the next page of results, rerun the previous search using the last hit’s sort values (including the tiebreaker) as the `search_after` argument. If using a PIT, use the latest PIT ID in the `pit.id` parameter. The search’s `query` and `sort` arguments must remain unchanged. If provided, the `from` argument must be `0` (default) or `-1`.

```console
GET /_search
{
  "size": 10000,
  "query": {
    "match" : {
      "user.id" : "elkbee"
    }
  },
  "pit": {
    "id":  "46ToAwMDaWR5BXV1aWQyKwZub2RlXzMAAAAAAAAAACoBYwADaWR4BXV1aWQxAgZub2RlXzEAAAAAAAAAAAEBYQADaWR5BXV1aWQyKgZub2RlXzIAAAAAAAAAAAwBYgACBXV1aWQyAAAFdXVpZDEAAQltYXRjaF9hbGw_gAAAAA==", <1>
    "keep_alive": "1m"
  },
  "sort": [
    {"@timestamp": {"order": "asc", "format": "strict_date_optional_time_nanos"}}
  ],
  "search_after": [                                <2>
    "2021-05-20T05:30:04.832Z",
    4294967298
  ],
  "track_total_hits": false                        <3>
}
```
% TEST[catch:unavailable]

1. PIT ID returned by the previous search.
2. Sort values from the previous search’s last hit.
3. Disable the tracking of total hits to speed up pagination.


You can repeat this process to get additional pages of results. If using a PIT, you can extend the PIT’s retention period using the `keep_alive` parameter of each search request.

When you’re finished, you should delete your PIT.

```console
DELETE /_pit
{
    "id" : "46ToAwMDaWR5BXV1aWQyKwZub2RlXzMAAAAAAAAAACoBYwADaWR4BXV1aWQxAgZub2RlXzEAAAAAAAAAAAEBYQADaWR5BXV1aWQyKgZub2RlXzIAAAAAAAAAAAwBYgACBXV1aWQyAAAFdXVpZDEAAQltYXRjaF9hbGw_gAAAAA=="
}
```
% TEST[catch:missing]


## Scroll search results [scroll-search-results]

::::{important}
We no longer recommend using the scroll API for deep pagination. If you need to preserve the index state while paging through more than 10,000 hits, use the [`search_after`](#search-after) parameter with a point in time (PIT).
::::


While a `search` request returns a single page of results, the `scroll` API can be used to retrieve large numbers of results (or even all results) from a single search request, in much the same way as you would use a cursor on a traditional database.

Scrolling is not intended for real time user requests, but rather for processing large amounts of data, e.g. in order to reindex the contents of one data stream or index into a new data stream or index with a different configuration.

::::{admonition} Client support for scrolling and reindexing
Some of the officially supported clients provide helpers to assist with scrolled searches and reindexing:

Perl
:   See [Search::Elasticsearch::Client::5_0::Bulk](https://metacpan.org/pod/Search::Elasticsearch::Client::5_0::Bulk) and [Search::Elasticsearch::Client::5_0::Scroll](https://metacpan.org/pod/Search::Elasticsearch::Client::5_0::Scroll)

Python
:   See [elasticsearch.helpers.*](https://elasticsearch-py.readthedocs.io/en/stable/helpers.html)

JavaScript
:   See [client.helpers.*](elasticsearch-js://reference/client-helpers.md)

::::


::::{note}
The results that are returned from a scroll request reflect the state of the data stream or index at the time that the initial `search` request was made, like a snapshot in time. Subsequent changes to documents (index, update or delete) will only affect later search requests.
::::


In order to use scrolling, the initial search request should specify the `scroll` parameter in the query string, which tells Elasticsearch how long it should keep the search context alive (see [Keeping the search context alive](#scroll-search-context)), eg `?scroll=1m`.

```console
POST /my-index-000001/_search?scroll=1m
{
  "size": 100,
  "query": {
    "match": {
      "message": "foo"
    }
  }
}
```
% TEST[setup:my_index]

The result from the above request includes a `_scroll_id`, which should be passed to the `scroll` API in order to retrieve the next batch of results.

```console
POST /_search/scroll                                                               <1>
{
  "scroll" : "1m",                                                                 <2>
  "scroll_id" : "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ==" <3>
}
```
% TEST[continued s/DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ==/$body._scroll_id/]

1. `GET` or `POST` can be used and the URL should not include the `index` name — this is specified in the original `search` request instead.
2. The `scroll` parameter tells Elasticsearch to keep the search context open for another `1m`.
3. The `scroll_id` parameter


The `size` parameter allows you to configure the maximum number of hits to be returned with each batch of results. Each call to the `scroll` API returns the next batch of results until there are no more results left to return, ie the `hits` array is empty.

::::{important}
The initial search request and each subsequent scroll request each return a `_scroll_id`. While the `_scroll_id` may change between requests, it doesn’t always change — in any case, only the most recently received `_scroll_id` should be used.
::::


::::{note}
If the request specifies aggregations, only the initial search response will contain the aggregations results.
::::


::::{note}
Scroll requests have optimizations that make them faster when the sort order is `_doc`. If you want to iterate over all documents regardless of the order, this is the most efficient option:
::::


```console
GET /_search?scroll=1m
{
  "sort": [
    "_doc"
  ]
}
```
% TEST[setup:my_index]


### Keeping the search context alive [scroll-search-context]

A scroll returns all the documents which matched the search at the time of the initial search request. It ignores any subsequent changes to these documents. The `scroll_id` identifies a *search context* which keeps track of everything that {{es}} needs to return the correct documents. The search context is created by the initial request and kept alive by subsequent requests.

The `scroll` parameter (passed to the `search` request and to every `scroll` request) tells Elasticsearch how long it should keep the search context alive. Its value (e.g. `1m`, see [Time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) does not need to be long enough to process all data — it just needs to be long enough to process the previous batch of results. Each `scroll` request (with the `scroll` parameter) sets a new expiry time. If a `scroll` request doesn’t pass in the `scroll` parameter, then the search context will be freed as part of *that* `scroll` request.

Normally, the background merge process optimizes the index by merging together smaller segments to create new, bigger segments. Once the smaller segments are no longer needed they are deleted. This process continues during scrolling, but an open search context prevents the old segments from being deleted since they are still in use.

::::{tip}
Keeping older segments alive means that more disk space and file handles are needed. Ensure that you have configured your nodes to have ample free file handles. See [File Descriptors](docs-content://deploy-manage/deploy/self-managed/file-descriptors.md).
::::


Additionally, if a segment contains deleted or updated documents then the search context must keep track of whether each document in the segment was live at the time of the initial search request. Ensure that your nodes have sufficient heap space if you have many open scrolls on an index that is subject to ongoing deletes or updates.

::::{note}
To prevent against issues caused by having too many scrolls open, the user is not allowed to open scrolls past a certain limit. By default, the maximum number of open scrolls is 500. This limit can be updated with the `search.max_open_scroll_context` cluster setting.
::::


You can check how many search contexts are open with the [nodes stats API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-stats):

```console
GET /_nodes/stats/indices/search
```


### Clear scroll [clear-scroll]

Search context are automatically removed when the `scroll` timeout has been exceeded. However keeping scrolls open has a cost, as discussed in the [previous section](#scroll-search-context) so scrolls should be explicitly cleared as soon as the scroll is not being used anymore using the `clear-scroll` API:

```console
DELETE /_search/scroll
{
  "scroll_id" : "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ=="
}
```
% TEST[catch:missing]

Multiple scroll IDs can be passed as array:

```console
DELETE /_search/scroll
{
  "scroll_id" : [
    "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ==",
    "DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAAABFmtSWWRRWUJrU2o2ZExpSGJCVmQxYUEAAAAAAAAAAxZrUllkUVlCa1NqNmRMaUhiQlZkMWFBAAAAAAAAAAIWa1JZZFFZQmtTajZkTGlIYkJWZDFhQQAAAAAAAAAFFmtSWWRRWUJrU2o2ZExpSGJCVmQxYUEAAAAAAAAABBZrUllkUVlCa1NqNmRMaUhiQlZkMWFB"
  ]
}
```
% TEST[catch:missing]

All search contexts can be cleared with the `_all` parameter:

```console
DELETE /_search/scroll/_all
```

The `scroll_id` can also be passed as a query string parameter or in the request body. Multiple scroll IDs can be passed as comma separated values:

```console
DELETE /_search/scroll/DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ==,DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAAABFmtSWWRRWUJrU2o2ZExpSGJCVmQxYUEAAAAAAAAAAxZrUllkUVlCa1NqNmRMaUhiQlZkMWFBAAAAAAAAAAIWa1JZZFFZQmtTajZkTGlIYkJWZDFhQQAAAAAAAAAFFmtSWWRRWUJrU2o2ZExpSGJCVmQxYUEAAAAAAAAABBZrUllkUVlCa1NqNmRMaUhiQlZkMWFB
```
% TEST[catch:missing]


### Sliced scroll [slice-scroll]

When paging through a large number of documents, it can be helpful to split the search into multiple slices to consume them independently:

```console
GET /my-index-000001/_search?scroll=1m
{
  "slice": {
    "id": 0,                      <1>
    "max": 2                      <2>
  },
  "query": {
    "match": {
      "message": "foo"
    }
  }
}
GET /my-index-000001/_search?scroll=1m
{
  "slice": {
    "id": 1,
    "max": 2
  },
  "query": {
    "match": {
      "message": "foo"
    }
  }
}
```
% TEST[setup:my_index_big]

1. The id of the slice
2. The maximum number of slices


The result from the first request returned documents that belong to the first slice (id: 0) and the result from the second request returned documents that belong to the second slice. Since the maximum number of slices is set to 2 the union of the results of the two requests is equivalent to the results of a scroll query without slicing. By default the splitting is done first on the shards, then locally on each shard using the `_id` field. The local splitting follows the formula `slice(doc) = floorMod(hashCode(doc._id), max))`.

Each scroll is independent and can be processed in parallel like any scroll request.

::::{note}
If the number of slices is bigger than the number of shards the slice filter is very slow on the first calls, it has a complexity of O(N) and a memory cost equals to N bits per slice where N is the total number of documents in the shard. After few calls the filter should be cached and subsequent calls should be faster but you should limit the number of sliced query you perform in parallel to avoid the memory explosion.
::::


The [point-in-time](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-open-point-in-time) API supports a more efficient partitioning strategy and does not suffer from this problem. When possible, it’s recommended to use a point-in-time search with slicing instead of a scroll.

Another way to avoid this high cost is to use the `doc_values` of another field to do the slicing. The field must have the following properties:

* The field is numeric.
* `doc_values` are enabled on that field
* Every document should contain a single value. If a document has multiple values for the specified field, the first value is used.
* The value for each document should be set once when the document is created and never updated. This ensures that each slice gets deterministic results.
* The cardinality of the field should be high. This ensures that each slice gets approximately the same amount of documents.

```console
GET /my-index-000001/_search?scroll=1m
{
  "slice": {
    "field": "@timestamp",
    "id": 0,
    "max": 10
  },
  "query": {
    "match": {
      "message": "foo"
    }
  }
}
```
% TEST[setup:my_index_big]

For append only time-based indices, the `timestamp` field can be used safely.

