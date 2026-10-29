---
navigation_title: "Time series"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-time-series-aggregation.html
applies_to:
  stack: deprecated 9.7+
---

# Time series aggregation [search-aggregations-bucket-time-series-aggregation]


::::{warning}
The `time_series` aggregation is deprecated and will be removed in a future version. Use the [{{esql}} `TS` command](/reference/query-languages/esql/commands/ts.md) to query time series data instead.

Each search request that uses the `time_series` aggregation returns a deprecation warning. The aggregations that rely on the `time_series` aggregation are deprecated with it. These are the [`rate`](/reference/aggregations/search-aggregations-metrics-rate-aggregation.md) aggregation on counter fields and the [`geo_line`](/reference/aggregations/search-aggregations-metrics-geo-line.md) aggregation inside a `time_series` aggregation.

Before it was deprecated, this functionality was in technical preview. Features in technical preview are not subject to the support SLA of official GA features.
::::


The time series aggregation queries data created using a [Time series data stream (TSDS)](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md). This is typically data such as metrics or other data streams with a time component, and requires creating an index using the time series mode.

::::{note}
Refer to the [TSDS documentation](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#differences-from-regular-data-stream) to learn more about the key differences from regular data streams.

::::


Data can be added to the time series index like other indices:

```js
PUT /my-time-series-index-0/_bulk
{ "index": {} }
{ "key": "a", "val": 1, "@timestamp": "2022-01-01T00:00:10Z" }
{ "index": {}}
{ "key": "a", "val": 2, "@timestamp": "2022-01-02T00:00:00Z" }
{ "index": {} }
{ "key": "b", "val": 2, "@timestamp": "2022-01-01T00:00:10Z" }
{ "index": {}}
{ "key": "b", "val": 3, "@timestamp": "2022-01-02T00:00:00Z" }
```
% NOTCONSOLE

To perform a time series aggregation, specify "time_series" as the aggregation type. When the boolean "keyed" is true (the default), each bucket is given a unique key.

$$$time-series-aggregation-example$$$

```js
GET /_search
{
  "aggs": {
    "ts": {
      "time_series": { "keyed": false }
    }
  }
}
```
% NOTCONSOLE

This will return all results in the time series, however a more typical query will use sub aggregations to reduce the date returned to something more relevant.

## Size [search-aggregations-bucket-time-series-aggregation-size]

By default, `time series` aggregations return 10000 results. The "size" parameter can be used to limit the results further. Alternatively, using sub aggregations can limit the amount of values returned as a time series aggregation.


## Keyed [search-aggregations-bucket-time-series-aggregation-keyed]

The `keyed` parameter determines if buckets are returned as a map with unique keys per bucket. By default (`keyed` set to `true`), buckets are returned as a map. When `keyed` is `false`, buckets are returned as an array.


## Limitations [times-series-aggregations-limitations]

The `time_series` aggregation has many limitations. It collects documents in `_tsid` and `@timestamp` order, and this changes how the rest of the search request runs.

The following aggregations return an error when they are combined with the `time_series` aggregation:

* `composite`
* `global`
* `top_hits`
* `random_sampler`

The following aggregations defer the collection of their sub-aggregations. Deferred collection is not possible when documents are collected in `_tsid` order. These aggregations return an error when they have a sub-aggregation that they must defer:

* `auto_date_histogram`
* `variable_width_histogram`
* `rare_terms`
* `sampler` and `diversified_sampler`
* `categorize_text`

The following limitations apply to the other aggregations in the same search request:

* The `terms` and `multi_terms` aggregations always use the depth first collect mode.
* The filter by filter optimization is disabled, and so are many other aggregation performance optimizations.
* The search request is not run in parallel across segments, because documents must be collected in order.
* The `time_series` aggregation returns at most `size` buckets.

The following aggregations behave differently inside a `time_series` aggregation:

* The `rate` aggregation accepts counter fields and compensates for counter resets. It cannot be nested in an `auto_date_histogram` aggregation.
* The `geo_line` aggregation uses `@timestamp` as its `sort` field, and it returns an error for any other `sort` field. It simplifies the line to `size` points instead of it truncating the line.
