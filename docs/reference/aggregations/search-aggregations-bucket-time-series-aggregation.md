---
navigation_title: "Time series"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-time-series-aggregation.html
applies_to:
  stack: deprecated 9.7+
---

# Time series aggregation [search-aggregations-bucket-time-series-aggregation]


::::{warning}
The `time_series` aggregation is deprecated and will be removed in a future version. It was in technical preview before, and the `rate` aggregation on counter fields and the `geo_line` aggregation inside it are deprecated with it. Use the [{{esql}} `TS` command](/reference/query-languages/esql/commands/ts.md) instead.
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

The `time_series` aggregation has many limitations, and we recommend that you do not use it. It turns off many aggregation performance optimizations, and it fails or behaves differently when you combine it with other aggregations, for example `composite`, `global`, `top_hits`, `rare_terms`, `auto_date_histogram` and the sampler aggregations. Use the [{{esql}} `TS` command](/reference/query-languages/esql/commands/ts.md) instead.
