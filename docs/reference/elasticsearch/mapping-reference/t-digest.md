---
applies_to:
  stack: preview 9.3
  serverless: preview
navigation_title: "T-digest"
---

# T-digest field type [tdigest]

A field to store pre-aggregated numerical data constructed using the [T-Digest](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md) algorithm.

## Structure of a `tdigest` field

A `tdigest` field requires two arrays:

* A `centroids` array of
  [`double`](/reference/elasticsearch/mapping-reference/number.md), containing
  the computed centroids.  These must be provided in ascending order.
* A `counts` array of
  [`long`](/reference/elasticsearch/mapping-reference/number.md), containing the
  computed counts for each of the centroids.  This must be the same length as
  the `centroids` array

The field also accepts three optional summary fields:

* `sum`, a [`double`](/reference/elasticsearch/mapping-reference/number.md),
  representing the sum of the values being summarized by the t-digest
* `min`, a [`double`](/reference/elasticsearch/mapping-reference/number.md),
  representing the minimum of the values being summarized by the t-digest
* `max`, a [`double`](/reference/elasticsearch/mapping-reference/number.md),
  representing the maximum of the values being summarized by the t-digest

Specifying the summary values enables them to be calculated with
higher accuracy from the raw data. If not specified, Elasticsearch
computes them based on the given `centroids` and `counts`, with some loss of
accuracy.

## Limitations

* A `tdigest` field can only store a single sketch per document. Multi-values or nested arrays are not supported.
* `tdigest` fields do not support sorting and are not searchable.


## Configuring T-Digest Fields

T-Digest fields accept two field-specific configuration parameters:

* `compression`, a
  [`double`](/reference/elasticsearch/mapping-reference/number.md) between `0` and
  `10000` (excluding `0`), which corresponds to the parameter of the same name in
  the [T-Digest](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md) algorithm.
  In general, the higher this number, the more space on disk the field will use
  but the more accurate the sketch approximations will be.  Default is `100`
* `digest_type`, which selects the merge strategy to use with the sketch.  Valid
  values are `default` and `high_accuracy`.  The default is `default`.  The
  `default` is optimized for storage and performance, while still producing a
  good approximation.  The `high_accuracy` variant uses more memory, disk, and
  CPU for a better approximation.

## Use cases [tdigest-use-cases]

`tdigest` fields are primarily intended for use with aggregations. To make them
efficient for aggregations, the data are stored as compact [doc
values](/reference/elasticsearch/mapping-reference/doc-values.md) and not
indexed.

`tdigest` fields are supported in the following [ES|QL](/reference/query-languages/esql.md) aggregation functions:

* [Avg](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-avg)
* [Max](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-max)
  and
  [Min](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-min)
* [Percentile](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-percentile)
* [Present](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-present) and
  [Absent](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-absent)


## Synthetic `_source` [tdigest-synthetic-source]

`tdigest` fields support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) in their default configuration.

::::{note}
To save space, zero-count buckets are not stored in `tdigest` doc values. If you index a `tdigest` field with zero-count buckets and synthetic `_source` is enabled, those buckets won't appear when you retrieve the field.
::::

## Examples

### Create an index with a `tdigest` field

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "latency": {
        "type": "tdigest"
      }
    }
  }
}
```

### Index a simple document

```console
PUT my-index-000001/_doc/1
{
  "latency": {
    "centroids": [0.1, 0.2, 0.3, 0.4, 0.5],
    "counts": [3, 7, 23, 12, 6]
  }
}
```

### Query via ES|QL

```console
POST /_query?format=txt
{
	"query": "FROM my-index-000001 | STATS Percentile(latency, 99)"
}


