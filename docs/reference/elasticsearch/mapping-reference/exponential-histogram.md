---
applies_to:
  stack: preview 9.3
  serverless: preview
navigation_title: "Exponential histogram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/exponential-histogram.html
---

# Exponential histogram field type [exponential-histogram]

A field to store pre-aggregated numerical data using an exponential histogram, compatible with the OpenTelemetry data model. This field captures a distribution of values with fixed, exponentially spaced bucket boundaries controlled by a `scale` parameter, and a special zero bucket for values close to zero.

An exponential histogram field has the following structure:

```text
{
  "scale": <int>,                                  // required, in range [-11, 38]
  "sum": <double>,                                 // optional; estimated if omitted; must be 0.0 or omitted for empty histograms
  "min": <double>,                                 // optional; estimated if omitted; must be null or omitted for empty histograms
  "max": <double>,                                 // optional; estimated if omitted; must be null or omitted for empty histograms
  "zero": {                                        // optional; can be omitted if threshold and count are the default values
    "threshold": <double, non-negative>,           // optional; default 0.0
    "count": <long, non-negative>                  // optional; default 0
  },
  "positive": {                                    // optional
    "indices": [<long>...],                        // unique, sorted; each in range [-((2^62) - 1), (2^62) - 1]
    "counts":  [<long>...]                         // same length as indices; each > 0
  },
  "negative": {                                    // optional
    "indices": [<long>...],                        // same constraints as positive.indices
    "counts":  [<long>...]                         // same constraints as positive.counts
  }
}
```

The `scale` controls  the bucket density and precision. Larger scales produce finer buckets.
Exponential histograms can represent both positive and negative values, which are split into separate bucket ranges.
Each bucket range is an object with two parallel arrays:
- `indices`: array of the bucket indices defining the bucket boundaries
- `counts`: array of counts for the corresponding buckets

See the ["Bucket boundaries and scale"](#exponential-histogram-buckets) section below for how bucket indices map to value ranges.
The indices should be provided in sorted order. Unsorted indices are supported, but will incur a performance penalty during indexing.

In order to represent zero values or values close to zero, there is a special `zero` bucket, which consists of:
  - `threshold`: that defines the upper bound considered "zero".
  - `count`: number of values in the zero bucket.

Optionally you can include precomputed summary statistics:

- `sum` (double): The sum of all values in the histogram
- `min` (double): The minimum value in the histogram
- `max` (double): The maximum value in the histogram

When `sum`, `min`, or `max` are omitted, Elasticsearch will estimate these values during indexing.

::::{important}
- An `exponential_histogram` field is single-valued: one histogram per field per document. Nested arrays are not supported.
- `exponential_histogram` fields are not searchable and do not support sorting.
::::

## Uses [exponential-histogram-uses]

`exponential_histogram` fields are primarily intended for use with aggregations. To make them efficient for aggregations, the histogram is stored as compact [doc values](/reference/elasticsearch/mapping-reference/doc-values.md) and not indexed.

Exponential histograms are supported in ES|QL; see the [ES|QL reference](/reference/query-languages/esql.md) for details.

In Query DSL, because the data is not indexed, you can use `exponential_histogram` fields with the following aggregations:

- [sum](/reference/aggregations/search-aggregations-metrics-sum-aggregation.md) aggregation
- [avg](/reference/aggregations/search-aggregations-metrics-avg-aggregation.md) aggregation
- [value_count](/reference/aggregations/search-aggregations-metrics-valuecount-aggregation.md) aggregation
- [histogram](/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md) aggregation

## Synthetic `_source` [exponential-histogram-synthetic-source]

`exponential_histogram` fields support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) in their default configuration.

When `_source` is reconstructed, empty positive/negative bucket ranges and a zero bucket with `count: 0` and `threshold: 0` may be omitted in the serialized form.

## Examples [exponential-histogram-ex]

Create an index with an `exponential_histogram` field and a `keyword` field:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_histo": {
        "type": "exponential_histogram"
      },
      "title": { "type": "keyword" }
    }
  }
}
```

Index a document with a full exponential histogram payload:

```console
PUT my-index-000001/_doc/1
{
  "title": "histo_1",
  "my_histo": {
    "scale": 12,
    "sum": 1234.0,
    "min": -123.456,
    "max": 456.456,
    "zero": {
      "threshold": 0.001,
      "count": 42
    },
    "positive": {
      "indices": [-10, 25, 26],
      "counts":  [  2,  3,  4]
    },
    "negative": {
      "indices": [-5, 0],
      "counts":  [10, 7]
    }
  }
}
```

Indexing an empty histogram is allowed. The `sum` must be `0.0` and `min` and `max` must be omitted:

```console
PUT my-index-000001/_doc/2
{
  "title": "empty",
  "my_histo": {
    "scale": 10,
    "zero": { "threshold": 0.42 }
  }
}
```

### Coercion from T-Digest [exponential-histogram-coercion]

In order to facilitate a transition from the existing `histogram` field type, `exponential_histogram` fields support coercion from the `histogram` field type's `values`/`counts` format.
When `coerce` is enabled (default), Elasticsearch will interpret data provided as `values` and `counts` arrays as T-Digest and convert it to an exponential histogram during indexing.

```console
PUT my-index-000002
{
  "mappings": {
    "properties": {
      "my_histo": {
        "type": "exponential_histogram"
      }
    }
  }
}

PUT my-index-000002/_doc/1
{
  "my_histo": {
    "values": [0.1, 0.2, 0.3, 0.4, 0.5],
    "counts": [3, 7, 23, 12, 6]
  }
}
```

To reject legacy input, disable coercion on the field mapping or at the index level. See [coerce](/reference/elasticsearch/mapping-reference/coerce.md).

## Bucket boundaries and scale [exponential-histogram-buckets]

Exponential histograms use exponentially growing bucket widths. All bucket boundaries are derived from a base and an integer bucket index:

- Base: base = 2^(2^-scale). This is a base-2 exponential; the scale controls the density of buckets.
- Positive bucket i covers the half-open interval (base^i, base^(i+1)].
- Negative bucket i covers the interval (-base^(i+1), -base^i].
- Values with absolute value ≤ `zero.threshold` belong to the special zero bucket.

Changing the scale adjusts bucket widths:

- Increasing the scale by 1 splits each bucket into two adjacent buckets.
- Decreasing the scale by 1 merges each pair of adjacent buckets into a single bucket without introducing additional error due to e.g. rounding or interpolation.
