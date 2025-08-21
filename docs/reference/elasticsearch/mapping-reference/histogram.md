---
navigation_title: "Histogram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/histogram.html
---

# Histogram field type [histogram]


A field to store pre-aggregated numerical data representing a histogram. This data is defined using two paired arrays:

* A `values` array of [`double`](/reference/elasticsearch/mapping-reference/number.md) numbers, representing the buckets for the histogram. These values must be provided in ascending order.
* A corresponding `counts` array of [`long`](/reference/elasticsearch/mapping-reference/number.md) numbers, representing how many values fall into each bucket. These numbers must be positive or zero.

Because the elements in the `values` array correspond to the elements in the same position of the `count` array, these two arrays must have the same length.

::::{important}
* A `histogram` field can only store a single pair of `values` and `count` arrays per document. Nested arrays are not supported.
* `histogram` fields do not support sorting.

::::


## Uses [histogram-uses]

`histogram` fields are primarily intended for use with aggregations. To make it more readily accessible for aggregations, `histogram` field data is stored as a binary [doc values](/reference/elasticsearch/mapping-reference/doc-values.md) and not indexed. Its size in bytes is at most `13 * numValues`, where `numValues` is the length of the provided arrays.

Because the data is not indexed, you only can use `histogram` fields for the following aggregations and queries:

* [min](/reference/aggregations/search-aggregations-metrics-min-aggregation.md#search-aggregations-metrics-min-aggregation-histogram-fields) aggregation
* [max](/reference/aggregations/search-aggregations-metrics-max-aggregation.md#search-aggregations-metrics-max-aggregation-histogram-fields) aggregation
* [sum](/reference/aggregations/search-aggregations-metrics-sum-aggregation.md#search-aggregations-metrics-sum-aggregation-histogram-fields) aggregation
* [value_count](/reference/aggregations/search-aggregations-metrics-valuecount-aggregation.md#search-aggregations-metrics-valuecount-aggregation-histogram-fields) aggregation
* [avg](/reference/aggregations/search-aggregations-metrics-avg-aggregation.md#search-aggregations-metrics-avg-aggregation-histogram-fields) aggregation
* [percentiles](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md) aggregation
* [percentile ranks](/reference/aggregations/search-aggregations-metrics-percentile-rank-aggregation.md) aggregation
* [boxplot](/reference/aggregations/search-aggregations-metrics-boxplot-aggregation.md) aggregation
* [histogram](/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md#search-aggregations-bucket-histogram-aggregation-histogram-fields) aggregation
* [range](/reference/aggregations/search-aggregations-bucket-range-aggregation.md#search-aggregations-bucket-range-aggregation-histogram-fields) aggregation
* [exists](/reference/query-languages/query-dsl/query-dsl-exists-query.md) query


## Building a histogram [mapping-types-histogram-building-histogram]

When using a histogram as part of an aggregation, the accuracy of the results will depend on how the histogram was constructed. It is important to consider the percentiles aggregation mode that will be used to build it. Some possibilities include:

* For the [T-Digest](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md) mode, the `values` array represents the mean centroid positions and the `counts` array represents the number of values that are attributed to each centroid. If the algorithm has already started to approximate the percentiles, this inaccuracy is carried over in the histogram.
* For the [High Dynamic Range (HDR)](/reference/aggregations/search-aggregations-metrics-percentile-rank-aggregation.md#_hdr_histogram) histogram mode, the `values` array represents fixed upper limits of each bucket interval, and the `counts` array represents the number of values that are attributed to each interval. This implementation maintains a fixed worse-case percentage error (specified as a number of significant digits), therefore the value used when generating the histogram would be the maximum accuracy you can achieve at aggregation time.

The histogram field is "algorithm agnostic" and does not store data specific to either T-Digest or HDRHistogram. While this means the field can technically be aggregated with either algorithm, in practice the user should chose one algorithm and index data in that manner (e.g. centroids for T-Digest or intervals for HDRHistogram) to ensure best accuracy.


## Synthetic `_source` [histogram-synthetic-source]

`histogram` fields support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) in their default configuration.

::::{note}
To save space, zero-count buckets are not stored in the histogram doc values. As a result, when indexing a histogram field in an index with synthetic source enabled, indexing a histogram including zero-count buckets will result in missing buckets when fetching back the histogram.
::::



## Examples [histogram-ex]

The following [create index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) API request creates a new index with two field mappings:

* `my_histogram`, a `histogram` field used to store percentile data
* `my_text`, a `keyword` field used to store a title for the histogram

```console
PUT my-index-000001
{
  "mappings" : {
    "properties" : {
      "my_histogram" : {
        "type" : "histogram"
      },
      "my_text" : {
        "type" : "keyword"
      }
    }
  }
}
```

The following [index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) API requests store pre-aggregated for two histograms: `histogram_1` and `histogram_2`.

```console
PUT my-index-000001/_doc/1
{
  "my_text" : "histogram_1",
  "my_histogram" : {
      "values" : [0.1, 0.2, 0.3, 0.4, 0.5], <1>
      "counts" : [3, 7, 23, 12, 6] <2>
   }
}

PUT my-index-000001/_doc/2
{
  "my_text" : "histogram_2",
  "my_histogram" : {
      "values" : [0.1, 0.25, 0.35, 0.4, 0.45, 0.5], <1>
      "counts" : [8, 17, 8, 7, 6, 2] <2>
   }
}
```

1. Values for each bucket. Values in the array are treated as doubles and must be given in increasing order. For [T-Digest](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-approximation) histograms this value represents the mean value. In case of HDR histograms this represents the value iterated to.
2. Count for each bucket. Values in the arrays are treated as long integers and must be positive or zero. Negative values will be rejected. The relation between a bucket and a count is given by the position in the array.



