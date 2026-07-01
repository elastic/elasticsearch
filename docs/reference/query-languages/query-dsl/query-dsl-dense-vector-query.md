---
navigation_title: "Dense vector"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-dense-vector-query.html
applies_to:
  stack: ga 9.5
  serverless: all
---

# Dense vector query [query-dsl-dense-vector-query]

Performs exact, brute-force scoring of a query vector against every document that has a value for a
[`dense_vector`](/reference/elasticsearch/mapping-reference/dense-vector.md) field. Unlike the approximate
[`knn` query](/reference/query-languages/query-dsl/query-dsl-knn-query.md), it does not use an index
structure or a `k`/`num_candidates` cutoff: every matching document is scored. This is useful when you need
exact scores, want to combine exact vector scoring with other queries, or want to score a `dense_vector`
field that is not indexed for approximate search (`index: false`).

By default, scoring uses the original, full-precision vectors, so a [quantized](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization)
index still produces full-precision scores. See [`quantized`](#dense-vector-query-params) to score against
the quantized representation instead.

## Example request [dense-vector-query-ex-request]

```console
PUT my-image-index
{
  "mappings": {
    "properties": {
      "image-vector": {
        "type": "dense_vector",
        "dims": 3,
        "index": true,
        "similarity": "l2_norm"
      }
    }
  }
}
```

1. Index your data.

    ```console
    POST my-image-index/_bulk?refresh=true
    { "index": { "_id": "1" } }
    { "image-vector": [1, 5, -20] }
    { "index": { "_id": "2" } }
    { "image-vector": [42, 8, -15] }
    { "index": { "_id": "3" } }
    { "image-vector": [15, 11, 23] }
    ```
    % TEST[continued]

2. Run the `dense_vector` query. Every document with the field is scored.

    ```console
    POST my-image-index/_search
    {
      "query": {
        "dense_vector": {
          "field": "image-vector",
          "query_vector": [-5, 9, -12]
        }
      }
    }
    ```
    % TEST[continued]

## Top-level parameters for `dense_vector` [dense-vector-query-params]

`field`
:   (Required, string) The name of the `dense_vector` field to search against.

`query_vector`
:   (Optional, array of floats or string) The query vector. Must have the same number of dimensions as the
target field. You must provide either `query_vector` or `query_vector_builder`, but not both. Accepts a
float array, or a hex-encoded or base64-encoded string.

`query_vector_builder`
:   (Optional, object) A configuration object used to convert a query into a `query_vector`, for example by
running a text embedding model. You must provide either `query_vector` or `query_vector_builder`, but not
both. See [`query_vector_builder`](/reference/query-languages/query-dsl/query-dsl-knn-query.md) for details.

`similarity_function`
:   (Optional, string) The similarity metric used for scoring, overriding the field's mapped similarity for
this query. One of `l2_norm`, `dot_product`, `cosine`, or `max_inner_product`. Defaults to the field's
configured similarity, or to `cosine` for a non-indexed (`index: false`) field, which has no configured
similarity. For `bit` fields, only `l2_norm` is supported. Cannot be combined with `quantized: true`.

`quantized` [dense-vector-query-quantized]
:   (Optional, Boolean) Defaults to `false`.

    When `false` (the default), scoring iterates the preserved full-precision vectors, producing raw scores
    regardless of the field's `index_options`.

    When `true`, scoring uses the codec's scorer, which on a [quantized](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization)
    index scores against the lossy quantized representation (faster, lower fidelity). It cannot be combined
    with `similarity_function`.

    ::::{note}
    `quantized: true` only makes sense for [quantized](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization)
    fields. For non-quantized `dense_vector` fields, the setting is ignored and the original vectors are used
    for scoring.
    ::::

`boost`
:   (Optional, float) Floating point number used to multiply the scores of the query. Defaults to `1.0`.

`_name`
:   (Optional, string) Name to identify the query for [named queries](/reference/query-languages/query-dsl/query-dsl-bool-query.md).

## Scoring non-indexed fields [dense-vector-query-non-indexed]

The `dense_vector` query can score a field mapped with `index: false`, whose vectors are stored as doc
values rather than in an approximate-search index. A non-indexed field has no configured `similarity`, so
scoring uses `cosine` unless you specify a different metric with
[`similarity_function`](#dense-vector-query-params):

```console
POST my-image-index/_search
{
  "query": {
    "dense_vector": {
      "field": "image-vector",
      "query_vector": [-5, 9, -12],
      "similarity_function": "cosine"
    }
  }
}
```
% TEST[continued]
