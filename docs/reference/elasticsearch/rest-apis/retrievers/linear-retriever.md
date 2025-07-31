---
applies_to:
 stack: all
 serverless: ga
---

# Linear retriever [linear-retriever]

A retriever that normalizes and linearly combines the scores of other retrievers.


## Parameters [linear-retriever-parameters]

::::{note}
Either `query` or `retrievers` must be specified.
Combining `query` and `retrievers` is not supported.
::::

`query` {applies_to}`stack: ga 9.1`
:   (Optional, String)

    The query to use when using the [multi-field query format](../retrievers.md#multi-field-query-format).

`fields` {applies_to}`stack: ga 9.1`
:   (Optional, array of strings)

    The fields to query when using the [multi-field query format](../retrievers.md#multi-field-query-format).
    Fields can include boost values using the `^` notation (e.g., `"field^2"`).
    If not specified, uses the index's default fields from the `index.query.default_field` index setting, which is `*` by default.

`normalizer` {applies_to}`stack: ga 9.1`
:   (Optional, String)

    The normalizer to use for score normalization. This serves as the default normalizer for all sub-retrievers.
    See [normalizers](#linear-retriever-normalizers) for supported values.
    
    When using the [multi-field query format](../retrievers.md#multi-field-query-format), this field is required when `query` is specified.
    
    When using the `retrievers` array format, this field serves as the default normalizer for all sub-retrievers. Individual sub-retrievers can override this default by specifying their own `normalizer` field.

    ::::{warning}
    Avoid using `none` as that will disable normalization and may bias the result set towards lexical matches.
    See [field grouping](../retrievers.md#multi-field-field-grouping) for more information.
    ::::

`retrievers`
:   (Optional, array of objects)

    A list of the sub-retrievers' configuration, that we will take into account and whose result sets we will merge through a weighted sum.
    Each configuration can have a different weight and normalization depending on the specified retriever.

`rank_window_size`
:   (Optional, integer)

    This value determines the size of the individual result sets per query. A higher value will improve result relevance at the cost of performance.
    The final ranked result set is pruned down to the search request’s [size](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-size-param).
    `rank_window_size` must be greater than or equal to `size` and greater than or equal to `1`.
    Defaults to 10.

`filter`
:   (Optional, [query object or list of query objects](/reference/query-languages/querydsl.md))

    Applies the specified [boolean query filter](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to all of the specified sub-retrievers, according to each retriever’s specifications.

Each entry in the `retrievers` array specifies the following parameters:

`retriever`
:   (Required, a `retriever` object)

    Specifies the retriever for which we will compute the top documents for. The retriever will produce `rank_window_size` results, which will later be merged based on the specified `weight` and `normalizer`.

`weight`
:   (Optional, float)

    The weight that each score of this retriever’s top docs will be multiplied with. Must be greater or equal to 0. Defaults to 1.0.

`normalizer`
:   (Optional, String)

    Specifies how the retriever’s score will be normalized before applying the specified `weight`.
    See [normalizers](#linear-retriever-normalizers) for supported values.
    Defaults to `none`.

See also [this hybrid search example](retrievers-examples.md#retrievers-examples-linear-retriever) using a linear retriever on how to independently configure and apply normalizers to retrievers.

## Normalizers [linear-retriever-normalizers]

The `linear` retriever supports the following normalizers:

* `none`: No normalization
* `minmax`: Normalizes scores based on the following formula:

    ```
    score = (score - min) / (max - min)
    ```
* `l2_norm`: Normalizes scores using the L2 norm of the score values {applies_to}`stack: ga 9.1`

## Examples [linear-retriever-examples]

### Top-level normalizer example

This example shows how to use a top-level normalizer that applies to all sub-retrievers:

```console
GET my_index/_search
{
  "retriever": {
    "linear": {
      "retrievers": [
        {
          "retriever": {
            "standard": {
              "query": {
                "match": {
                  "title": "elasticsearch"
                }
              }
            }
          },
          "weight": 1.0
        },
        {
          "retriever": {
            "knn": {
              "field": "title_vector",
              "query_vector": [0.1, 0.2, 0.3],
              "k": 10,
              "num_candidates": 100
            }
          },
          "weight": 2.0
        }
      ],
      "normalizer": "minmax"
    }
  }
}
```

In this example, the `minmax` normalizer is applied to both the standard retriever and the kNN retriever. The top-level normalizer serves as a default that can be overridden by individual sub-retrievers. When using the multi-field query format, the top-level normalizer is applied to all generated inner retrievers.
