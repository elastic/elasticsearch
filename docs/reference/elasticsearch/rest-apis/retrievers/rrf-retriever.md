---
applies_to:
 stack: all
 serverless: ga
---

# RRF retriever [rrf-retriever]

An [RRF](/reference/elasticsearch/rest-apis/reciprocal-rank-fusion.md) retriever returns top documents based on the RRF formula, combining two or more child retrievers.
Reciprocal rank fusion (RRF) is a method for combining multiple result sets with different relevance indicators into a single result set.


## Parameters [rrf-retriever-parameters]

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
    If not specified, uses the index's default fields from the `index.query.default_field` index setting, which is `*` by default.

`retrievers`
:   (Optional, array of retriever objects)

    A list of child retrievers to specify which sets of returned top documents will have the RRF formula applied to them.
    Each retriever can optionally include a weight to adjust its influence on the final ranking. {applies_to}`stack: ga 9.2`
    
    When weights are specified, the final RRF score is calculated as:
    ```
    rrf_score = weight_1 × rrf_score_1 + weight_2 × rrf_score_2 + ... + weight_n × rrf_score_n
    ```
    where `rrf_score_i` is the RRF score for document from retriever `i`, and `weight_i` is the weight for that retriever.

`rank_constant`
:   (Optional, integer)

    This value determines how much influence documents in individual result sets per query have over the final ranked result set. A higher value indicates that lower ranked documents have more influence. This value must be greater than or equal to `1`. Defaults to `60`.

`rank_window_size`
:   (Optional, integer)

    This value determines the size of the individual result sets per query.
    A higher value will improve result relevance at the cost of performance.
    The final ranked result set is pruned down to the search request’s [size](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-size-param).
    `rank_window_size` must be greater than or equal to `size` and greater than or equal to `1`.
    Defaults to 10.

`filter`
:   (Optional, [query object or list of query objects](/reference/query-languages/querydsl.md))

    Applies the specified [boolean query filter](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to all of the specified sub-retrievers, according to each retriever’s specifications.

Each entry in the `retrievers` array can be specified using the direct format or the wrapped format. {applies_to}`stack: ga 9.2`

**Direct format** (default weight of `1.0`):
```json
{
  "rrf": {
    "retrievers": [
      {
        "standard": {
          "query": {
            "multi_match": {
              "query": "search text",
              "fields": ["field1", "field2"]
            }
          }
        }
      },
      {
        "knn": {
          "field": "vector",
          "query_vector": [1, 2, 3],
          "k": 10,
          "num_candidates": 50
        }
      }
    ]
  }
}
```

**Wrapped format with custom weights** {applies_to}`stack: ga 9.2`:
```json
{
  "rrf": {
    "retrievers": [
      {
        "retriever": {
          "standard": {
            "query": {
              "multi_match": {
                "query": "search text",
                "fields": ["field1", "field2"]
              }
            }
          }
        },
        "weight": 2.0
      },
      {
        "retriever": {
          "knn": {
            "field": "vector",
            "query_vector": [1, 2, 3],
            "k": 10,
            "num_candidates": 50
          }
        },
        "weight": 1.0
      }
    ]
  }
}
```

In the wrapped format:

`retriever`
:   (Required, a retriever object)

    Specifies a child retriever. Any valid retriever type can be used (e.g., `standard`, `knn`, `text_similarity_reranker`, etc.).

`weight` {applies_to}`stack: ga 9.2`
:   (Optional, float)

    The weight that each score of this retriever's top docs will be multiplied in the RRF formula. Higher values increase this retriever's influence on the final ranking. Must be non-negative. Defaults to `1.0`.

## Example: Hybrid search [rrf-retriever-example-hybrid]

A simple hybrid search example (lexical search + dense vector search) combining a `standard` retriever with a `knn` retriever using RRF:

<!--
```console
PUT /restaurants
{
  "mappings": {
    "properties": {
      "region": { "type": "keyword" },
      "year": { "type": "keyword" },
      "vector": {
        "type": "dense_vector",
        "dims": 3
      }
    }
  }
}

POST /restaurants/_bulk?refresh
{"index":{}}
{"region": "Austria", "year": "2019", "vector": [10, 22, 77]}
{"index":{}}
{"region": "France", "year": "2019", "vector": [10, 22, 78]}
{"index":{}}
{"region": "Austria", "year": "2020", "vector": [10, 22, 79]}
{"index":{}}
{"region": "France", "year": "2020", "vector": [10, 22, 80]}

PUT /movies

PUT /books
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "copy_to": "title_semantic"
      },
      "description": {
        "type": "text",
        "copy_to": "description_semantic"
      },
      "title_semantic": {
        "type": "semantic_text"
      },
      "description_semantic": {
        "type": "semantic_text"
      }
    }
  }
}

PUT _query_rules/my-ruleset
{
    "rules": [
        {
            "rule_id": "my-rule1",
            "type": "pinned",
            "criteria": [
                {
                    "type": "exact",
                    "metadata": "query_string",
                    "values": [ "pugs" ]
                }
            ],
            "actions": {
                "ids": [
                    "id1"
                ]
            }
        }
    ]
}
```
% TESTSETUP

```console
DELETE /restaurants
DELETE /movies
DELETE /books
```
% TEARDOWN
-->

```console
GET /restaurants/_search
{
  "retriever": {
    "rrf": { <1>
      "retrievers": [ <2>
        {
          "standard": { <3>
            "query": {
              "multi_match": {
                "query": "Austria",
                "fields": [
                  "city",
                  "region"
                ]
              }
            }
          }
        },
        {
          "knn": { <4>
            "field": "vector",
            "query_vector": [10, 22, 77],
            "k": 10,
            "num_candidates": 10
          }
        }
      ],
      "rank_constant": 1, <5>
      "rank_window_size": 50  <6>
    }
  }
}
```
% TEST[continued]

1. Defines a retriever tree with an RRF retriever.
2. The sub-retriever array.
3. The first sub-retriever is a `standard` retriever.
4. The second sub-retriever is a `knn` retriever.
5. The rank constant for the RRF retriever.
6. The rank window size for the RRF retriever.

## Example: Weighted hybrid search [rrf-retriever-example-weighted]

{applies_to}`stack: ga 9.2`

This example demonstrates how to use weights to adjust the influence of different retrievers in the RRF ranking.
In this case, we're giving the `standard` retriever more importance (weight 2.0) compared to the `knn` retriever (weight 1.0):

```console
GET /restaurants/_search
{
  "retriever": {
    "rrf": {
      "retrievers": [
        {
          "retriever": { <1>
            "standard": {
              "query": {
                "multi_match": {
                  "query": "Austria",
                  "fields": ["city", "region"]
                }
              }
            }
          },
          "weight": 2.0 <2>
        },
        {
          "retriever": { <3>
            "knn": {
              "field": "vector",
              "query_vector": [10, 22, 77],
              "k": 10,
              "num_candidates": 10
            }
          },
          "weight": 1.0 <4>
        }
      ],
      "rank_constant": 60,
      "rank_window_size": 50
    }
  }
}
```
% TEST[continued]

1. The first retriever in weighted format.
2. This retriever has a weight of 2.0, giving it twice the influence of the kNN retriever.
3. The second retriever in weighted format.
4. This retriever has a weight of 1.0 (default weight).

::::{note}
You can mix weighted and non-weighted formats in the same query.
The direct format (without explicit `retriever` wrapper) uses the default weight of `1.0`:

```json
{
  "rrf": {
    "retrievers": [
      { "standard": { "query": {...} } },
      { "retriever": { "knn": {...} }, "weight": 2.0 }
    ]
  }
}
```

In this example, the `standard` retriever uses weight `1.0` (default), while the `knn` retriever uses weight `2.0`.
::::

## Example: Hybrid search with sparse vectors [rrf-retriever-example-hybrid-sparse]

A more complex hybrid search example (lexical search + ELSER sparse vector search + dense vector search) using RRF:

```console
GET movies/_search
{
  "retriever": {
    "rrf": {
      "retrievers": [
        {
          "standard": {
            "query": {
              "sparse_vector": {
                "field": "plot_embedding",
                "inference_id": "my-elser-model",
                "query": "films that explore psychological depths"
              }
            }
          }
        },
        {
          "standard": {
            "query": {
              "multi_match": {
                "query": "crime",
                "fields": [
                  "plot",
                  "title"
                ]
              }
            }
          }
        },
        {
          "knn": {
            "field": "vector",
            "query_vector": [10, 22, 77],
            "k": 10,
            "num_candidates": 10
          }
        }
      ]
    }
  }
}
```
% TEST[skip:uses ELSER]
