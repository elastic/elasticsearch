---
applies_to:
 stack: all
 serverless: ga
---

# RRF retriever [rrf-retriever]

An [RRF](/reference/elasticsearch/rest-apis/reciprocal-rank-fusion.md) retriever returns top documents based on the RRF formula, combining two or more child retrievers.
Reciprocal rank fusion (RRF) is a method for combining multiple result sets with different relevance indicators into a single result set.
Each child retriever can optionally be assigned a weight to adjust its influence on the final ranking.


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
    Two or more child retrievers are required.
    
    Each retriever can be specified in two formats:
    
    - **Direct format**: `{ "standard": { "query": {...} } }` - Uses default weight of `1.0`
    - **Weighted format**: `{ "retriever": { "standard": { "query": {...} } }, "weight": 2.0 }` - Specifies a custom weight

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

Each entry in the `retrievers` array can specify the following parameters:

`retriever`
:   (Optional, a retriever object)

    Specifies a child retriever. Can be omitted when using the direct format (e.g., `{ "standard": {...} }`).

`weight`
:   (Optional, float)

    The weight that each score of this retriever's top docs will be multiplied with in the RRF formula. Higher values increase this retriever's influence on the final ranking. Must be non-negative. Defaults to `1.0`.

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

This example demonstrates how to use weights to adjust the influence of different retrievers in the RRF ranking.
In this case, we're giving more importance to lexical matches (weight 2.0) compared to vector similarity (weight 1.0):

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
2. This retriever has a weight of 2.0, giving lexical matches twice the influence.
3. The second retriever in weighted format.
4. This retriever has a weight of 1.0 (default weight).

::::{note}
You can also mix weighted and non-weighted formats in the same query.
Retrievers without an explicit weight default to 1.0:

```json
{
  "rrf": {
    "retrievers": [
      { "standard": { "query": {...} } },  // weight defaults to 1.0
      { "retriever": { "knn": {...} }, "weight": 2.0 }
    ]
  }
}
```
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
