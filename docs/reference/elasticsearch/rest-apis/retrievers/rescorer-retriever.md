---
applies_to:
 stack: all
 serverless: ga
---

# Rescorer retriever [rescorer-retriever]

The `rescorer` retriever re-scores only the results produced by its child retriever. For the `standard` and `knn` retrievers, the `window_size` parameter specifies the number of documents examined per shard.

For compound retrievers like `rrf`, the `window_size` parameter defines the total number of documents examined globally.

When using the `rescorer`, an error is returned if the following conditions are not met:

* The minimum configured rescore’s `window_size` is:

    * Greater than or equal to the `size` of the parent retriever for nested `rescorer` setups.
    * Greater than or equal to the `size` of the search request when used as the primary retriever in the tree.

* And the maximum rescore’s `window_size` is:

    * Smaller than or equal to the `size` or `rank_window_size` of the child retriever.

## Parameters [rescorer-retriever-parameters]

`rescore`
:   (Required. [A rescorer definition or an array of rescorer definitions](/reference/elasticsearch/rest-apis/rescore-search-results.md#rescore))

    Defines the [rescorers](/reference/elasticsearch/rest-apis/rescore-search-results.md#rescore) applied sequentially to the top documents returned by the child retriever.


`retriever`
:   (Required. `retriever`)

    Specifies the child retriever responsible for generating the initial set of top documents to be re-ranked.


`filter`
:   (Optional. [query object or list of query objects](/reference/query-languages/querydsl.md))

    Applies a [boolean query filter](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to the retriever, ensuring that all documents match the filter criteria without affecting their scores.



## Example [rescorer-retriever-example]

The `rescorer` retriever can be placed at any level within the retriever tree. The following example demonstrates a `rescorer` applied to the results produced by an `rrf` retriever:

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
GET movies/_search
{
  "size": 10, <1>
  "retriever": {
    "rescorer": { <2>
      "rescore": {
        "window_size": 50, <3>
        "query": { <4>
          "rescore_query": {
            "script_score": {
              "query": {
                "match_all": {}
              },
              "script": {
                "source": "cosineSimilarity(params.queryVector, 'product-vector_final_stage') + 1.0",
                "params": {
                  "queryVector": [-0.5, 90.0, -10, 14.8, -156.0]
                }
              }
            }
          }
        }
      },
      "retriever": { <5>
        "rrf": {
          "rank_window_size": 100, <6>
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
  }
}
```
% TEST[skip:uses ELSER]

1. Specifies the number of top documents to return in the final response.
2. A `rescorer` retriever applied as the final step.
3. Defines the number of documents to rescore from the child retriever.
4. The definition of the `query` rescorer.
5. Specifies the child retriever definition.
6. Defines the number of documents returned by the `rrf` retriever, which limits the available documents to

