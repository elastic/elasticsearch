---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rescore-search-results.html
applies_to:
  stack: all
  serverless: all
---

# Rescore search results

Rescoring can help to improve precision by reordering just the top
(e.g. 100 - 500) documents returned by initial retrieval phase
(query, knn search) by using a secondary (usually more costly) algorithm,
instead of applying the costly algorithm to all documents in the index.

## How `rescore` works [rescore]

A `rescore` request is executed on each shard before it returns its results
to be sorted by the node handling the overall search request.

The rescore API has 3 options:

1. `query` rescorer that executes a provided `rescore_query` on the top documents
2. `script` rescorer that uses a script to modify the scores of the top documents
3. `learning_to_rank` rescorer that uses an LTR model to re-rank the top documents

All rescores have the `window_size` parameter that controls how many top
documents will be considered for rescoring. The default is 10.

::::{note}
When implementing pagination, keep the `window_size` consistent across pages.
Changing it while advancing through results (by using different `from` values)
can cause the top hits to shift, leading to a confusing user experience.
::::

## Query Rescorer [query-rescorer]

The query rescorer executes a second query only on the top documents returned
from the previous phase. The number of docs which is examined on each shard
can be controlled by the `window_size` parameter.

By default, the scores from the original query and the rescore query are combined
linearly to produce the final `_score` for each document.
The relative importance of the original query and of the rescore query can be
controlled with the `query_weight` and `rescore_query_weight` respectively.
Both default to `1`.

For example:

```console
POST /_search
{
   "query" : {
      "match" : {
         "message" : {
            "operator" : "or",
            "query" : "the quick brown"
         }
      }
   },
   "rescore" : {
      "window_size" : 10,
      "query" : {
         "rescore_query" : {
            "match_phrase" : {
               "message" : {
                  "query" : "the quick brown",
                  "slop" : 2
               }
            }
         },
         "query_weight" : 0.7,
         "rescore_query_weight" : 1.2
      }
   }
}
```

::::{note}
An error will be thrown if an explicit [`sort`](/reference/elasticsearch/rest-apis/sort-search-results.md)
(other than `_score` in descending order) is provided with a `rescore` query.
::::


The way the scores are combined can be controlled with the `score_mode`:

| Score Mode | Description                                                                                                                                                             |
| --- |-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `total` | Add the original score and the rescore query score. The default.                                                                                                        |
| `multiply` | Multiply the original score by the rescore query score. Useful for [`function query`](/reference/query-languages/query-dsl/query-dsl-function-score-query.md) rescores. |
| `avg` | Average the original score and the rescore query score.                                                                                                                 |
| `max` | Take the max of original score and the rescore query score.                                                                                                             |
| `min` | Take the min of the original score and the rescore query score.                                                                                                         |

## Script rescorer  [script-rescorer]
```{applies_to}
stack: ga 9.2
```

`script` rescorer uses a script to rescore the top documents returned
from the previous phase. The script has access to the original score as well
as values of document fields.

For example, the following script rescores documents based on the document's
original query score and the value of field `num_likes`:

```console
POST /_search
{
   "query" : {
      "match" : {
         "message" : {
            "operator" : "or",
            "query" : "the quick brown"
         }
      }
   },
   "rescore" : {
      "window_size" : 10,
      "script" : {
         "script" : {
            "source": "doc['num_likes'].value * params.multiplier + _score",
            "parameters": {
               "multiplier": 0.1
            }
         }
      }
   }
}
```

## Learning to rank rescorer [learning-to-rank-rescorer]

`learning_to_rank` uses an LTR model to rescore the top documents. You must
provide the `model_id` of a deployed model, as well as any named parameters
required by the query templates for features used by the model.

```console
GET my-index/_search
{
  "query": {
    "multi_match": {
      "fields": ["title", "content"],
      "query": "the quick brown fox"
    }
  },
  "rescore": {
    "learning_to_rank": {
      "model_id": "ltr-model",
      "params": {
        "query_text": "the quick brown fox"
      }
    },
    "window_size": 100
  }
}
```

## Multiple rescores [multiple-rescores]

You can apply multiple rescoring operations in sequence. The first rescorer
works on the top documents from the initial retrieval phase, while the second
rescorer works on the output of the first rescorer, and so on. A common practice
is to use a larger window for the first rescorer and smaller windows for more
expensive subsequent rescorers.

```console
POST /_search
{
  "query": {
    "match": {
      "message": {
        "operator": "or",
        "query": "the quick brown"
      }
    }
  },
  "rescore": [
    {
      "window_size": 10,
      "query": {
        "rescore_query": {
          "match_phrase": {
            "message": {
              "query": "the quick brown",
              "slop": 2
            }
          }
        },
        "query_weight": 0.7,
        "rescore_query_weight": 1.2
      }
    },
    {
      "window_size": 5,
      "query": {
        "score_mode": "multiply",
        "rescore_query": {
          "function_score": {
            "script_score": {
              "script": {
                "source": "Math.log10(doc.count.value + 2)"
              }
            }
          }
        }
      }
    }
  ]
}
```
