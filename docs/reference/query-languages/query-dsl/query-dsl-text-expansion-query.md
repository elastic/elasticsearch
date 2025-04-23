---
navigation_title: "Text expansion"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-text-expansion-query.html
---

# Text expansion query [query-dsl-text-expansion-query]


::::{admonition} Deprecated in 8.15.0.
:class: warning

This query has been replaced by [Sparse vector](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md).
::::


::::{admonition} Deprecation usage note
You can continue using `rank_features` fields with `text_expansion` queries in the current version. However, if you plan to upgrade, we recommend updating mappings to use the `sparse_vector` field type and [reindexing your data](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex). This will allow you to take advantage of the new capabilities and improvements available in newer versions.

::::


The text expansion query uses a {{nlp}} model to convert the query text into a list of token-weight pairs which are then used in a query against a [sparse vector](/reference/elasticsearch/mapping-reference/sparse-vector.md) or [rank features](/reference/elasticsearch/mapping-reference/rank-features.md) field.


## Example request [text-expansion-query-ex-request]

```console
GET _search
{
   "query":{
      "text_expansion":{
         "<sparse_vector_field>":{
            "model_id":"the model to produce the token weights",
            "model_text":"the query string"
         }
      }
   }
}
```


## Top level parameters for `text_expansion` [text-expansion-query-params]

`<sparse_vector_field>`
:   (Required, object) The name of the field that contains the token-weight pairs the NLP model created based on the input text.


## Top level parameters for `<sparse_vector_field>` [text-expansion-rank-feature-field-params]

`model_id`
:   (Required, string) The ID of the model to use to convert the query text into token-weight pairs. It must be the same model ID that was used to create the tokens from the input text.

`model_text`
:   (Required, string) The query text you want to use for search.

`pruning_config`
:   (Optional, object) [preview] Optional pruning configuration. If enabled, this will omit non-significant tokens from the query in order to improve query performance. Default: Disabled.

    Parameters for `<pruning_config>` are:

    `tokens_freq_ratio_threshold`
    :   (Optional, integer) [preview] Tokens whose frequency is more than `tokens_freq_ratio_threshold` times the average frequency of all tokens in the specified field are considered outliers and pruned. This value must between 1 and 100. Default: `5`.

    `tokens_weight_threshold`
    :   (Optional, float) [preview] Tokens whose weight is less than `tokens_weight_threshold` are considered insignificant and pruned. This value must be between 0 and 1. Default: `0.4`.

    `only_score_pruned_tokens`
    :   (Optional, boolean) [preview] If `true` we only input pruned tokens into scoring, and discard non-pruned tokens. It is strongly recommended to set this to `false` for the main query, but this can be set to `true` for a rescore query to get more relevant results. Default: `false`.

    ::::{note}
    The default values for `tokens_freq_ratio_threshold` and `tokens_weight_threshold` were chosen based on tests using ELSER that provided the most optimal results.
    ::::



## Example ELSER query [text-expansion-query-example]

The following is an example of the `text_expansion` query that references the ELSER model to perform semantic search. For a more detailed description of how to perform semantic search by using ELSER and the `text_expansion` query, refer to [this tutorial](docs-content://solutions/search/semantic-search/semantic-search-elser-ingest-pipelines.md).

```console
GET my-index/_search
{
   "query":{
      "text_expansion":{
         "ml.tokens":{
            "model_id":".elser_model_2",
            "model_text":"How is the weather in Jamaica?"
         }
      }
   }
}
```

Multiple `text_expansion` queries can be combined with each other or other query types. This can be achieved by wrapping them in [boolean query clauses](/reference/query-languages/query-dsl/query-dsl-bool-query.md) and using linear boosting:

```console
GET my-index/_search
{
  "query": {
    "bool": {
      "should": [
        {
          "text_expansion": {
            "ml.inference.title_expanded.predicted_value": {
              "model_id": ".elser_model_2",
              "model_text": "How is the weather in Jamaica?",
              "boost": 1
            }
          }
        },
        {
          "text_expansion": {
            "ml.inference.description_expanded.predicted_value": {
              "model_id": ".elser_model_2",
              "model_text": "How is the weather in Jamaica?",
              "boost": 1
            }
          }
        },
        {
          "multi_match": {
            "query": "How is the weather in Jamaica?",
            "fields": [
              "title",
              "description"
            ],
            "boost": 4
          }
        }
      ]
    }
  }
}
```

This can also be achieved using [reciprocal rank fusion (RRF)](/reference/elasticsearch/rest-apis/reciprocal-rank-fusion.md), through an [`rrf` retriever](/reference/elasticsearch/rest-apis/retrievers.md#rrf-retriever) with multiple [`standard` retrievers](/reference/elasticsearch/rest-apis/retrievers.md#standard-retriever).

```console
GET my-index/_search
{
  "retriever": {
    "rrf": {
      "retrievers": [
        {
          "standard": {
            "query": {
              "multi_match": {
                "query": "How is the weather in Jamaica?",
                "fields": [
                  "title",
                  "description"
                ]
              }
            }
          }
        },
        {
          "standard": {
            "query": {
              "text_expansion": {
                "ml.inference.title_expanded.predicted_value": {
                  "model_id": ".elser_model_2",
                  "model_text": "How is the weather in Jamaica?"
                }
              }
            }
          }
        },
        {
          "standard": {
            "query": {
              "text_expansion": {
                "ml.inference.description_expanded.predicted_value": {
                  "model_id": ".elser_model_2",
                  "model_text": "How is the weather in Jamaica?"
                }
              }
            }
          }
        }
      ],
      "window_size": 10,
      "rank_constant": 20
    }
  }
}
```


## Example ELSER query with pruning configuration and rescore [text-expansion-query-with-pruning-config-and-rescore-example]

The following is an extension to the above example that adds a [preview] pruning configuration to the `text_expansion` query. The pruning configuration identifies non-significant tokens to prune from the query in order to improve query performance.

Token pruning happens at the shard level. While this should result in the same tokens being labeled as insignificant across shards, this is not guaranteed based on the composition of each shard. Therefore, if you are running `text_expansion` with a `pruning_config` on a multi-shard index, we strongly recommend adding a [Rescore filtered search results](/reference/elasticsearch/rest-apis/filter-search-results.md#rescore) function with the tokens that were originally pruned from the query. This will help mitigate any shard-level inconsistency with pruned tokens and provide better relevance overall.

```console
GET my-index/_search
{
   "query":{
      "text_expansion":{
         "ml.tokens":{
            "model_id":".elser_model_2",
            "model_text":"How is the weather in Jamaica?",
            "pruning_config": {
               "tokens_freq_ratio_threshold": 5,
               "tokens_weight_threshold": 0.4,
               "only_score_pruned_tokens": false
           }
         }
      }
   },
   "rescore": {
      "window_size": 100,
      "query": {
         "rescore_query": {
            "text_expansion": {
               "ml.tokens": {
                  "model_id": ".elser_model_2",
                  "model_text": "How is the weather in Jamaica?",
                  "pruning_config": {
                     "tokens_freq_ratio_threshold": 5,
                     "tokens_weight_threshold": 0.4,
                     "only_score_pruned_tokens": true
                  }
               }
            }
         }
      }
   }
}
```

::::{note}
Depending on your data, the text expansion query may be faster with `track_total_hits: false`.

::::


