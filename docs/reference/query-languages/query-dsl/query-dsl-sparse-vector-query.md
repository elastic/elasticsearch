---
navigation_title: "Sparse vector"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-sparse-vector-query.html
---

# Sparse vector query [query-dsl-sparse-vector-query]


The sparse vector query executes a query consisting of sparse vectors, such as built by a learned sparse retrieval model. This can be achieved with one of two strategies:

* Using an {{nlp}} model to convert query text into a list of token-weight pairs
* Sending in precalculated token-weight pairs as query vectors

These token-weight pairs are then used in a query against a [sparse vector](/reference/elasticsearch/mapping-reference/sparse-vector.md) or a [semantic_text](/reference/elasticsearch/mapping-reference/semantic-text.md) field with a compatible sparse inference model. At query time, query vectors are calculated using the same inference model that was used to create the tokens. When querying, these query vectors are ORed together with their respective weights, which means scoring is effectively a [dot product](/reference/query-languages/query-dsl/query-dsl-script-score-query.md#vector-functions-dot-product) calculation between stored dimensions and query dimensions.

For example, a stored vector `{"feature_0": 0.12, "feature_1": 1.2, "feature_2": 3.0}` with query vector `{"feature_0": 2.5, "feature_2": 0.2}` would score the document `_score = 0.12*2.5 + 3.0*0.2 = 0.9`


## Example request using an {{nlp}} model [sparse-vector-query-ex-request]

```console
GET _search
{
   "query":{
      "sparse_vector": {
        "field": "ml.tokens",
        "inference_id": "the inference ID to produce the token weights",
        "query": "the query string"
      }
   }
}
```


## Example request using precomputed vectors [_example_request_using_precomputed_vectors]

```console
GET _search
{
   "query":{
      "sparse_vector": {
        "field": "ml.tokens",
        "query_vector": { "token1": 0.5, "token2": 0.3, "token3": 0.2 }
      }
   }
}
```


## Top level parameters for `sparse_vector` [sparse-vector-field-params]

`field`
:   (Required, string) The name of the field that contains the token-weight pairs to be searched against.

`inference_id`
:   (Optional, string) The [inference ID](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-inference) to use to convert the query text into token-weight pairs. It must be the same inference ID that was used to create the tokens from the input text. Only one of `inference_id` and `query_vector` is allowed. If `inference_id` is specified, `query` must also be specified. If all queried fields are of type [semantic_text](/reference/elasticsearch/mapping-reference/semantic-text.md), the inference ID associated with the `semantic_text` field will be inferred. You can reference a `deployment_id` of a {{ml}} trained model deployment as an `inference_id`. For example, if you download and deploy the ELSER model in the {{ml-cap}} trained models UI in {{kib}}, you can use the `deployment_id` of that deployment as the `inference_id`.

`query`
:   (Optional, string) The query text you want to use for search. If `inference_id` is specified, `query` must also be specified. If `query_vector` is specified, `query` must not be specified.

`query_vector`
:   (Optional, dictionary) A dictionary of token-weight pairs representing the precomputed query vector to search. Searching using this query vector will bypass additional inference. Only one of `inference_id` and `query_vector` is allowed.

`prune`
:   (Optional, boolean) Whether to perform pruning, omitting the non-significant tokens from the query to improve query performance. If `prune` is true but the `pruning_config` is not specified, pruning will occur but default values will be used. Default: false.

`pruning_config`
:   (Optional, object) Optional pruning configuration. If enabled, this will omit non-significant tokens from the query in order to improve query performance. This is only used if `prune` is set to `true`. If `prune` is set to `true` but `pruning_config` is not specified, default values will be used.

    Parameters for `pruning_config` are:

    `tokens_freq_ratio_threshold`
    :   (Optional, integer) Tokens whose frequency is more than `tokens_freq_ratio_threshold` times the average frequency of all tokens in the specified field are considered outliers and pruned. This value must between 1 and 100. Default: `5`.

    `tokens_weight_threshold`
    :   (Optional, float) Tokens whose weight is less than `tokens_weight_threshold` are considered insignificant and pruned. This value must be between 0 and 1. Default: `0.4`.

    `only_score_pruned_tokens`
    :   (Optional, boolean) If `true` we only input pruned tokens into scoring, and discard non-pruned tokens. It is strongly recommended to set this to `false` for the main query, but this can be set to `true` for a rescore query to get more relevant results. Default: `false`.

    ::::{note}
    The default values for `tokens_freq_ratio_threshold` and `tokens_weight_threshold` were chosen based on tests using ELSERv2 that provided the most optimal results.
    ::::



## Example ELSER query [sparse-vector-query-example]

The following is an example of the `sparse_vector` query that references the ELSER model to perform semantic search. For a more detailed description of how to perform semantic search by using ELSER and the `sparse_vector` query, refer to [this tutorial](docs-content://solutions/search/semantic-search/semantic-search-elser-ingest-pipelines.md).

```console
GET my-index/_search
{
   "query":{
      "sparse_vector": {
         "field": "ml.tokens",
         "inference_id": "my-elser-model",
         "query": "How is the weather in Jamaica?"
      }
   }
}
```

Multiple `sparse_vector` queries can be combined with each other or other query types. This can be achieved by wrapping them in [boolean query clauses](/reference/query-languages/query-dsl/query-dsl-bool-query.md) and using linear boosting:

```console
GET my-index/_search
{
  "query": {
    "bool": {
      "should": [
        {
          "sparse_vector": {
            "field": "ml.inference.title_expanded.predicted_value",
            "inference_id": "my-elser-model",
            "query": "How is the weather in Jamaica?",
            "boost": 1
          }
        },
        {
          "sparse_vector": {
            "field": "ml.inference.description_expanded.predicted_value",
            "inference_id": "my-elser-model",
            "query": "How is the weather in Jamaica?",
            "boost": 1
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
              "sparse_vector": {
                "field": "ml.inference.title_expanded.predicted_value",
                "inference_id": "my-elser-model",
                "query": "How is the weather in Jamaica?",
                "boost": 1
              }
            }
          }
        },
        {
          "standard": {
            "query": {
              "sparse_vector": {
                "field": "ml.inference.description_expanded.predicted_value",
                "inference_id": "my-elser-model",
                "query": "How is the weather in Jamaica?",
                "boost": 1
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


## Example ELSER query with pruning configuration and rescore [sparse-vector-query-with-pruning-config-and-rescore-example]

The following is an extension to the above example that adds a pruning configuration to the `sparse_vector` query. The pruning configuration identifies non-significant tokens to prune from the query in order to improve query performance.

Token pruning happens at the shard level. While this should result in the same tokens being labeled as insignificant across shards, this is not guaranteed based on the composition of each shard. Therefore, if you are running `sparse_vector` with a `pruning_config` on a multi-shard index, we strongly recommend adding a [Rescore filtered search results](/reference/elasticsearch/rest-apis/filter-search-results.md#rescore) function with the tokens that were originally pruned from the query. This will help mitigate any shard-level inconsistency with pruned tokens and provide better relevance overall.

```console
GET my-index/_search
{
   "query":{
      "sparse_vector":{
         "field": "ml.tokens",
         "inference_id": "my-elser-model",
         "query":"How is the weather in Jamaica?",
         "prune": true,
         "pruning_config": {
           "tokens_freq_ratio_threshold": 5,
           "tokens_weight_threshold": 0.4,
           "only_score_pruned_tokens": false
         }
      }
   },
   "rescore": {
      "window_size": 100,
      "query": {
         "rescore_query": {
            "sparse_vector": {
               "field": "ml.tokens",
               "inference_id": "my-elser-model",
               "query": "How is the weather in Jamaica?",
               "prune": true,
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
```

::::{note}
When performing [cross-cluster search](docs-content://solutions/search/cross-cluster-search.md), inference is performed on the local cluster.
::::


