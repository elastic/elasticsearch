---
navigation_title: "Weighted tokens"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-weighted-tokens-query.html
---

# Weighted tokens query [query-dsl-weighted-tokens-query]


::::{admonition} Deprecated in 8.15.0.
:class: warning

This query has been replaced by the [Sparse vector](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md) and will be removed in an upcoming release.
::::


::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The weighted tokens query requires a list of token-weight pairs that are sent in with a query rather than calculated using a {{nlp}} model. These token pairs are then used in a query against a [sparse vector](/reference/elasticsearch/mapping-reference/sparse-vector.md) or [rank features](/reference/elasticsearch/mapping-reference/rank-features.md) field.

Weighted tokens queries are useful when you want to use an external query expansion model, or quickly prototype changes without reindexing a new model.


### Example request [weighted-tokens-query-ex-request]

```console
POST _search
{
  "query": {
    "weighted_tokens": {
      "query_expansion_field": {
        "tokens": {"2161": 0.4679, "2621": 0.307, "2782": 0.1299, "2851": 0.1056, "3088": 0.3041, "3376": 0.1038, "3467": 0.4873, "3684": 0.8958, "4380": 0.334, "4542": 0.4636, "4633": 2.2805, "4785": 1.2628, "4860": 1.0655, "5133": 1.0709, "7139": 1.0016, "7224": 0.2486, "7387": 0.0985, "7394": 0.0542, "8915": 0.369, "9156": 2.8947, "10505": 0.2771, "11464": 0.3996, "13525": 0.0088, "14178": 0.8161, "16893": 0.1376, "17851": 1.5348, "19939": 0.6012},
        "pruning_config": {
          "tokens_freq_ratio_threshold": 5,
          "tokens_weight_threshold": 0.4,
          "only_score_pruned_tokens": false
        }
      }
    }
  }
}
```
% TEST[skip: TBD]


## Top level parameters for `weighted_token` [weighted-token-query-params]

### `<tokens>`

(Required, dictionary) A dictionary of token-weight pairs.

`pruning_config`
:   (Optional, object) Optional pruning configuration. If enabled, this will omit non-significant tokens from the query in order to improve query performance. Default: Disabled.

    Parameters for `<pruning_config>` are:

    `tokens_freq_ratio_threshold`
    :   (Optional, integer) Tokens whose frequency is more than `tokens_freq_ratio_threshold` times the average frequency of all tokens in the specified field are considered outliers and pruned. This value must between 1 and 100. Default: `5`.

    `tokens_weight_threshold`
    :   (Optional, float) Tokens whose weight is less than `tokens_weight_threshold` are considered insignificant and pruned. This value must be between 0 and 1. Default: `0.4`.

    `only_score_pruned_tokens`
    :   (Optional, boolean) If `true` we only input pruned tokens into scoring, and discard non-pruned tokens. It is strongly recommended to set this to `false` for the main query, but this can be set to `true` for a rescore query to get more relevant results. Default: `false`.

    ::::{note}
    The default values for `tokens_freq_ratio_threshold` and `tokens_weight_threshold` were chosen based on tests using ELSER that provided the most optimal results.
    ::::



### Example weighted tokens query with pruning configuration and rescore [weighted-tokens-query-with-pruning-config-and-rescore-example]

The following example adds a pruning configuration to the `text_expansion` query. The pruning configuration identifies non-significant tokens to prune from the query in order to improve query performance.

Token pruning happens at the shard level. While this should result in the same tokens being labeled as insignificant across shards, this is not guaranteed based on the composition of each shard. Therefore, if you are running `text_expansion` with a `pruning_config` on a multi-shard index, we strongly recommend adding a [Rescore filtered search results](/reference/elasticsearch/rest-apis/filter-search-results.md#rescore) function with the tokens that were originally pruned from the query. This will help mitigate any shard-level inconsistency with pruned tokens and provide better relevance overall.

```console
GET my-index/_search
{
   "query":{
      "weighted_tokens": {
      "query_expansion_field": {
        "tokens": {"2161": 0.4679, "2621": 0.307, "2782": 0.1299, "2851": 0.1056, "3088": 0.3041, "3376": 0.1038, "3467": 0.4873, "3684": 0.8958, "4380": 0.334, "4542": 0.4636, "4633": 2.2805, "4785": 1.2628, "4860": 1.0655, "5133": 1.0709, "7139": 1.0016, "7224": 0.2486, "7387": 0.0985, "7394": 0.0542, "8915": 0.369, "9156": 2.8947, "10505": 0.2771, "11464": 0.3996, "13525": 0.0088, "14178": 0.8161, "16893": 0.1376, "17851": 1.5348, "19939": 0.6012},
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
            "weighted_tokens": {
              "query_expansion_field": {
                "tokens": {"2161": 0.4679, "2621": 0.307, "2782": 0.1299, "2851": 0.1056, "3088": 0.3041, "3376": 0.1038, "3467": 0.4873, "3684": 0.8958, "4380": 0.334, "4542": 0.4636, "4633": 2.2805, "4785": 1.2628, "4860": 1.0655, "5133": 1.0709, "7139": 1.0016, "7224": 0.2486, "7387": 0.0985, "7394": 0.0542, "8915": 0.369, "9156": 2.8947, "10505": 0.2771, "11464": 0.3996, "13525": 0.0088, "14178": 0.8161, "16893": 0.1376, "17851": 1.5348, "19939": 0.6012},
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
% TEST[skip: TBD]

