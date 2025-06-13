---
navigation_title: "Sparse vector"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sparse-vector.html
---

# Sparse vector field type [sparse-vector]


A `sparse_vector` field can index features and weights so that they can later be used to query documents in queries with a [`sparse_vector`](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md). This field can also be used with a legacy [`text_expansion`](/reference/query-languages/query-dsl/query-dsl-text-expansion-query.md) query.

`sparse_vector` is the field type that should be used with [ELSER mappings](docs-content://solutions/search/semantic-search/semantic-search-elser-ingest-pipelines.md#elser-mappings).

```console
PUT my-index
{
  "mappings": {
    "properties": {
      "text.tokens": {
        "type": "sparse_vector"
      }
    }
  }
}
```

With any new indices created, token pruning will be turned on by default with appropriate defaults. You can control this behaviour using the optional `index_options` parameters for the field:

```console
PUT my-index
{
  "mappings": {
    "properties": {
      "text.tokens": {
        "type": "sparse_vector",
        "index_options": {
          "prune": true,
          "pruning_config": {
            "tokens_freq_ratio_threshold": 5,
            "tokens_weight_threshold": 0.4
          }
        }
      }
    }
  }
}
```

See [semantic search with ELSER](docs-content://solutions/search/semantic-search/semantic-search-elser-ingest-pipelines.md) for a complete example on adding documents to a `sparse_vector` mapped field using ELSER.

## Parameters for `sparse_vector` fields [sparse-vectors-params]

The following parameters are accepted by `sparse_vector` fields:

[store](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Indicates whether the field value should be stored and retrievable independently of the [_source](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepted values: true or false (default). The field’s data is stored using term vectors, a disk-efficient structure compared to the original JSON input. The input map can be retrieved during a search request via the [`fields` parameter](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#search-fields-param). To benefit from reduced disk usage, you must either:

    * Exclude the field from [_source](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering).
    * Use [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source).

index_options
:   (Optional, object) You can set index options for your  `sparse_vector` field to determine if you should prune tokens, and the parameter configurations for the token pruning. If pruning options are not set in your `sparse_query` vector, Elasticsearch will use the default options configured for the field, if any. The available options for the index options are:

Parameters for `index_options` are:

`prune` {applies_to}`stack: preview 9.1`
:   (Optional, boolean) Whether to perform pruning, omitting the non-significant tokens from the query to improve query performance. If `prune` is true but the `pruning_config` is not specified, pruning will occur but default values will be used. Default: true.

`pruning_config` {applies_to}`stack: preview 9.1`
:   (Optional, object) Optional pruning configuration. If enabled, this will omit non-significant tokens from the query in order to improve query performance. This is only used if `prune` is set to `true`. If `prune` is set to `true` but `pruning_config` is not specified, default values will be used. If `prune` is set to false but `pruning_config` is specified, an exception will occur.

    Parameters for `pruning_config` include:

    `tokens_freq_ratio_threshold` {applies_to}`stack: preview 9.1`
    :   (Optional, integer) Tokens whose frequency is more than `tokens_freq_ratio_threshold` times the average frequency of all tokens in the specified field are considered outliers and pruned. This value must between 1 and 100. Default: `5`.

    `tokens_weight_threshold` {applies_to}`stack: preview 9.1`
    :   (Optional, float) Tokens whose weight is less than `tokens_weight_threshold` are considered insignificant and pruned. This value must be between 0 and 1. Default: `0.4`.

    ::::{note}
    The default values for `tokens_freq_ratio_threshold` and `tokens_weight_threshold` were chosen based on tests using ELSERv2 that provided the most optimal results.
    ::::

When token pruning is applied, non-significant tokens will be pruned from the query.
Non-significant tokens can be defined as tokens that meet both of the following criteria:
* The token appears much more frequently than most tokens, indicating that it is a very common word and may not benefit the overall search results much.
* The weight/score is so low that the token is likely not very relevant to the original term

Both the token frequency threshold and weight threshold must show the token is non-significant in order for the token to be pruned.
This ensures the tokens that are kept are frequent enough and have very high scoring or very infrequent tokens that may not have as high of a score.


## Multi-value sparse vectors [index-multi-value-sparse-vectors]

When passing in arrays of values for sparse vectors the max value for similarly named features is selected.

The paper Adapting Learned Sparse Retrieval for Long Documents ([https://arxiv.org/pdf/2305.18494.pdf](https://arxiv.org/pdf/2305.18494.pdf)) discusses this in more detail. In summary, research findings support representation aggregation typically outperforming score aggregation.

For instances where you want to have overlapping feature names use should store them separately or use nested fields.

Below is an example of passing in a document with overlapping feature names. Consider that in this example two categories exist for positive sentiment and negative sentiment. However, for the purposes of retrieval we also want the overall impact rather than specific sentiment. In the example `impact` is stored as a multi-value sparse vector and only the max values of overlapping names are stored. More specifically the final `GET` query here returns a `_score` of ~1.2 (which is the `max(impact.delicious[0], impact.delicious[1])` and is approximate because we have a relative error of 0.4% as explained below)

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "text": {
        "type": "text",
        "analyzer": "standard"
      },
      "impact": {
        "type": "sparse_vector"
      },
      "positive": {
        "type": "sparse_vector"
      },
      "negative": {
        "type": "sparse_vector"
      }
    }
  }
}

POST my-index-000001/_doc
{
    "text": "I had some terribly delicious carrots.",
    "impact": [{"I": 0.55, "had": 0.4, "some": 0.28, "terribly": 0.01, "delicious": 1.2, "carrots": 0.8},
               {"I": 0.54, "had": 0.4, "some": 0.28, "terribly": 2.01, "delicious": 0.02, "carrots": 0.4}],
    "positive": {"I": 0.55, "had": 0.4, "some": 0.28, "terribly": 0.01, "delicious": 1.2, "carrots": 0.8},
    "negative": {"I": 0.54, "had": 0.4, "some": 0.28, "terribly": 2.01, "delicious": 0.02, "carrots": 0.4}
}

GET my-index-000001/_search
{
  "query": {
    "term": {
      "impact": {
         "value": "delicious"
      }
    }
  }
}
```

::::{note}
`sparse_vector` fields can not be included in indices that were **created** on {{es}} versions between 8.0 and 8.10
::::


::::{note}
`sparse_vector` fields only support strictly positive values. Negative values will be rejected.
::::


::::{note}
`sparse_vector` fields do not support [analyzers](docs-content://manage-data/data-store/text-analysis.md), querying, sorting or aggregating. They may only be used within specialized queries. The recommended query to use on these fields are [`sparse_vector`](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md) queries. They may also be used within legacy [`text_expansion`](/reference/query-languages/query-dsl/query-dsl-text-expansion-query.md) queries.
::::


::::{note}
`sparse_vector` fields only preserve 9 significant bits for the precision, which translates to a relative error of about 0.4%.
::::



