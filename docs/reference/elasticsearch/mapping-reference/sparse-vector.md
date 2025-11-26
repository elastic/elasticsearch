---
applies_to:
  stack:
  serverless:
products:
  - id: elasticsearch
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

## Token pruning
```{applies_to}
stack: ga 9.1
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

index_options {applies_to}`stack: ga 9.1`
:   (Optional, object) You can set index options for your  `sparse_vector` field to determine if you should prune tokens, and the parameter configurations for the token pruning. If pruning options are not set in your [`sparse_vector` query](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md), Elasticsearch will use the default options configured for the field, if any.

Parameters for `index_options` are:

`prune` {applies_to}`stack: ga 9.1`
:   (Optional, boolean) Whether to perform pruning, omitting the non-significant tokens from the query to improve query performance. If `prune` is true but the `pruning_config` is not specified, pruning will occur but default values will be used. Default: true.

`pruning_config` {applies_to}`stack: ga 9.1`
:   (Optional, object) Optional pruning configuration. If enabled, this will omit non-significant tokens from the query in order to improve query performance. This is only used if `prune` is set to `true`. If `prune` is set to `true` but `pruning_config` is not specified, default values will be used. If `prune` is set to false but `pruning_config` is specified, an exception will occur.

    Parameters for `pruning_config` include:

    `tokens_freq_ratio_threshold` {applies_to}`stack: ga 9.1`
    :   (Optional, integer) Tokens whose frequency is more than `tokens_freq_ratio_threshold` times the average frequency of all tokens in the specified field are considered outliers and pruned. This value must between 1 and 100. Default: `5`.

    `tokens_weight_threshold` {applies_to}`stack: ga 9.1`
    :   (Optional, float) Tokens whose weight is less than `tokens_weight_threshold` are considered insignificant and pruned. This value must be between 0 and 1. Default: `0.4`.

    ::::{note}
    The default values for `tokens_freq_ratio_threshold` and `tokens_weight_threshold` were chosen based on tests using ELSERv2 that provided the optimal results.
    ::::

When token pruning is applied, non-significant tokens will be pruned from the query.
Non-significant tokens can be defined as tokens that meet both of the following criteria:
* The token appears much more frequently than most tokens, indicating that it is a very common word and may not benefit the overall search results much.
* The weight/score is so low that the token is likely not very relevant to the original term

Both the token frequency threshold and weight threshold must show the token is non-significant in order for the token to be pruned.
This ensures that:
* The tokens that are kept are frequent enough and have significant scoring.
* Very infrequent tokens that may not have as high of a score are removed.

## Accessing `sparse_vector` fields in search responses
```{applies_to}
stack: ga 9.2
serverless: ga
```

By default, `sparse_vector` fields are **not included in `_source`** in responses from the `_search`, `_msearch`, `_get`, and `_mget` APIs.
This helps reduce response size and improve performance, especially in scenarios where vectors are used solely for similarity scoring and not required in the output.

To retrieve vector values explicitly, you can use:

* The `fields` option to request specific vector fields directly:

```console
POST my-index-2/_search
{
  "fields": ["my_vector"]
}
```
% TEST[skip:no index setup]

- The `_source.exclude_vectors` flag to re-enable vector inclusion in `_source` responses:

```console
POST my-index-2/_search
{
  "_source": {
    "exclude_vectors": false
  }
}
```
% TEST[skip:no index setup]

:::{tip}
For more context about the decision to exclude vectors from `_source` by default, read the [blog post](https://www.elastic.co/search-labs/blog/elasticsearch-exclude-vectors-from-source).
:::

### Storage behavior and `_source`

By default, `sparse_vector` fields are not stored in `_source` on disk. This is also controlled by the index setting `index.mapping.exclude_source_vectors`.
This setting is enabled by default for newly created indices and can only be set at index creation time.

When enabled:

* `sparse_vector` fields are removed from `_source` and the rest of the `_source` is stored as usual.
* If a request includes `_source` and vector values are needed (e.g., during recovery or reindex), the vectors are rehydrated from their internal format.

This setting is compatible with synthetic `_source`, where the entire `_source` document is reconstructed from columnar storage. In full synthetic mode, no `_source` is stored on disk, and all fields — including vectors — are rebuilt when needed.

### Rehydration and precision

When vector values are rehydrated (e.g., for reindex, recovery, or explicit `_source` requests), they are restored from their internal format.
Internally, vectors are stored as floats with 9 significant bits for the precision, so the rehydrated values will have reduced precision.
This lossy representation is intended to save space while preserving search quality.

### Storing original vectors in `_source`

If you want to preserve the original vector values exactly as they were provided, you can re-enable vector storage in `_source`:

```console
PUT my-index-include-vectors
{
  "settings": {
    "index.mapping.exclude_source_vectors": false
  },
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "sparse_vector"
      }
    }
  }
}
```

When this setting is disabled:

* `sparse_vector` fields are stored as part of the `_source`, exactly as indexed.
* The index will store both the original `_source` value and the internal representation used for vector search, resulting in increased storage usage.
* Vectors are once again returned in `_source` by default in all relevant APIs, with no need to use `exclude_vectors` or `fields`.

This configuration is appropriate when full source fidelity is required, such as for auditing or round-tripping exact input values.

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

## Updating `sparse_vector` fields

When using the [Update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) with the `doc` parameter, `sparse_vector` fields behave like nested objects and are **merged** rather than replaced. This means:

- Existing tokens in the sparse vector are preserved
- New tokens are added
- Tokens present in both the existing and new data will have their values updated

This is different from primitive array fields (like `keyword`), which are replaced entirely during updates.

### Example of merging behavior

Original document:

```console
PUT /my-index/_doc/1
{
  "my_vector": {
    "token_a": 0.5,
    "token_b": 0.8
  }
}
```

Partial update:

```console
POST /my-index/_update/1
{
  "doc": {
    "my_vector": {
      "token_c": 0.3
    }
  }
}
```
% TEST[continued]

Observe that tokens are merged, not replaced:

```js
{
  "my_vector": {
    "token_a": 0.5,
    "token_b": 0.8,
    "token_c": 0.3
  }
}
```
% NOTCONSOLE

### Replacing the entire `sparse_vector` field

To replace the entire contents of a `sparse_vector` field, use a [script](docs-content://explore-analyze/scripting/modules-scripting-using.md) in your update request:

```console
POST /my-index/_update/1
{
  "script": {
    "source": "ctx._source.my_vector = params.new_vector",
    "params": {
      "new_vector": {
        "token_x": 1.0,
        "token_y": 0.6
      }
    }
  }
}
```
% TEST[continued]

:::{note}
This same merging behavior also applies to [`rank_features` fields](/reference/elasticsearch/mapping-reference/rank-features.md), because they are also object-like structures.
:::

## Important notes and limitations

- `sparse_vector` fields cannot be included in indices that were **created** on {{es}} versions between 8.0 and 8.10
- `sparse_vector` fields only support strictly positive values. Negative values will be rejected.
- `sparse_vector` fields do not support [analyzers](docs-content://manage-data/data-store/text-analysis.md), querying, sorting or aggregating. They may only be used within specialized queries. The recommended query to use on these fields are [`sparse_vector`](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md) queries. They may also be used within legacy [`text_expansion`](/reference/query-languages/query-dsl/query-dsl-text-expansion-query.md) queries.
- `sparse_vector` fields only preserve 9 significant bits for the precision, which translates to a relative error of about 0.4%.
