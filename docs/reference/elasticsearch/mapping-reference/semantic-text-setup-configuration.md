---
navigation_title: "Set up and configure"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Set up and configure `semantic_text` fields [set-up-configuration-semantic-text]

This page provides instructions for setting up and configuring `semantic_text` fields. Learn how to configure {{infer}} endpoints, including [default](#default-endpoints) and [preconfigured](#preconfigured-endpoints) options, ELSER on EIS, custom endpoints, and dedicated endpoints for ingestion and search operations.

## Configure {{infer}} endpoints [configure-inference-endpoints]

You can configure {{infer}} endpoints for `semantic_text` fields in the following ways:

- [Use ELSER on EIS](#using-elser-on-eis)
- [Default](#default-endpoints) and [preconfigured](#preconfigured-endpoints) endpoints
- [Use a custom {{infer}} endpoint](#using-custom-endpoint)

:::{note}
If you use a [custom {{infer}} endpoint](#using-custom-endpoint) through your ML node and not through Elastic {{infer-cap}} Service (EIS), the recommended method is to [use dedicated endpoints for ingestion and search](#dedicated-endpoints-for-ingestion-and-search).

{applies_to}`stack: ga 9.1.0` If you use EIS, you don't have to set up dedicated endpoints.
:::

### Use default endpoints [default-endpoints]

A default endpoint is the {{infer}} endpoint that is used when you create a `semantic_text` field without specifying an `inference_id`.

The following example shows a `semantic_text` field configured to use the default {{infer}} endpoint:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text"
      }
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]



The default {{infer}} endpoint varies by deployment type and version:

- {applies_to}`stack: ga 9.4` {applies_to}`serverless: ga` On {{ech}} deployments running {{stack}} 9.4+ and on {{serverless-short}}, the `inference_id` parameter defaults to `.jina-embeddings-v5-text-small` and runs on [EIS](docs-content://explore-analyze/elastic-inference/eis.md). 

  :::{important}
  Jina models are not supported for deployment on [{{ml}} nodes](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#ml-node-role). They can be used through [EIS](docs-content://explore-analyze/elastic-inference/eis.md) or [external {{infer}}](docs-content://explore-analyze/elastic-inference/external.md) providers, and therefore require external network connectivity. Fully air-gapped environments are not currently supported.
  :::

- {applies_to}`stack: ga 9.3` On {{ech}} deployments running {{stack}} 9.3, the `inference_id` parameter defaults to `.elser-2-elastic` and runs on [EIS](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis).

- {applies_to}`stack: ga 9.0-9.2` On {{ech}} deployments running {{stack}} 9.0-9.2, the `inference_id` parameter defaults to `.elser-2-elasticsearch` and runs on the `elasticsearch` service.

If you use the default {{infer}} endpoint, it might be updated to a newer version and use a different embedding model than the previous default endpoints. Queries that target these indices together can produce unexpected ranking results.
For details, refer to [potential issues when mixing embedding models across indices](#default-endpoint-considerations).

:::::{dropdown} Potential issues when mixing embedding models across indices
:name: default-endpoint-considerations

If a `semantic_text` field relies on the default {{infer}} endpoint, the model used to generate embeddings might change across versions or deployments. This can result in indices using different embedding models.

For example, if the `semantic_text` field is created without specifying `inference_id`, indices created on {{ecloud}} 9.3 use the `.elser-2-elastic` endpoint by default, while indices created on {{ecloud}} 9.4+ use `.jina-embeddings-v5-text-small`. As a result, older indices contain ELSER embeddings while newer indices contain Jina embeddings.

Mixed embedding models across indices can occur in several common scenarios, including:

- [Data streams](docs-content://manage-data/data-store/data-streams.md) with [{{ilm-init}} rollover](docs-content://manage-data/lifecycle/index-lifecycle-management.md): When a data stream rolls over, older backing indices may contain ELSER embeddings while newer indices created after an upgrade use Jina embeddings.
- [Aliases](docs-content://manage-data/data-store/aliases.md) referencing multiple indices: An alias can point to several indices created at different times. If some indices use ELSER and others use Jina, searches against the alias will query both.
- Explicit [multi-index searches](/reference/elasticsearch/rest-apis/search-multiple-data-streams-indices.md): Queries that target multiple indices (for example `GET index1,index2/_search`) might combine results from indices using different embedding models.
- [{{ccs-cap}}](docs-content://explore-analyze/cross-cluster-search.md): Searches across multiple clusters may query indices created on different stack versions, which may use different default {{infer}} endpoints.

Queries that target indices using different embedding models can lead to issues or unexpected results. The following sections describe these issues and how to mitigate them.

#### Incorrect ranking due to different scoring scales

ELSER and Jina use different scoring ranges. ELSER scores typically range between `0` and above `10`, while Jina scores are normalized between `0` and `1`.

When results from both models are ranked together, ELSER documents might appear ahead of Jina documents even if the Jina results are more relevant. This can lead to misleading rankings without any errors being returned.

To mitigate this issue, ensure that indices queried together use the same embedding model by explicitly specifying the `inference_id` when defining `semantic_text` fields:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text",
        "inference_id": ".jina-embeddings-v5-text-small"
      }
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]

If indices using different embedding models must be queried together, normalize the scores using a [linear retriever](/reference/elasticsearch/rest-apis/retrievers/linear-retriever.md):

```console
GET my-index/_search
{
  "retriever": {
    "linear": {
      "query": "how do neural networks learn",
      "fields": ["inference_field"],
      "normalizer": "minmax"
    }
  }
}
```

#### Increased {{infer}} cost during search

When a query targets indices that use different {{infer}} endpoints, {{es}} must generate query embeddings for each model. This increases {{infer}} workload and cost during search.

To mitigate this issue, ensure that indices queried together use the same {{infer}} endpoint. You can do this by:

- explicitly setting the `inference_id` when defining the `semantic_text` field for new indices, or
- by [reindexing](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex) older indices with the desired endpoint.

#### Alerts based on raw relevance scores might stop triggering

Some [alerts](docs-content://explore-analyze/alerting/alerts.md) or [rules](docs-content://explore-analyze/alerting/alerts/rule-types.md) rely on raw `_score` values. Because ELSER and Jina use different score ranges, score thresholds designed for ELSER might no longer work when results are generated with Jina.

For example, a condition such as `_score > 10` might never be satisfied by Jina results, because Jina scores are normalized between `0` and `1`.

To mitigate this issue, adjust alert thresholds to match the scoring range of the embedding model being used, or avoid relying on raw `_score` values in alert conditions.

:::::

### Use preconfigured endpoints [preconfigured-endpoints]

Preconfigured endpoints are {{infer}} endpoints that are automatically available in the deployment or project and do not require manual creation. The available preconfigured endpoints vary across deployment types and versions.

To view the list of available preconfigured endpoints for your deployment, go to **{{infer-cap}} endpoints** in {{kib}}.

To use a preconfigured endpoint, set the `inference_id` parameter to the identifier of the endpoint you want to use:

```console
PUT my-index-000004
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text",
        "inference_id": ".jina-embeddings-v5-text-nano"
      }
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]

### Use ELSER on EIS [using-elser-on-eis]

```{applies_to}
stack: preview =9.1, ga 9.2+
deployment:
  self: unavailable
serverless: ga
```

If you use the preconfigured `.elser-2-elastic` endpoint that utilizes the ELSER model as a service through the Elastic {{infer-cap}} Service ([ELSER on EIS](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis)), you can
set up `semantic_text` with the following API request:

:::::::{tab-set}

::::::{tab-item} Using ELSER on EIS on Serverless

```{applies_to}
serverless: ga
```

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text"
      }
    }
  }
}
```

::::::

::::::{tab-item} Using ELSER on EIS in Cloud

```{applies_to}
stack: ga 9.2
deployment:
  self: unavailable
```

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text",
        "inference_id": ".elser-2-elastic"
      }
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]

::::::

:::::::

### Use a custom {{infer}} endpoint [using-custom-endpoint]

To use a custom {{infer}} endpoint instead of the [default](#default-endpoints) or [preconfigured](#preconfigured-endpoints) endpoints, you
must [Create {{infer}} API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put)
and specify its `inference_id` when setting up the `semantic_text` field type.

```console
PUT my-index-000002
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text",
        "inference_id": "my-openai-endpoint" <1>
      }
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]

1. The `inference_id` of the {{infer}} endpoint to use to generate embeddings.

### Use dedicated endpoints for ingestion and search [dedicated-endpoints-for-ingestion-and-search]

If you use a [custom {{infer}} endpoint](#using-custom-endpoint) through your ML node and not through Elastic {{infer-cap}} Service, the recommended way to use `semantic_text` is by having dedicated {{infer}} endpoints for ingestion and search.

This ensures that search speed remains unaffected by ingestion workloads, and vice versa. After creating dedicated {{infer}} endpoints for both, you can reference them using the `inference_id`
and `search_inference_id` parameters when setting up the index mapping for an index that uses the `semantic_text` field.

```console
PUT my-index-000003
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text",
        "inference_id": "my-elser-endpoint-for-ingest",
        "search_inference_id": "my-elser-endpoint-for-search"
      }
    }
  }
}
```

## Set `index_options` for `sparse_vectors` [index-options-sparse_vectors]

```{applies_to}
stack: ga 9.2
```

Configuring `index_options` for [sparse vector fields](/reference/elasticsearch/mapping-reference/sparse-vector.md) lets you configure [token pruning](/reference/elasticsearch/mapping-reference/sparse-vector.md#token-pruning), which controls whether non-significant or overly frequent tokens are omitted to improve query performance.

The following example enables token pruning and sets pruning thresholds for a `sparse_vector` field:


```console
PUT semantic-embeddings
{
  "mappings": {
    "properties": {
      "content": {
        "type": "semantic_text",
        "index_options": {
          "sparse_vector": {
            "prune": true, <1>
            "pruning_config": {
              "tokens_freq_ratio_threshold": 10, <2>
              "tokens_weight_threshold": 0.5 <3>
            }
          }
        }
      }
    }
  }
}
```
1. (Optional) Enables pruning. Default is `true`.
2. (Optional) Prunes tokens whose frequency is more than 10 times the average token frequency in the field. Default is `5`.
3. (Optional) Prunes tokens whose weight is lower than 0.5. Default is `0.4`.

Learn more about [sparse_vector index options](/reference/elasticsearch/mapping-reference/sparse-vector.md#sparse-vector-index-options) settings and [token pruning](/reference/elasticsearch/mapping-reference/sparse-vector.md#token-pruning).

## Set `index_options` for `dense_vectors` [index-options-dense_vectors]

Configuring `index_options` for [dense vector fields](/reference/elasticsearch/mapping-reference/dense-vector.md) lets you control how dense vectors are indexed for kNN search. You can select the indexing algorithm, such as `int8_hnsw`, `int4_hnsw`, or `disk_bbq`, among [other available index options](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-index-options).

The following example shows how to configure `index_options` for a dense vector field using the `int8_hnsw` indexing algorithm:


```console
PUT semantic-embeddings
{
  "mappings": {
    "properties": {
      "content": {
        "type": "semantic_text",
        "index_options": {
          "dense_vector": {
            "type": "int8_hnsw", <1>
            "m": 15, <2>
            "ef_construction": 90 <3>
          }
        }
      }
    }
  }
}
```
1. (Optional) Selects the `int8_hnsw` vector quantization strategy. Learn about [default quantization types](/reference/elasticsearch/mapping-reference/dense-vector.md#default-quantization-types).
2. (Optional) Sets `m` to 15 to control how many neighbors each node connects to in the HNSW graph. Default is `16`.
3. (Optional) Sets `ef_construction` to 90 to control how many candidate neighbors are considered during graph construction. Default is `100`.

### Default `bfloat16` element type [index-options-dense_vectors-bfloat16]

```{applies_to}
stack: ga 9.4
serverless: ga
```

For {{infer}} endpoints that produce `float` embeddings, `semantic_text` fields automatically use the [`bfloat16`](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-element-type) element type.
This reduces the storage required per vector dimension from 4 to 2 bytes with a negligible impact on search relevance for the vast majority of use cases.

The `bfloat16` format uses a 2-byte floating-point encoding that maintains the same value range as 4-byte floats, but with reduced precision.
You can check if your `semantic_text` field is using `bfloat16` by default by inspecting the default `index_options` for the field using the [get field mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-field-mapping):

```console
GET semantic-embeddings/_mapping/field/content?include_defaults
```

```console-result
{
  "semantic-embeddings": {
    "mappings": {
      "content": {
        "full_name": "content",
        "mapping": {
          "content": {
            "type": "semantic_text",
            "inference_id": "my-float-embedding-endpoint",
            "index_options": {
              "dense_vector": {
                "element_type": "bfloat16" <1>
              }
            }
          }
        }
      }
    }
  }
}
```
1. Indicates that the `semantic_text` field defaulted to the `bfloat16` element type.

#### Override the default element type [index-options-dense_vectors-element-type-override]

If your use case requires full `float` precision, you can override the default `bfloat16` element type by specifying `element_type` in `index_options.dense_vector`:

```console
PUT semantic-embeddings
{
  "mappings": {
    "properties": {
      "content": {
        "type": "semantic_text",
        "inference_id": "my-float-embedding-endpoint",
        "index_options": {
          "dense_vector": {
            "element_type": "float" <1>
          }
        }
      }
    }
  }
}
```
1. Overrides the default `bfloat16` element type to use full-precision `float` (4 bytes per dimension).

You can also combine `element_type` with other index options:

```console
PUT semantic-embeddings
{
  "mappings": {
    "properties": {
      "content": {
        "type": "semantic_text",
        "inference_id": "my-float-embedding-endpoint",
        "index_options": {
          "dense_vector": {
            "element_type": "float",
            "type": "int8_hnsw"
          }
        }
      }
    }
  }
}
```

:::{note}
The `element_type` override is only valid for {{infer}} endpoints that produce `float` embeddings.
For `float` models, valid values are `float` and `bfloat16`.
For other model element types (such as `byte` or `bit`), the `element_type` must match the model's native element type if specified.
:::

