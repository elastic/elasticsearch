---
navigation_title: "Set up and configure"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Set up and configure `semantic_text` fields [set-up-configuration-semantic-text]

This page provides instructions for setting up and configuring `semantic_text` fields. Learn how to configure {{infer}} endpoints, including default and preconfigured options, ELSER on EIS, custom endpoints, and dedicated endpoints for ingestion and search operations.

## Configure {{infer}} endpoints [configure-inference-endpoints]

You can configure {{infer}} endpoints for `semantic_text` fields in the following ways: 

- [Use ELSER on EIS](#using-elser-on-eis)
- [Use default and preconfigured endpoints](#default-and-preconfigured-endpoints)
- [Use a custom {{infer}} endpoint](#using-custom-endpoint)

:::{note}
If you use a [custom {{infer}} endpoint](#using-custom-endpoint) through your ML node and not through Elastic {{infer-cap}} Service (EIS), the recommended method is to [use dedicated endpoints for ingestion and search](#dedicated-endpoints-for-ingestion-and-search). 

{applies_to}`stack: ga 9.1.0` If you use EIS, you don't have to set up dedicated endpoints.
:::

### Use default and preconfigured endpoints [default-and-preconfigured-endpoints]

This section shows you how to set up `semantic_text` with different default and preconfigured endpoints.

:::::::{tab-set}

::::::{tab-item} Default ELSER on EIS on {{serverless-short}}

```{applies_to}
serverless: ga
```

To use the default `.elser-v2-elastic` endpoint that runs on [EIS](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis), you can set up `semantic_text` with the following API request:

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

If you don't specify an {{infer}} endpoint, the `inference_id` field defaults to
`.elser-v2-elastic`, a preconfigured endpoint for the `elasticsearch` service.

::::::

::::::{tab-item} Preconfigured ELSER on EIS in Cloud

```{applies_to}
stack: ga 9.2
deployment:
  self: unavailable
```

To use the preconfigured `.elser-v2-elastic` endpoint that runs on [EIS](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis), you can set up `semantic_text` with the following API request:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text",
        "inference_id": ".elser-v2-elastic"
      }
    }
  }
}
```

If you don't specify an {{infer}} endpoint, the `inference_id` field defaults to
`.elser-2-elasticsearch`, a preconfigured endpoint for the `elasticsearch` service.

::::::

::::::{tab-item} Default ELSER

If you use the default `.elser-2-elasticsearch` endpoint, you can set up `semantic_text` with the following API request:

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

:::::::

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

To use a custom {{infer}} endpoint instead of the default endpoint, you
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
            "ef_construction": 90, <3>
            "confidence_interval": 0.95 <4>
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
4. (Optional) Sets `confidence_interval` to 0.95 to limit the value range used during quantization and balance accuracy with memory efficiency.

