---
navigation_title: "Setup and configuration"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Setup and configuration guides for the `semantic_text` field type [set-up-configuration-semantic-text]

This page provides instructions for setting up and configuring `semantic_text` fields. Learn how to configure {{infer}} endpoints, including default and preconfigured options, ELSER on EIS, custom endpoints, and dedicated endpoints for ingestion and search operations.

## Configure {{infer}} endpoints [configure-inference-endpoints]

You can configure {{infer}} endpoints for `semantic_text` fields in the following ways: 

- [Use ELSER on EIS](#using-elser-on-eis)
- [Use default and preconfigured endpoints](#default-and-preconfigured-endpoints)
- [Use a custom {{infer}} endpoint](#using-custom-endpoint)

The recommended method is to [use dedicated endpoints for ingestion and search](#dedicated-endpoints-for-ingestion-and-search).

### Use default and preconfigured endpoints [default-and-preconfigured-endpoints]

:::::::{tab-set}

::::::{tab-item} Default ELSER on EIS endpoint on {{serverless-short}}

```{applies_to}
serverless: ga
```

If you want to use the default `.elser-v2-elastic` endpoint that runs on [EIS](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis), you can set up `semantic_text` with the following API request:

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

::::::{tab-item} Preconfigured ELSER on EIS endpoint in Cloud

```{applies_to}
stack: ga 9.2
deployment:
  self: unavailable
```

If you want to use the preconfigured `.elser-v2-elastic` endpoint that runs on [EIS](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis), you can set up `semantic_text` with the following API request:

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

::::::{tab-item} Default ELSER endpoint

If you use the preconfigured `.elser-2-elasticsearch` endpoint, you can set up `semantic_text` with the following API request:

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
stack: preview 9.1, ga 9.2
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

The recommended way to use `semantic_text` is by having dedicated {{infer}}
endpoints for ingestion and search. This ensures that search speed remains
unaffected by ingestion workloads, and vice versa. After creating dedicated
{{infer}} endpoints for both, you can reference them using the `inference_id`
and `search_inference_id` parameters when setting up the index mapping for an
index that uses the `semantic_text` field.

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
% TEST[skip:Requires {{infer}} endpoint]