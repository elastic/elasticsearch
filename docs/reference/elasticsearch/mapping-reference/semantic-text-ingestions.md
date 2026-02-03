---
navigation_title: "Ingest data"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Ingest data with `semantic_text` fields [set-up-configuration-semantic-text]

This page provides instructions for ingesting data into `semantic_text` fields. Learn how to index pre-chunked content, use `copy_to` and multi-fields to collect values from multiple fields, and perform updates and partial updates to optimize ingestion costs.

## Index pre-chunked content [pre-chunking]
```{applies_to}
stack: ga 9.1
```

To index pre-chunked content, provide your text as an array of strings. Each element in the array represents a single chunk that will be sent directly to the {{infer}} service without further chunking.

:::::{stepper}

::::{step} Disable automatic chunking

Disable automatic chunking in your index mapping by setting `chunking_settings.strategy` to `none`:

```console
PUT test-index
{
  "mappings": {
    "properties": {
      "my_semantic_field": {
        "type": "semantic_text",
        "chunking_settings": {
          "strategy": "none"    <1>
        }
      }
    }
  }
}
```

1. Disables automatic chunking on `my_semantic_field`.

::::

::::{step} Index documents

Index documents with pre-chunked text as an array:

```console
PUT test-index/_doc/1
{
    "my_semantic_field": ["my first chunk", "my second chunk", ...]    <1>
    ...
}
```

1. The text is pre-chunked and provided as an array of strings. Each element represents a single chunk.

::::

:::::

:::{important}
When providing pre-chunked input:

- Ensure that you set the chunking strategy to `none` to avoid additional processing.
- Size each chunk carefully, staying within the token limit of the {{infer}} service and the underlying model.
- If a chunk exceeds the model's token limit, the behavior depends on the service:
  - Some services (such as OpenAI) will return an error.
  - Others (such as `elastic` and `elasticsearch`) will automatically truncate the input.
:::

## Use `copy_to` and multi-fields with `semantic_text` [use-copy-to-with-semantic-text]

You can use a single `semantic_text` field to collect values from multiple fields for semantic search. The `semantic_text` field type can serve as the target of [copy_to fields](/reference/elasticsearch/mapping-reference/copy-to.md), be part of a [multi-field](/reference/elasticsearch/mapping-reference/multi-fields.md) structure, or contain [multi-fields](/reference/elasticsearch/mapping-reference/multi-fields.md) internally.

### Use copy_to

Use `copy_to` to copy values from source fields to a `semantic_text` field:

```console
PUT test-index
{
    "mappings": {
        "properties": {
            "source_field": {
                "type": "text",
                "copy_to": "infer_field"
            },
            "infer_field": {
                "type": "semantic_text",
                "inference_id": ".elser-2-elasticsearch"
            }
        }
    }
}
```
% TEST[skip:Requires {{infer}} endpoint]

### Use multi-fields

Declare `semantic_text` as a multi-field:

```console
PUT test-index
{
    "mappings": {
        "properties": {
            "source_field": {
                "type": "text",
                "fields": {
                    "infer_field": {
                        "type": "semantic_text",
                        "inference_id": ".elser-2-elasticsearch"
                    }
                }
            }
        }
    }
}
```
% TEST[skip:Requires {{infer}} endpoint]

## Updates and partial updates [updates-and-partial-updates]

When updating documents that contain `semantic_text` fields, it's important to understand how {{infer}} is triggered:

Full document updates
:   Full document updates re-run {{infer}} on all `semantic_text` fields, even if their values did not change. This ensures that embeddings remain consistent with the current document state but can increase ingestion costs.

Partial updates using the Bulk API
:   Partial updates submitted through the [Bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) reuse existing embeddings when you omit `semantic_text` fields. {{infer}} does not run for omitted fields, which can significantly reduce processing time and cost.

Partial updates using the Update API
:   Partial updates submitted through the [Update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) re-run {{infer}} on all `semantic_text` fields, even when you omit them from the `doc` object. Embeddings are re-generated regardless of whether field values changed.

To preserve existing embeddings and avoid unnecessary {{infer}} costs:

 * Use partial updates with the Bulk API.
 * Omit any `semantic_text` fields that did not change from the `doc` object in your request.

### Scripted updates [scripted-updates]

For indices containing `semantic_text` fields, updates that use scripts have the
following behavior:

- ✅ **Supported:** [Update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update)
- ❌ **Not supported:** [Bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk-1). Scripted updates will fail even if the script targets non-`semantic_text` fields.