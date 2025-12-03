---
navigation_title: "Semantic text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Semantic text field type [semantic-text]

The `semantic_text` field type simplifies [semantic search](docs-content://solutions/search/semantic-search.md) by providing sensible defaults that automate most of the manual work typically required for vector search. Using `semantic_text`, you don't have to manually configure mappings, set up ingestion pipelines, or handle chunking. The field type automatically:

- Configures index mappings: Chooses the correct field type (`sparse_vector` or `dense_vector`), dimensions, similarity functions, and storage optimizations based on the {{infer}} endpoint.
- Generates embeddings during indexing: Automatically generates embeddings when you index documents, without requiring ingestion pipelines or {{infer}} processors.
- Handles chunking: Automatically chunks long text documents during indexing.

## Basic `semantic_text` mapping example

The following example creates an index mapping with a `semantic_text` field:

```console
PUT semantic-embeddings 
{
  "mappings": { <1>
    "properties": {
      "content": { 
        "type": "semantic_text" 
      }
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]

1. In this example, the `semantic_text` field uses a [default {{infer}} endpoint](./semantic-text-how-tos.md#default-and-preconfigured-endpoints) because the `inference_id` parameter isn't specified.

:::{tip}
For a complete example, refer to the [Semantic search with `semantic_text`](docs-content://solutions/search/semantic-search/semantic-search-semantic-text.md) tutorial.
::: 

## Overview

The `semantic_text` field type documentation is organized into reference content and how-to guides. 

### Reference

The [Reference](./semantic-text-reference.md) page provides technical reference content, for instance:

- [Parameters](./semantic-text-reference.md#semantic-text-params): Parameter descriptions for `semantic_text` fields.

- [{{infer-cap}} endpoints](./semantic-text-reference.md#configuring-inference-endpoints): Overview of {{infer}} endpoints used with `semantic_text` fields.

- [Chunking](./semantic-text-reference.md#chunking-behavior): Overview of how `semantic_text` automatically processes long text passages by generating smaller chunks.

- [Querying](./semantic-text-reference.md#querying-semantic-text-fields): Supported query types for `semantic_text` fields.

### How-to guides

The [How-to guides](./semantic-text-how-tos.md) page contains procedure descriptions and examples for common tasks, for instance:

- [Configuring {{infer}} endpoints](./semantic-text-how-tos.md#configure-inference-endpoints): Learn how to use [default and preconfigured endpoints](./semantic-text-how-tos.md#default-and-preconfigured-endpoints), [ELSER on EIS](./semantic-text-how-tos.md#using-elser-on-eis), and [custom {{infer}} endpoints](./semantic-text-how-tos.md#using-custom-endpoint).

- [Setting up dedicated endpoints](./semantic-text-how-tos.md#dedicated-endpoints-for-ingestion-and-search): Learn how to configure separate {{infer}} endpoints for ingestion and search operations.

- [Pre-chunking content](./semantic-text-how-tos.md#pre-chunking): Learn how to provide pre-chunked text as arrays and [retrieve indexed chunks](./semantic-text-how-tos.md#retrieving-indexed-chunks).

- [Returning embeddings](./semantic-text-how-tos.md#returning-semantic-field-embeddings): Learn how to return `semantic_text` field embeddings in [`_source`](./semantic-text-how-tos.md#returning-semantic-field-embeddings-in-_source) (for {{es}} 9.2+) or using the [`fields` parameter](./semantic-text-how-tos.md#returning-semantic-field-embeddings-using-fields) (for earlier versions).

- [Highlighting fragments](./semantic-text-how-tos.md#highlighting-fragments): Learn how to extract the most relevant text fragments from `semantic_text` fields.


