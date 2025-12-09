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
  "mappings": { 
    "properties": {
      "content": { 
        "type": "semantic_text" <1>
      }
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]

1. In this example, the `semantic_text` field uses a [default {{infer}} endpoint](./semantic-text-setup-configuration.md#default-and-preconfigured-endpoints) because the `inference_id` parameter isn't specified.

:::{tip}
For a complete example, refer to the [Semantic search with `semantic_text`](docs-content://solutions/search/semantic-search/semantic-search-semantic-text.md) tutorial.
::: 

## Overview

The `semantic_text` field type documentation is organized into reference content and how-to guides. 

### Reference

The [Reference](./semantic-text-reference.md) page provides technical reference content, including [parameter descriptions](./semantic-text-reference.md#semantic-text-params) for `semantic_text` fields, an overview of [{{infer}} endpoints](./semantic-text-reference.md#configuring-inference-endpoints) used with `semantic_text` fields, how [chunking](./semantic-text-reference.md#chunking-behavior) automatically processes long text passages, [supported query types](./semantic-text-reference.md#querying-semantic-text-fields), [limitations](./semantic-text-reference.md#limitations), and [document count discrepancies](./semantic-text-reference.md#document-count-discrepancy) in `_cat/indices`.

### How-to guides

The [How-to guides](./semantic-text-how-tos.md) page organizes procedure descriptions and examples into the following guides:

- [Setup and configuration](./semantic-text-setup-configuration.md): Learn how to configure {{infer}} endpoints, including default and preconfigured options, ELSER on EIS, custom endpoints, and dedicated endpoints for ingestion and search operations.

- [Ingestion](./semantic-text-ingestions.md): Learn how to index pre-chunked content, use `copy_to` and multi-fields to collect values from multiple fields, and perform updates and partial updates to optimize ingestion costs.

- [Search and retrieval](./semantic-text-search-retrieval.md): Learn how to query `semantic_text` fields, retrieve indexed chunks, return field embeddings, and highlight the most relevant fragments from search results.


