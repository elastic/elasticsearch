---
navigation_title: "Semantic text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Semantic text field type [semantic-text]

:::::{warning}
The `semantic_text` field mapping can be added regardless of license state. However, it typically calls the [{{infer-cap}} API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-inference), which requires an [appropriate license](https://www.elastic.co/subscriptions). In these cases, using `semantic_text` in a cluster without the appropriate license causes operations such as indexing and reindexing to fail.
:::::

The `semantic_text` field type simplifies [semantic search](docs-content://solutions/search/semantic-search.md) by providing sensible defaults that automate most of the manual work typically required for vector search. Using `semantic_text`, you don't have to manually configure mappings, set up ingestion pipelines, or handle chunking. The field type automatically:

- Configures index mappings: Chooses the correct field type (`sparse_vector` or `dense_vector`), dimensions, similarity functions, and storage optimizations based on the {{infer}} endpoint.
- Generates embeddings during indexing: Automatically generates embeddings when you index documents, without requiring ingestion pipelines or {{infer}} processors.
- Handles chunking: Automatically chunks long text documents during indexing.

## Basic `semantic_text` mapping example

The following example creates an index mapping with a `semantic_text` field, using default values:

```console
PUT semantic-embeddings 
{
  "mappings": { 
    "properties": {
      "content": { 
        "type": "semantic_text"
      }
    }
  }
}
```

## Extended `semantic_text` mapping example

The following example creates an index mapping with a `semantic_text` field that uses dense vectors:

```console
PUT semantic-embeddings
{
  "mappings": {
    "properties": {
      "content": {
        "type": "semantic_text",
        "inference_id": "my-inference-endpoint", <1>
        "search_inference_id": "my-search-inference-endpoint", <2>
        "index_options": { <3>
          "dense_vector": {
            "type": "bbq_disk"
          }
        },
        "chunking_settings": { <4>
          "strategy": "word",
          "max_chunk_size": 120,
          "overlap": 40
        }
      }
    }
  }
}
```

1. (Optional) Specifies the [{{infer}} endpoint](/reference/elasticsearch/mapping-reference/semantic-text-reference.md#configuring-inference-endpoints) used to generate embeddings at index time. If you don’t specify an `inference_id`, the `semantic_text` field uses a [default {{infer}} endpoint](/reference/elasticsearch/mapping-reference/semantic-text-setup-configuration.md#default-and-preconfigured-endpoints).
2. (Optional) The {{infer}} endpoint used to generate embeddings at query time. If not specified, the endpoint defined by `inference_id` is used at both index and query time.
3. (Optional) Configures how the underlying vector representation is indexed. In this example, [`bbq_disk`](/reference/elasticsearch/mapping-reference/bbq.md#bbq-disk) is selected for dense vectors. You can configure different index options depending on whether the field uses dense or sparse vectors. Learn how to [set `index_options` for `sparse_vectors`](/reference/elasticsearch/mapping-reference/semantic-text-setup-configuration.md#index-options-sparse_vectors) and how to [set `index_options` for `dense_vectors`](/reference/elasticsearch/mapping-reference/semantic-text-setup-configuration.md#index-options-dense_vectors).
4. (Optional) Overrides the [chunking settings](/reference/elasticsearch/mapping-reference/semantic-text-reference.md#chunking-behavior) from the {{infer}} endpoint. In this example, the `word` strategy splits text on individual words with a maximum of 120 words per chunk and an overlap of 40 words between chunks. The default chunking strategy is `sentence`.

:::{tip}
For a complete example, refer to the [Semantic search with `semantic_text`](docs-content://solutions/search/semantic-search/semantic-search-semantic-text.md) tutorial.
::: 

## Overview

The `semantic_text` field type documentation is organized into reference content and how-to guides. 

### Reference

The [Reference](./semantic-text-reference.md) section provides technical reference content:

- [Parameters](./semantic-text-reference.md#semantic-text-params): Parameter descriptions for `semantic_text` fields.
- [{{infer-cap}} endpoints](./semantic-text-reference.md#configuring-inference-endpoints): Overview of {{infer}} endpoints used with `semantic_text` fields.
- [Chunking](./semantic-text-reference.md#chunking-behavior): How `semantic_text` automatically processes long text passages by generating smaller chunks.
- [Pre-filtering for dense vector queries](./semantic-text-reference.md#pre-filtering-for-dense-vector-queries): Automatic pre-filtering behavior for dense vector queries on `semantic_text` fields.
- [Limitations](./semantic-text-reference.md#limitations): Current limitations of `semantic_text` fields.
- [Document count discrepancy](./semantic-text-reference.md#document-count-discrepancy): Understanding document counts in `_cat/indices` for indices with `semantic_text` fields.
- [Querying `semantic_text` fields](./semantic-text-search-retrieval.md#querying-semantic-text-fields): Supported query types for `semantic_text` fields.

### How-to guides

The [How-to guides](./semantic-text-how-tos.md) section organizes procedure descriptions and examples into the following guides:

- [Set up and configure `semantic_text` fields](./semantic-text-setup-configuration.md): Learn how to configure {{infer}} endpoints, including default and preconfigured options, ELSER on EIS, custom endpoints, and dedicated endpoints for ingestion and search operations.

- [Ingest data with `semantic_text` fields](./semantic-text-ingestions.md): Learn how to index pre-chunked content, use `copy_to` and multi-fields to collect values from multiple fields, and perform updates and partial updates to optimize ingestion costs.

- [Search and retrieve `semantic_text` fields](./semantic-text-search-retrieval.md): Learn how to query `semantic_text` fields, retrieve indexed chunks, return field embeddings, and highlight the most relevant fragments from search results.


