---
navigation_title: "Semantic"
applies_to:
  stack: preview 9.5
  serverless: preview
---

# Semantic field type [semantic-field]

:::::{warning}
The `semantic` field mapping can be added regardless of license state. However, it calls the [{{infer-cap}} API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-inference), which requires an [appropriate license](https://www.elastic.co/subscriptions). Using a `semantic` field without the appropriate license causes operations such as indexing and reindexing to fail.
:::::

The `semantic` field type simplifies semantic and multimodal search across text, images, audio, video, and PDF files. With a compatible multimodal embedding model, you can search from any supported input type to any other supported input type. The field automatically:

- Generates embeddings when you index field values, without an ingest pipeline or {{infer}} processor.
- Splits long text into smaller passages, called chunks.
- Indexes the generated embeddings using default index options that optimize for common use cases.
- Searches the embeddings generated for each value or text chunk.

For multimodal search, Elastic recommends [Jina multimodal embeddings](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-jina.md#jina-multimodal-embeddings).

Elasticsearch refers to `semantic` and `semantic_text` as *inference fields*: mapped fields that use {{infer}} endpoints and store generated embeddings in internal subfields.

Multiple `semantic` fields can use the same {{infer}} endpoint. For example, an index can use one field for image embeddings and another for description embeddings, then search either field or both.

:::{tip}
The `semantic` field type shares many capabilities with `semantic_text`, but `semantic_text` accepts text only. If you're working exclusively with text, consider using [`semantic_text`](./semantic-text.md).

For a comparison table, refer to [Should I use `semantic_text` or `semantic`?](#should-i-use-semantictext-or-semantic).
:::

:::{include} _snippets/semantic-field-type-comparison.md
:::

## Basic `semantic` mapping example

The following example creates an index mapping with a `semantic` field using `.jina-embeddings-v5-omni-small`, the preconfigured {{infer}} endpoint for the Jina Embeddings v5 Omni Small model:

```console
PUT semantic-embeddings
{
  "mappings": {
    "properties": {
      "content": {
        "type": "semantic",
        "inference_id": ".jina-embeddings-v5-omni-small"
      }
    }
  }
}
```
% TEST[skip:Requires access to the preconfigured EIS endpoint]

Unlike [`semantic_text`](./semantic-text.md), a `semantic` field has no default {{infer}} endpoint. You must use an endpoint that uses the `embedding` task type and specify its ID in the field mapping. The endpoint determines which input modalities the field supports.

## Extended `semantic` mapping example

The following example customizes the search endpoint, text chunking, and dense-vector index options:

```console
PUT my-semantic-index
{
  "mappings": {
    "properties": {
      "content": {
        "type": "semantic",
        "inference_id": "my-index-embedding-endpoint", <1>
        "search_inference_id": "my-search-embedding-endpoint", <2>
        "chunking_settings": { <3>
          "strategy": "word",
          "max_chunk_size": 250,
          "overlap": 50
        },
        "index_options": { <4>
          "dense_vector": {
            "type": "int8_hnsw"
          }
        }
      }
    }
  }
}
```
% TEST[skip:Requires embedding {{infer}} endpoints]

1. Endpoint used to generate embeddings while indexing.
2. Compatible endpoint used to generate embeddings while querying.
3. Splits text into chunks of at most 250 words, with an overlap of 50 words.
4. Indexes the embeddings using `int8_hnsw` quantization.

## Quickstart [semantic-quickstart-overview]

Follow the [multimodal search tutorial](docs-content://solutions/search/multimodal-search/multimodal-search-tutorial.md) to index a small collection of images into {{es}} and search those images using text, other images, and PDFs.

## Reference documentation [semantic-reference]

Refer to the [`semantic` field reference](./semantic-field-reference.md) for the complete technical details, including:

- [Parameters for `semantic` fields](./semantic-field-reference.md#semantic-params)
- [{{infer-cap}} endpoint requirements](./semantic-field-reference.md#semantic-inference-endpoint)
- [Supported input types](./semantic-field-reference.md#semantic-input)
- [Limitations](./semantic-field-reference.md#semantic-limitations)

<!-- TODO: When published, link to the semantic field release blog here. -->
