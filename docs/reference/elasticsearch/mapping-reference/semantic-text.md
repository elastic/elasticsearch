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
:::{important}
If you don't specify an `inference_id`, like in the example above, and upgrade to a later version, newly created indices might use a different embedding model than existing ones. Queries that target these indices together can produce unexpected ranking results.
For details, refer to [potential issues when mixing embedding models across indices](./semantic-text-setup-configuration.md#default-endpoint-considerations).
:::

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
            "type": "int8_hnsw"
          }
        }
      }
    }
  }
}
```

1. The `inference_id` of the {{infer}} endpoint used to generate embeddings at index time.
2. The `search_inference_id` of the {{infer}} endpoint used to generate embeddings at query time. If omitted, the endpoint defined by `inference_id` is used at both index and query time.
3. (Optional) Overrides default index options for the underlying vector, for example, `int8_hnsw` for dense vectors. For available configurations, refer to [dense vector index options](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-index-options).

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

### Using ELSER on EIS
```{applies_to}
stack: preview 9.1
serverless: preview
```

If you use the preconfigured `.elser-2-elastic` endpoint that utilizes the ELSER model as a service through the Elastic Inference Service ([ELSER on EIS](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis)), you can
set up `semantic_text` with the following API request:

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

::::{note}
While we do encourage experimentation, we do not recommend implementing production use cases on top of this feature while it is in Technical Preview.

::::

## Parameters for `semantic_text` fields [semantic-text-params]

`inference_id`
:   (Optional, string) {{infer-cap}} endpoint that will be used to generate
embeddings for the field. By default, `.elser-2-elasticsearch` is used. This
parameter cannot be updated. Use
the [Create {{infer}} API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put)
to create the endpoint. If `search_inference_id` is specified, the {{infer}}
endpoint will only be used at index time.

`search_inference_id`
:   (Optional, string) {{infer-cap}} endpoint that will be used to generate
embeddings at query time. You can update this parameter by using
the [Update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
Use
the [Create {{infer}} API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put)
to create the endpoint. If not specified, the {{infer}} endpoint defined by
`inference_id` will be used at both index and query time.

`index_options` {applies_to}`stack: ga 9.1`
:   (Optional, object) Specifies the index options to override default values
for the field. Currently, `dense_vector` and `sparse_vector` index options are supported.
For text embeddings, `index_options` may match any allowed.

* [dense_vector index options](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-index-options).
* [sparse_vector index options](/reference/elasticsearch/mapping-reference/sparse-vector.md#sparse-vectors-params). {applies_to}`stack: ga 9.2`

`chunking_settings` {applies_to}`stack: ga 9.1`
:   (Optional, object) Settings for chunking text into smaller passages.
If specified, these will override the chunking settings set in the {{infer-cap}}
endpoint associated with `inference_id`.
If chunking settings are updated, they will not be applied to existing documents
until they are reindexed.
To completely disable chunking, use the `none` chunking strategy.

    **Valid values for `chunking_settings`**:

    `strategy`
    :   Indicates the strategy of chunking strategy to use. Valid values are `none`, `word`, `recursive` or
    `sentence`. Required.

    `max_chunk_size`
    :   The maximum number of words in a chunk. Required for `word` and `sentence` strategies.

    `overlap`
    :   The number of overlapping words allowed in chunks. This cannot be defined as
    more than half of the `max_chunk_size`. Required for `word` type chunking
    settings.

    `sentence_overlap`
    :   The number of overlapping sentences allowed in chunks. Valid values are `0`
    or `1`. Required for `sentence` type chunking settings

::::{warning}
When using the `none` chunking strategy, if the input exceeds the maximum token
limit of the underlying model, some
services (such as OpenAI) may return an
error. In contrast, the `elastic` and `elasticsearch` services will
automatically truncate the input to fit within the
model's limit.
::::

## {{infer-cap}} endpoint validation [infer-endpoint-validation]

The `inference_id` will not be validated when the mapping is created, but when
documents are ingested into the index. When the first document is indexed, the
`inference_id` will be used to generate underlying indexing structures for the
field.

::::{warning}
Removing an {{infer}} endpoint will cause ingestion of documents and semantic
queries to fail on indices that define `semantic_text` fields with that
{{infer}} endpoint as their `inference_id`. Trying
to [delete an {{infer}} endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-delete)
that is used on a `semantic_text` field will result in an error.
::::

## Text chunking [auto-text-chunking]

{{infer-cap}} endpoints have a limit on the amount of text they can process. To
allow for large amounts of text to be used in semantic search, `semantic_text`
automatically generates smaller passages if needed, called *chunks*.

Each chunk refers to a passage of the text and the corresponding embedding
generated from it. When querying, the individual passages will be automatically
searched for each document, and the most relevant passage will be used to
compute a score.

Chunks are stored as start and end character offsets rather than as separate
text strings. These offsets point to the exact location of each chunk within the
original input text.

For more details on chunking and how to configure chunking settings,
see [Configuring chunking](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-inference)
in the Inference API documentation.

Refer
to [this tutorial](docs-content://solutions/search/semantic-search/semantic-search-semantic-text.md)
to learn more about semantic search using `semantic_text`.

### Pre-chunking [pre-chunking]
```{applies_to}
stack: ga 9.1
```

You can pre-chunk the input by sending it to Elasticsearch as an array of
strings.

For example:

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

1. Disable chunking on `my_semantic_field`.

```console
PUT test-index/_doc/1
{
    "my_semantic_field": ["my first chunk", "my second chunk", ...]    <1>
    ...
}
```

1. The text is pre-chunked and provided as an array of strings.
   Each element in the array represents a single chunk that will be sent
   directly to the inference service without further chunking.

**Important considerations**:

* When providing pre-chunked input, ensure that you set the chunking strategy to
  `none` to avoid additional processing.
* Each chunk should be sized carefully, staying within the token limit of the
  inference service and the underlying model.
* If a chunk exceeds the model's token limit, the behavior depends on the
  service:
    * Some services (such as OpenAI) will return an error.
    * Others (such as `elastic` and `elasticsearch`) will automatically truncate
      the input.

## Retrieving indexed chunks
```{applies_to}
stack: ga 9.2
serverless: ga
```

You can retrieve the individual chunks generated by your semantic field’s chunking
strategy using the [fields parameter](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#search-fields-param):

```console
POST test-index/_search
{
  "query": {
    "ids" : {
      "values" : ["1"]
    }
  },
  "fields": [
    {
      "field": "semantic_text_field",
      "format": "chunks"      <1>
    }
  ]
}
```

1. Use `"format": "chunks"` to return the field’s text as the original text chunks that were indexed.

## Extracting relevant fragments from semantic text [semantic-text-highlighting]

You can extract the most relevant fragments from a semantic text field by using
the [highlight parameter](/reference/elasticsearch/rest-apis/highlighting.md) in
the [Search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search).

```console
POST test-index/_search
{
    "query": {
        "match": {
            "my_semantic_field": "Which country is Paris in?"
        }
    },
    "highlight": {
        "fields": {
            "my_semantic_field": {
                "number_of_fragments": 2,  <1>
                "order": "score"           <2>
            }
        }
    }
}
```

1. Specifies the maximum number of fragments to return.
2. Sorts the most relevant highlighted fragments by score when set to `score`. By default,
   fragments will be output in the order they appear in the field (order: none).

Highlighting is supported on fields other than semantic_text. However, if you
want to restrict highlighting to the semantic highlighter and return no
fragments when the field is not of type semantic_text, you can explicitly
enforce the `semantic` highlighter in the query:

```console
POST test-index/_search
{
    "query": {
        "match": {
            "my_field": "Which country is Paris in?"
        }
    },
    "highlight": {
        "fields": {
            "my_field": {
                "type": "semantic",         <1>
                "number_of_fragments": 2,
                "order": "score"
            }
        }
    }
}
```

1. Ensures that highlighting is applied exclusively to semantic_text fields.

To retrieve all fragments from the `semantic` highlighter in their original indexing order
without scoring, use a `match_all` query as the `highlight_query`.
This ensures fragments are returned in the order they appear in the document:

```console
POST test-index/_search
{
  "query": {
    "ids": {
      "values": ["1"]
    }
  },
  "highlight": {
    "fields": {
      "my_semantic_field": {
        "number_of_fragments": 5,        <1>
        "highlight_query": { "match_all": {} }
      }
    }
  }
}
```

1. Returns the first 5 fragments. Increase this value to retrieve additional fragments.

## Updates and partial updates for `semantic_text` fields [semantic-text-updates]

When updating documents that contain `semantic_text` fields, it’s important to understand how inference is triggered:

* **Full document updates**
  When you perform a full document update, **all `semantic_text` fields will re-run inference** even if their values did not change. This ensures that the embeddings are always consistent with the current document state but can increase ingestion costs.

* **Partial updates using the Bulk API**
  Partial updates that **omit `semantic_text` fields** and are submitted through the [Bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) will **reuse the existing embeddings** stored in the index. In this case, inference is **not triggered** for fields that were not updated, which can significantly reduce processing time and cost.

* **Partial updates using the Update API**
  When using the [Update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) with a `doc` object that **omits `semantic_text` fields**, inference **will still run** on all `semantic_text` fields. This means that even if the field values are not changed, embeddings will be re-generated.

If you want to avoid unnecessary inference and keep existing embeddings:

    * Use **partial updates through the Bulk API**.
    * Omit any `semantic_text` fields that did not change from the `doc` object in your request.

## Customizing `semantic_text` indexing [custom-indexing]

`semantic_text` uses defaults for indexing data based on the {{infer}} endpoint
specified. It enables you to quickstart your semantic search by providing
automatic {{infer}} and a dedicated query so you don’t need to provide further
details.

### Customizing using `semantic_text` parameters [custom-by-parameters]
```{applies_to}
stack: ga 9.1
```

If you want to override those defaults and customize the embeddings that
`semantic_text` indexes, you can do so by
modifying [parameters](#semantic-text-params):

- Use `index_options` to specify alternate index options such as specific
  `dense_vector` quantization methods
- Use `chunking_settings` to override the chunking strategy associated with the
  {{infer}} endpoint, or completely disable chunking using the `none` type

Here is an example of how to set these parameters for a text embedding endpoint:

```console
PUT my-index-000004
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text",
        "inference_id": "my-text-embedding-endpoint", <1>
        "search_inference_id": "my-search-endpoint", <2>
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

1. (Optional) Specifies the [{{infer}} endpoint](/reference/elasticsearch/mapping-reference/semantic-text-reference.md#configuring-inference-endpoints) used to generate embeddings at index time. If you don’t specify an `inference_id`, the `semantic_text` field uses a [default {{infer}} endpoint](/reference/elasticsearch/mapping-reference/semantic-text-setup-configuration.md#default-endpoints).
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


