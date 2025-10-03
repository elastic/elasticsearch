---
navigation_title: "Semantic text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Semantic text field type [semantic-text]

The `semantic_text` field type automatically generates embeddings for text
content using an inference endpoint. Long passages
are [automatically chunked](#auto-text-chunking) to smaller sections to enable
the processing of larger corpuses of text.

The `semantic_text` field type specifies an inference endpoint identifier that
will be used to generate embeddings. You can create the inference endpoint by
using
the [Create {{infer}} API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put).
This field type and the [
`semantic` query](/reference/query-languages/query-dsl/query-dsl-semantic-query.md)
type make it simpler to perform semantic search on your data. The
`semantic_text` field type may also be queried
with [match](/reference/query-languages/query-dsl/query-dsl-match-query.md), [sparse_vector](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md)
or [knn](/reference/query-languages/query-dsl/query-dsl-knn-query.md) queries.

If you don’t specify an inference endpoint, the `inference_id` field defaults to
`.elser-2-elasticsearch`, a preconfigured endpoint for the elasticsearch
service.

Using `semantic_text`, you won’t need to specify how to generate embeddings for
your data, or how to index it. The {{infer}} endpoint automatically determines
the embedding generation, indexing, and query to use.

{applies_to}`stack: ga 9.1`  Newly created indices with `semantic_text` fields using dense embeddings will be
[quantized](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization)
to `bbq_hnsw` automatically as long as they have a minimum of 64 dimensions.

## Default and custom endpoints

You can use either preconfigured endpoints in your `semantic_text` fields which
are ideal for most use cases or create custom endpoints and reference them in
the field mappings.

### Using the default ELSER endpoint

If you use the preconfigured `.elser-2-elasticsearch` endpoint, you can set up
`semantic_text` with the following API request:

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

### Using a custom endpoint

To use a custom {{infer}} endpoint instead of the default
`.elser-2-elasticsearch`, you
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

1. The `inference_id` of the {{infer}} endpoint to use to generate embeddings.

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
    :   Indicates the strategy of chunking strategy to use. Valid values are `none`, `word` or
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

## Returning semantic field embeddings in `_source`

```{applies_to}
stack: ga 9.2
serverless: ga
```

By default, the embeddings generated for `semantic_text` fields are stored internally and **not included in `_source`** when retrieving documents.

To include the full inference fields, including their embeddings, in `_source`, set the `_source.exclude_vectors` option to `false`.
This works with the
[Get](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get),
[Search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search),
and
[Reindex](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex)
APIs.

```console
POST my-index/_search
{
  "_source": {
    "exclude_vectors": false
  },
  "query": {
    "match_all": {}
  }
}
```

The embeddings will appear under `_inference_fields` in `_source`.

**Use cases**
Including embeddings in `_source` is useful when you want to:

* Reindex documents into another index **with the same `inference_id`** without re-running inference.
* Export or migrate documents while preserving their embeddings.
* Inspect or debug the raw embeddings generated for your content.

### Example: Reindex while preserving embeddings

```console
POST _reindex
{
  "source": {
    "index": "my-index-src",
    "_source": {
      "exclude_vectors": false            <1>
    }
  },
  "dest": {
    "index": "my-index-dest"
  }
}
```

1. Sends the source documents with their stored embeddings to the destination index.

::::{warning}
If the target index’s `semantic_text` field does **not** use the **same `inference_id`** as the source index,
the documents will **fail the reindex task**.
Matching `inference_id` values are required to reuse the existing embeddings.
::::

This allows documents to be re-indexed without triggering inference again, **as long as the target `semantic_text` field uses the same `inference_id` as the source**.

::::{note}
**For versions prior to 9.2.0**

Older versions do not support the `exclude_vectors` option to retrieve the embeddings of the semantic text fields.
To return the `_inference_fields`, use the `fields` option in a search request instead:

```console
POST test-index/_search
{
  "query": {
    "match": {
      "my_semantic_field": "Which country is Paris in?"
    }
  },
  "fields": [
    "_inference_fields"
  ]
}
```

This returns the chunked embeddings used for semantic search under `_inference_fields` in `_source`.
Note that the `fields` option is **not** available for the Reindex API.
::::

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
        "inference_id": "my-text-embedding-endpoint",
        "index_options": {
          "dense_vector": {
            "type": "int4_flat"
          }
        },
        "chunking_settings": {
          "type": "none"
        }
      }
    }
  }
}
```

## Updates to `semantic_text` fields [update-script]

For indices containing `semantic_text` fields, updates that use scripts have the
following behavior:

* Are supported through
  the [Update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update).
* Are not supported through
  the [Bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk-1)
  and will fail. Even if the script targets non-`semantic_text` fields, the
  update will fail when the index contains a `semantic_text` field.

## `copy_to` and multi-fields support [copy-to-support]

The semantic_text field type can serve as the target
of [copy_to fields](/reference/elasticsearch/mapping-reference/copy-to.md), be
part of
a [multi-field](/reference/elasticsearch/mapping-reference/multi-fields.md)
structure, or
contain [multi-fields](/reference/elasticsearch/mapping-reference/multi-fields.md)
internally. This means you can use a single field to collect the values of other
fields for semantic search.

For example, the following mapping:

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

can also be declared as multi-fields:

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

## Troubleshooting semantic_text fields [troubleshooting-semantic-text-fields]

If you want to verify that your embeddings look correct, you can view the
inference data that `semantic_text` typically hides using `fields`.

```console
POST test-index/_search
{
  "query": {
    "match": {
      "my_semantic_field": "Which country is Paris in?"
    }
  },
  "fields": [
    "_inference_fields"
  ]
}
```

This will return verbose chunked embeddings content that is used to perform
semantic search for `semantic_text` fields.

## Limitations [limitations]

`semantic_text` field types have the following limitations:

* `semantic_text` fields are not currently supported as elements
  of [nested fields](/reference/elasticsearch/mapping-reference/nested.md).
* `semantic_text` fields can’t currently be set as part
  of [dynamic templates](docs-content://manage-data/data-store/mapping/dynamic-templates.md).
* `semantic_text` fields are not supported with Cross-Cluster Search (CCS) or
  Cross-Cluster Replication (CCR).
