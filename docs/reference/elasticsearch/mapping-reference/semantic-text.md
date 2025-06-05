
navigation_title: "Semantic text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
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
Newly created indices with `semantic_text` fields using dense embeddings will be
[quantized](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization)
to `bbq_hnsw` automatically.

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

`chunking_settings`
:   (Optional, object) Settings for chunking text into smaller passages.
If specified, these will override the chunking settings set in the {{infer-cap}}
endpoint associated with `inference_id`.
If chunking settings are updated, they will not be applied to existing documents
until they are reindexed.

    **Valid values for `chunking_settings`**:

    `type`
    :   Indicates the type of chunking strategy to use. Valid values are `word` or
    `sentence`. Required.

    `max_chunk_size`
    :   The maximum number of works in a chunk. Required.

    `overlap`
    :   The number of overlapping words allowed in chunks. This cannot be defined as
    more than half of the `max_chunk_size`. Required for `word` type chunking
    settings.

    `sentence_overlap`
    :   The number of overlapping sentences allowed in chunks. Valid values are `0`
    or `1`. Required for `sentence` type chunking settings

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

For more details on chunking and how to configure chunking settings,
see [Configuring chunking](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-inference)
in the Inference API documentation.

Refer
to [this tutorial](docs-content://solutions/search/semantic-search/semantic-search-semantic-text.md)
to learn more about semantic search using `semantic_text` and the `semantic`
query.

## Extracting Relevant Fragments from Semantic Text [semantic-text-highlighting]

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
2. Sorts highlighted fragments by score when set to `score`. By default,
   fragments will be output in the order they appear in the field (order: none).

Highlighting is supported on fields other than semantic_text. However, if you
want to restrict highlighting to the semantic highlighter and return no
fragments when the field is not of type semantic_text, you can explicitly
enforce the `semantic` highlighter in the query:

```console
PUT test-index
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

## Customizing `semantic_text` indexing [custom-indexing]

`semantic_text` uses defaults for indexing data based on the {{infer}} endpoint
specified. It enables you to quickstart your semantic search by providing
automatic {{infer}} and a dedicated query so you don’t need to provide further
details.

In case you want to customize data indexing, use the [
`sparse_vector`](/reference/elasticsearch/mapping-reference/sparse-vector.md)
or [`dense_vector`](/reference/elasticsearch/mapping-reference/dense-vector.md)
field types and create an ingest pipeline with
an [{{infer}} processor](/reference/enrich-processor/inference-processor.md) to
generate the
embeddings. [This tutorial](docs-content://solutions/search/semantic-search/semantic-search-inference.md)
walks you through the process. In these cases - when you use `sparse_vector` or
`dense_vector` field types instead of the `semantic_text` field type to
customize indexing - using the [
`semantic_query`](/reference/query-languages/query-dsl/query-dsl-semantic-query.md)
is not supported for querying the field data.

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

## Limitations [limitations]

`semantic_text` field types have the following limitations:

* `semantic_text` fields are not currently supported as elements
  of [nested fields](/reference/elasticsearch/mapping-reference/nested.md).
* `semantic_text` fields can’t currently be set as part
  of [Dynamic templates](docs-content://manage-data/data-store/mapping/dynamic-templates.md).
* `semantic_text` fields are not supported with Cross-Cluster Search (CCS) or
  Cross-Cluster Replication (CCR).
