---
navigation_title: "Semantic text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Semantic text field type [semantic-text]

The `semantic_text` field type automatically generates embeddings for text content using an [inference endpoint](#configuring-inference-endpoints). 
Using `semantic_text`, you don't need to specify how to generate embeddings for your data, or how to index it. The inference endpoint automatically determines the embedding generation, indexing, and query to use. 

Long passages are automatically [chunked](#chunking-behavior) to smaller sections to enable the processing of larger corpuses of text.

{applies_to}`stack: ga 9.1`  Newly created indices with `semantic_text` fields using dense embeddings will be
[quantized](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization)
to `bbq_hnsw` automatically as long as they have a minimum of 64 dimensions.

## Inference endpoints [configuring-inference-endpoints]

The `semantic_text` field type specifies an inference endpoint identifier (`inference_id`) that is used to generate embeddings.

You can use [default and preconfigured endpoints](#default-and-preconfigured-endpoints) without creating an inference endpoint manually. To use [custom endpoints](#using-custom-endpoint), you must create an inference endpoint using the [Create inference API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put). 

The recommended method is to configure [dedicated endpoints for ingestion and search](#dedicated-endpoints-for-ingestion-and-search) using the `inference_id` and `search_inference_id` parameters.

::::{warning}
Removing an {{infer}} endpoint will cause ingestion of documents and semantic
queries to fail on indices that define `semantic_text` fields with that
{{infer}} endpoint as their `inference_id`. Trying
to [delete an {{infer}} endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-delete)
that is used on a `semantic_text` field will result in an error.
::::

### Default and preconfigured endpoints [default-and-preconfigured-endpoints]

:::::::{tab-set}

::::::{tab-item} Default ELSER on EIS endpoint on {{serverless-short}}

```{applies_to}
serverless: ga
```

If you don't specify an {{infer}} endpoint, the `inference_id` field defaults to
`.elser-v2-elastic`, a preconfigured endpoint for the `elasticsearch` service.
This endpoint uses the [Elastic {{infer-cap}} Service (EIS)](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis).

If you want to use the default `.elser-v2-elastic` endpoint that runs on EIS, you can set up `semantic_text` with the following API request:

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
% TEST[skip:Requires inference endpoint]

::::::

::::::{tab-item} Preconfigured ELSER on EIS endpoint in Cloud

```{applies_to}
stack: ga 9.2
deployment:
  self: unavailable
```

If you don't specify an {{infer}} endpoint, the `inference_id` field defaults to
`.elser-2-elasticsearch`, a preconfigured endpoint for the `elasticsearch` service.
This endpoint uses the [Elastic {{infer-cap}} Service (EIS)](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis).

If you want to use the preconfigured `.elser-v2-elastic` endpoint that runs on EIS, you can set up `semantic_text` with the following API request:

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

### Using ELSER on EIS [using-elser-on-eis]

```{applies_to}
stack: preview 9.1, ga 9.2
deployment:
  self: unavailable
serverless: ga
```

If you use the preconfigured `.elser-2-elastic` endpoint that utilizes the ELSER model as a service through the Elastic Inference Service ([ELSER on EIS](docs-content://explore-analyze/elastic-inference/eis.md#elser-on-eis)), you can
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
% TEST[skip:Requires inference endpoint]

::::::

:::::::

### Using a custom endpoint [using-custom-endpoint]

To use a custom {{infer}} endpoint instead of the default, you
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
% TEST[skip:Requires inference endpoint]

1. The `inference_id` of the {{infer}} endpoint to use to generate embeddings.

### Dedicated endpoints for ingestion and search [dedicated-endpoints-for-ingestion-and-search]

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
% TEST[skip:Requires inference endpoint]

### {{infer-cap}} endpoint validation [infer-endpoint-validation]

The `inference_id` will not be validated when the mapping is created, but when
documents are ingested into the index. When the first document is indexed, the
`inference_id` will be used to generate underlying indexing structures for the
field.

## Parameters [semantic-text-params]

The `semantic_text` field type uses default indexing settings based on the [{{infer}} endpoint](#configuring-inference-endpoints) specified, enabling you to get started without providing additional configuration details. You can override these defaults by customizing the parameters described below.

`inference_id`
:   (Optional, string) {{infer-cap}} endpoint that will be used to generate
embeddings for the field. If `search_inference_id` is specified, the {{infer}}
endpoint will only be used at index time. Learn more about [configuring this parameter](#configuring-inference-endpoints).

**Updating the `inference_id` parameter**

::::{applies-switch}

:::{applies-item} { "stack": "ga 9.0" }
This parameter cannot be updated.
:::

:::{applies-item} { "stack": "ga 9.3" }

You can update this parameter by using
the [Update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
You can update the inference endpoint if no values have been indexed or if the new endpoint is compatible with the current one.

::::{warning}
When updating an `inference_id` it is important to ensure the new {{infer}} endpoint produces embeddings compatible with those already indexed. This typically means using the same underlying model.
::::

:::

::::

`search_inference_id`
:   (Optional, string) The {{infer}} endpoint that will be used to generate
embeddings at query time. Use the [Create {{infer}} API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) to create the endpoint. If not specified, the {{infer}} endpoint defined by
`inference_id` will be used at both index and query time. You can update this parameter by using
the [Update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).

`index_options` {applies_to}`stack: ga 9.1`
:   (Optional, object) Specifies the index options to override default values
for the field. Currently, `dense_vector` and `sparse_vector` index options are supported. For text embeddings, `index_options` may match any allowed.

- [dense_vector index options](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-index-options)

- [sparse_vector index options](/reference/elasticsearch/mapping-reference/sparse-vector.md#sparse-vectors-params) {applies_to}`stack: ga 9.2`

`chunking_settings` {applies_to}`stack: ga 9.1`
:   (Optional, object) Settings for chunking text into smaller passages.
If specified, these will override the chunking settings set in the {{infer-cap}}
endpoint associated with `inference_id`.

    If chunking settings are updated, they will not be applied to existing documents
until they are reindexed.

    To completely disable chunking, use the `none` chunking strategy. Defaults to the optimal chunking settings for [Elastic Rerank](docs-content:///explore-analyze/machine-learning/nlp/ml-nlp-rerank.md).

     Refer to the [Inference API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put#operation-inference-put-body-application-json-chunking_settings) for values for `chunking_settings` and to [Configuring chunking](https://www.elastic.co/docs/explore-analyze/elastic-inference/inference-api#infer-chunking-config) to learn about different chunking strategies.

    ::::{warning}
    When using the `none` chunking strategy, if the input exceeds the maximum token limit of the underlying model,
    some services (such as OpenAI) may return an error. In contrast, the `elastic` and `elasticsearch` services will
    automatically truncate the input to fit within the model's limit.
     ::::

### Example: Overriding default parameters

The following example shows how to configure `inference_id`, `index_options` and `chunking_settings` for a `semantic_text` field type:

```console
PUT my-index-000004
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text",
        "inference_id": "my-text-embedding-endpoint", <1>
        "index_options": { <2>
          "dense_vector": {
            "type": "int4_flat"
          }
        },
        "chunking_settings": { <3>
          "type": "none"
        }
      }
    }
  }
}
```
% TEST[skip:Requires inference endpoint]

1. The `inference_id` of the {{infer}} endpoint to use for generating embeddings.
2. Overrides default index options by specifying `int4_flat` quantization for dense vector embeddings.
3. Disables automatic chunking by setting the chunking strategy to `none`.

## Chunking [chunking-behavior]

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
refer to [Configuring chunking](https://www.elastic.co/docs/explore-analyze/elastic-inference/inference-api#infer-chunking-config).

### Pre-chunking [pre-chunking]
```{applies_to}
stack: ga 9.1
```

You can pre-chunk the input by sending it to {{es}} as an array of
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

### Retrieving indexed chunks [retrieving-indexed-chunks]
```{applies_to}
stack: ga 9.2
serverless: ga
```

You can retrieve the individual chunks generated by your semantic field's chunking
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
% TEST[skip:Requires inference endpoint]

1. Use `"format": "chunks"` to return the field's text as the original text chunks that were indexed.

## Querying `semantic_text` fields [querying-semantic-text-fields]

You can query `semantic_text` fields using the following query types:

- Match query: The recommended method for querying `semantic_text` fields. You can use [Query DSL](/reference/query-languages/query-dsl/query-dsl-match-query.md) or [ES|QL](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match) syntax. To learn how to run match queries on `semantic_text` fields, refer to this [example](https://www.elastic.co/docs/solutions/search/semantic-search/semantic-search-semantic-text#semantic-text-semantic-search).

- kNN query: Finds the nearest vectors to a query vector using a similarity metric, mainly for advanced or combined search use cases. You can use [Query DSL](/reference/query-languages/query-dsl/query-dsl-knn-query.md#knn-query-with-semantic-text) or {applies_to}`stack: ga 9.2` [ES|QL](/reference/query-languages/esql/functions-operators/dense-vector-functions.md#esql-knn) syntax. To learn how to run knn queries on `semantic_text` fields, refer to this [example](/reference/query-languages/query-dsl/query-dsl-knn-query.md#knn-query-with-semantic-text).

- Sparse vector query: Executes searches using sparse vectors generated by a sparse retrieval model such as [ELSER](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-elser.md). You can use it with [Query DSL](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md) syntax. To learn how to run sparse vector queries on `semantic_text` fields, refer to this [example](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md#example-query-on-a-semantic_text-field).

- [Semantic query](/reference/query-languages/query-dsl/query-dsl-semantic-query.md): We don't recommend this legacy query type for _new_ projects, because the alternatives in this list enable more flexibility and customization. The `semantic` query remains available to support existing implementations.

## Returning `semantic_text` field embeddings [returning-semantic-field-embeddings]

By default, embeddings generated for `semantic_text` fields are stored internally and not included in the response when retrieving documents. Retrieving embeddings is useful when you want to:

- Reindex documents into another index with the same `inference_id` without re-running inference
- Export or migrate documents while preserving their embeddings
- Inspect or debug the raw embeddings generated for your content

The method for retrieving embeddings depends on your {{es}} version:

- If you use {{es}} 9.2 and later, or Serverless, use the [`_source.exclude_vectors`](#returning-semantic-field-embeddings-in-_source) parameter to include embeddings in `_source`. 
- If you use {{es}} versions earlier than 9.2, use the [`fields` parameter](#returning-semantic-field-embeddings-using-fields) with `_inference_fields` to retrieve embeddings.

### Returning semantic field embeddings in `_source` [returning-semantic-field-embeddings-in-_source]

```{applies_to}
stack: ga 9.2
serverless: ga
```
To include the full {{infer}} fields, including their embeddings, in `_source`, set the `_source.exclude_vectors` option to `false`:

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
% TEST[skip:Requires {{infer}} endpoint]

The embeddings will appear under `_inference_fields` in `_source`.

This works with the
[Get](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get),
[Search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search),
and
[Reindex](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex)
APIs.

#### Example: Reindex while preserving embeddings [reindex-while-preserving-embeddings]
```{applies_to}
stack: ga 9.2
serverless: ga
```

When reindexing documents with `semantic_text` fields, you can preserve existing embeddings by including them in the source documents. This allows documents to be re-indexed without triggering {{infer}} again. 

::::{warning}
The target index's `semantic_text` field must use the same `inference_id` as the source index to reuse existing embeddings. If the `inference_id` values do not match, the documents will fail the reindex task.
::::

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
% TEST[skip:Requires {{infer}} endpoint]

1. Sends the source documents with their stored embeddings to the destination index.

#### Example: Troubleshooting semantic_text fields [troubleshooting-semantic-text-fields]
```{applies_to}
stack: ga 9.2
serverless: ga
```

To verify that your embeddings look correct or debug embedding generation, retrieve the stored embeddings:

```console
POST test-index/_search
{
  "_source": {
    "exclude_vectors": false
  },
  "query": {
    "match": {
      "my_semantic_field": "Which country is Paris in?"
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]

This will return verbose chunked embeddings content that is used to perform
semantic search for `semantic_text` fields:

```console-response
{
  "took": 18,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": { "value": 1, "relation": "eq" },
    "max_score": 16.532316,
    "hits": [
      {
        "_index": "test-index",
        "_id": "1",
        "_score": 16.532316,
        "_source": {
          "my_semantic_field": "Paris is the capital of France.",
          "_inference_fields": {
            "my_semantic_field": {
              "inference": {
                "inference_id": ".elser-2-elasticsearch", <1>
                "model_settings": { <2>
                  "service": "elasticsearch",
                  "task_type": "sparse_embedding"
                },
                "chunks": {
                  "my_semantic_field": [
                    {
                      "start_offset": 0,
                      "end_offset": 31,
                      "embeddings": { <3>
                        "airport": 0.12011719,
                        "brussels": 0.032836914,
                        "capital": 2.1328125,
                        "capitals": 0.6386719,
                        "capitol": 1.2890625,
                        "cities": 0.78125,
                        "city": 1.265625,
                        "continent": 0.26953125,
                        "country": 0.59765625,
                        ...
                      }
                    }
                  ]
                }
              }
            }
          }
        }
      }
    ]
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]
1. The {{infer}} endpoint used to generate embeddings.
2. Lists details about the model used to generate embeddings, such as the service name and task type.
3. The embeddings generated for this chunk.

### Returning semantic field embeddings using `fields` [returning-semantic-field-embeddings-using-fields]

:::{important}
This method for returning semantic field embeddings is recommended only for {{es}} versions earlier than 9.2.
For version 9.2 and later, use the [`exclude_vectors`](#returning-semantic-field-embeddings-in-_source) parameter instead.
:::

To retrieve stored embeddings, use the `fields` parameter with `_inference_fields`. This lets you include the vector data that is not shown by default in the response. 

```console
POST my-index/_search
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
% TEST[skip:Requires {{infer}} endpoint]

The `fields` parameter works with the [Search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) API.

## Highlighting fragments [highlighting-fragments]

You can extract the most relevant fragments from a `semantic_text` field by using
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
% TEST[skip:Requires inference endpoint]

1. Specifies the maximum number of fragments to return.
2. Sorts the most relevant highlighted fragments by score when set to `score`. By default,
   fragments will be output in the order they appear in the field (order: none).

Highlighting is supported on fields other than `semantic_text`. However, if you
want to restrict highlighting to the semantic highlighter and return no
fragments when the field is not of type `semantic_text`, you can explicitly
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
% TEST[skip:Requires inference endpoint]

1. Ensures that highlighting is applied exclusively to `semantic_text` fields.

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
% TEST[skip:Requires inference endpoint]

1. Returns the first 5 fragments. Increase this value to retrieve additional fragments.

## Updates and partial updates [updates-and-partial-updates]

When updating documents that contain `semantic_text` fields, it's important to understand how inference is triggered:

Full document updates
:   Full document updates re-run inference on all `semantic_text` fields, even if their values did not change. This ensures that embeddings remain consistent with the current document state but can increase ingestion costs.

Partial updates using the Bulk API
:   Partial updates submitted through the [Bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) reuse existing embeddings when you omit `semantic_text` fields. Inference does not run for omitted fields, which can significantly reduce processing time and cost.

Partial updates using the Update API
:   Partial updates submitted through the [Update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) re-run inference on all `semantic_text` fields, even when you omit them from the `doc` object. Embeddings are re-generated regardless of whether field values changed.

To preserve existing embeddings and avoid unnecessary inference costs:

 * Use partial updates with the Bulk API.
 * Omit any `semantic_text` fields that did not change from the `doc` object in your request.

### Scripted updates [scripted-updates]

For indices containing `semantic_text` fields, updates that use scripts have the
following behavior:

- ✅ **Supported:** [Update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update)
- ❌ **Not supported:** [Bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk-1). Scripted updates will fail even if the script targets non-`semantic_text` fields.

## `copy_to` and multi-fields support [copy-to-and-multi-fields-support]

The `semantic_text` field type can serve as the target
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
% TEST[skip:Requires inference endpoint]

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
% TEST[skip:Requires inference endpoint]

## Document count discrepancy in `_cat/indices` [document-count-discrepancy]

When an index contains a `semantic_text` field, the `docs.count` value returned by the [`_cat/indices`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-indices) API may be higher than the number of documents you indexed. 
This occurs because `semantic_text` stores embeddings in [nested documents](/reference/elasticsearch/mapping-reference/nested.md), one per chunk. The `_cat/indices` API counts all documents in the Lucene index, including these hidden nested documents.

To count only top-level documents, excluding the nested documents that store embeddings, use one of the following APIs:

* `GET /<index>/_count`
* `GET _cat/count/<index>`

## Cross-cluster search (CCS) [cross-cluster-search]

```{applies_to}
stack: ga 9.2
serverless: unavailable
```

`semantic_text` supports [Cross-Cluster Search (CCS)](docs-content://solutions/search/cross-cluster-search.md) through the [`_search` endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search)
when [`ccs_minimize_roundtrips`](docs-content://solutions/search/cross-cluster-search.md#ccs-network-delays) is set to `true`.
This is the default value for `ccs_minimize_roundtrips`, so most CCS queries should work automatically:

```console
POST local-index,remote-cluster:remote-index/_search
{
  "query": {
    "match": {
      "my_semantic_field": "Which country is Paris in?"
    }
  }
}
```

## Limitations [limitations]

`semantic_text` field types have the following limitations:

* `semantic_text` fields are not currently supported as elements
  of [nested fields](/reference/elasticsearch/mapping-reference/nested.md).
* `semantic_text` fields can't currently be set as part
  of [dynamic templates](docs-content://manage-data/data-store/mapping/dynamic-templates.md).
* `semantic_text` fields are not supported in indices created prior to 8.11.0.
* `semantic_text` fields do not support [Cross-Cluster Search (CCS)](docs-content://solutions/search/cross-cluster-search.md) when [`ccs_minimize_roundtrips`](docs-content://solutions/search/cross-cluster-search.md#ccs-network-delays) is set to `false`.
* `semantic_text` fields do not support [Cross-Cluster Search (CCS)](docs-content://solutions/search/cross-cluster-search.md) in [ES|QL](/reference/query-languages/esql.md).
* `semantic_text` fields do not support [Cross-Cluster Replication (CCR)](docs-content://deploy-manage/tools/cross-cluster-replication.md).
