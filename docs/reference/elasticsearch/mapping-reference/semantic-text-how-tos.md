---
navigation_title: "How-to guides"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# How-to guides for `semantic_text`

This page provides procedure descriptions and examples for common tasks when working with `semantic_text` fields, including configuring {{infer}} endpoints, pre-chunking content, retrieving embeddings, highlighting fragments, and performing {{ccs}}.

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

### Retrieve indexed chunks [retrieving-indexed-chunks]
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
% TEST[skip:Requires {{infer}} endpoint]

1. Use `"format": "chunks"` to return the field's text as the original text chunks that were indexed.

## Return `semantic_text` field embeddings [returning-semantic-field-embeddings]

By default, embeddings generated for `semantic_text` fields are stored internally and not included in the response when retrieving documents. Retrieving embeddings is useful when you want to:

- Reindex documents into another index with the same `inference_id` without re-running {{infer}}
- Export or migrate documents while preserving their embeddings
- Inspect or debug the raw embeddings generated for your content

The method for retrieving embeddings depends on your {{es}} version:

- If you use {{es}} 9.2 and later, or {{serverless-short}}, use the [`_source.exclude_vectors`](#returning-semantic-field-embeddings-in-_source) parameter to include embeddings in `_source`. 
- If you use {{es}} versions earlier than 9.2, use the [`fields` parameter](#returning-semantic-field-embeddings-using-fields) with `_inference_fields` to retrieve embeddings.

### Return semantic field embeddings in `_source` [returning-semantic-field-embeddings-in-_source]

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

#### Example: Troubleshoot `semantic_text` fields [troubleshooting-semantic-text-fields]
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

### Return semantic field embeddings using `fields` [returning-semantic-field-embeddings-using-fields]

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

## Highlight the most relevant fragments [highlighting-fragments]

Extract the most relevant fragments from a `semantic_text` field using the [highlight parameter](/reference/elasticsearch/rest-apis/highlighting.md) in the [Search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search).

:::::{stepper}

::::{step} Highlight semantic_text fields

Use the highlight parameter with `number_of_fragments` and `order` to control fragment selection and sorting:

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
% TEST[skip:Requires {{infer}} endpoint]

1. Specifies the maximum number of fragments to return.
2. Sorts the most relevant highlighted fragments by score when set to `score`. By default, fragments are output in the order they appear in the field (order: none).

::::

::::{step} Enforce semantic highlighter

To restrict highlighting to the semantic highlighter and return no fragments when the field is not of type `semantic_text`, explicitly set the highlighter type to `semantic`:

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
% TEST[skip:Requires {{infer}} endpoint]

1. Ensures that highlighting is applied exclusively to `semantic_text` fields.

::::

::::{step} Retrieve fragments in original order

To retrieve all fragments from the semantic highlighter in their original indexing order without scoring, use a `match_all` query as the `highlight_query`:

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
% TEST[skip:Requires {{infer}} endpoint]

1. Returns the first 5 fragments. Increase this value to retrieve additional fragments.

::::

:::::

## Perform {{ccs}} (CCS) for `semantic_text` [cross-cluster-search]

```{applies_to}
stack: ga 9.2
serverless: unavailable
```

`semantic_text` supports [{{ccs}} (CCS)](docs-content://solutions/search/cross-cluster-search.md) through the [`_search` endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) when [`ccs_minimize_roundtrips`](docs-content://solutions/search/cross-cluster-search.md#ccs-network-delays) is set to `true`. This is the default value, so most CCS queries work automatically.

Query `semantic_text` fields across clusters using the standard search endpoint with cluster notation:

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

