---
navigation_title: "Knn"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-knn-query.html
applies_to:
  stack: all
  serverless: all
---

# Knn query [query-dsl-knn-query]


Finds the *k* nearest vectors to a query vector, as measured by a similarity metric. *knn* query finds nearest vectors through approximate search on indexed dense_vectors. The preferred way to do approximate kNN search is through the [top level knn section](docs-content://solutions/search/vector/knn.md) of a search request. *knn* query is reserved for expert cases, where there is a need to combine this query with other queries, or perform a kNN search against a [semantic_text](/reference/elasticsearch/mapping-reference/semantic-text.md) field.

## Example request [knn-query-ex-request]

```console
PUT my-image-index
{
  "mappings": {
    "properties": {
       "image-vector": {
        "type": "dense_vector",
        "dims": 3,
        "index": true,
        "similarity": "l2_norm"
      },
      "file-type": {
        "type": "keyword"
      },
      "title": {
        "type": "text"
      }
    }
  }
}
```

1. Index your data.

    ```console
    POST my-image-index/_bulk?refresh=true
    { "index": { "_id": "1" } }
    { "image-vector": [1, 5, -20], "file-type": "jpg", "title": "mountain lake" }
    { "index": { "_id": "2" } }
    { "image-vector": [42, 8, -15], "file-type": "png", "title": "frozen lake"}
    { "index": { "_id": "3" } }
    { "image-vector": [15, 11, 23], "file-type": "jpg", "title": "mountain lake lodge" }
    ```
    % TEST[continued]

2. Run the search using the `knn` query, asking for the top 10 nearest vectors from each shard, and then combine shard results to get the top 3 global results.

    ```console
    POST my-image-index/_search
    {
      "size" : 3,
      "query" : {
        "knn": {
          "field": "image-vector",
          "query_vector": [-5, 9, -12],
          "k": 10
        }
      }
    }
    ```
    % TEST[continued]

3. {applies_to}`stack: ga 9.0-9.3` You can also provide a hex-encoded query vector string. Hex query vectors are byte-oriented (one byte per dimension, represented as two hex characters). For example, `[-5, 9, -12]` as signed bytes is `fb09f4`.

    ```console
    POST my-image-index/_search
    {
      "size" : 3,
      "query" : {
        "knn": {
          "field": "image-vector",
          "query_vector": "fb09f4",
          "k": 10
        }
      }
    }
    ```
    % TEST[continued]

4. {applies_to}`stack: ga 9.4` You can also provide a base64-encoded query vector string. For example, `[-5, 9, -12]` encoded as float32 big-endian bytes is `wKAAAEEQAADBQAAA`.

    ```console
    POST my-image-index/_search
    {
      "size" : 3,
      "query" : {
        "knn": {
          "field": "image-vector",
          "query_vector": "wKAAAEEQAADBQAAA",
          "k": 10
        }
      }
    }
    ```
    % TEST[continued]


## Top-level parameters for `knn` [knn-query-top-level-parameters]

`field`
:   (Required, string) The name of the vector field to search against. Must be a [`dense_vector` field with indexing enabled](/reference/elasticsearch/mapping-reference/dense-vector.md#index-vectors-knn-search), or a [`semantic_text` field](/reference/elasticsearch/mapping-reference/semantic-text.md) with a compatible dense vector {{infer}} model.

$$$knn-query-query-vector$$$ `query_vector`
:   (Optional, array of floats or string) Query vector. Must have the same number of dimensions as the vector field you are searching against.
    Must be one of:
      - An array of floats
      - A hex-encoded byte vector (one byte per dimension; for `bit`, one byte per 8 dimensions). {applies_to}`stack: ga 9.0-9.3`
      - A base64-encoded vector string. Base64 supports `float` and `bfloat16` (big-endian), `byte`, and `bit` encodings depending on the target field type. {applies_to}`stack: ga 9.4` {applies_to}`serverless: ga`
    Either this or `query_vector_builder` must be provided.


$$$knn-query-query-vector-builder$$$ `query_vector_builder`
:   (Optional, object) Query vector builder. A configuration object indicating how to build a query vector before executing the request. You must provide either a `query_vector_builder` or `query_vector`, but not both.  Refer to [Query vector builder types](#query-vector-builders-parameters) for parameter details and [Query vector builder examples](#query-vector-builders-overview) for usage examples.


`k`
:   (Optional, integer) The number of nearest neighbors to return from each shard. {{es}} collects `k` (or `k * oversample` if conditions for [`rescore_vector`](docs-content://solutions/search/vector/knn.md#the-rescore_vector-option) are met) results from each shard, then merges them to find the global top `k` results. This value must be less than or equal to `num_candidates`. Defaults to search request size.


`num_candidates`
:   (Optional, integer) The number of nearest neighbor candidates to consider per shard while doing knn search. Cannot exceed 10,000. Increasing `num_candidates` tends to improve the accuracy of the final results. Defaults to `1.5 * k` if `k` is set, or `1.5 * size` if `k` is not set. When [`rescore_vector`](docs-content://solutions/search/vector/knn.md#the-rescore_vector-option) are met) is applied, `num_candidates` is set to `max(num_candidates, k * oversample)`


`visit_percentage` {applies_to}`stack: ga 9.2`
:   (Optional, float) The percentage of vectors to explore per shard while doing knn search with `bbq_disk`. Must be between 0 and 100.  0 will default to using `num_candidates` for calculating the percent visited. Increasing `visit_percentage` tends to improve the accuracy of the final results.  If `visit_percentage` is set for `bbq_disk`, `num_candidates` is ignored. Defaults to ~1% per shard for every 1 million vectors.


`filter`
:   (Optional, query object) Query to filter the documents that can match. The kNN search will return the top documents that also match this filter. The value can be a single query or a list of queries. If `filter` is not provided, all documents are allowed to match.

The filter is a pre-filter, meaning that it is applied **during** the approximate kNN search to ensure that `num_candidates` matching documents are returned.


`similarity`
:   (Optional, float) The minimum similarity required for a document to be considered a match. The similarity value calculated relates to the raw [`similarity`](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-similarity) used. Not the document score. The matched documents are then scored according to [`similarity`](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-similarity) and the provided `boost` is applied.

`boost`
:   (Optional, float) Floating point number used to multiply the scores of matched documents. This value cannot be negative. Defaults to `1.0`.


`_name`
:   (Optional, string) Name field to identify the query


`rescore_vector` {applies_to}`stack: preview =9.0, ga 9.1+`
:   (Optional, object) Apply oversampling and rescoring to quantized vectors.

    **Parameters for `rescore_vector`**:

    `oversample`
    :   (Required, float)

        Applies the specified oversample factor to `k` on the approximate kNN search. The approximate kNN search will:

     * Retrieve `num_candidates` candidates per shard.
     * From these candidates, the top `k * oversample` candidates per shard will be rescored using the original vectors.
     * The top `k` rescored candidates will be returned. Must be one of the following values:
       * \>= 1f to indicate the oversample factor
       * Exactly `0` to indicate that no oversampling and rescoring should occur. {applies_to}`stack: ga 9.1`

    For more information, refer to [oversampling and rescoring quantized vectors](docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring).

    ::::{note}
    Rescoring only makes sense for [quantized](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization) vectors. The `rescore_vector` option will be ignored for non-quantized `dense_vector` fields, because the original vectors are used for scoring.
    ::::

## Pre-filters and post-filters in knn query [knn-query-filtering]

There are two ways to filter documents that match a kNN query:

1. **pre-filtering** – filter is applied during the approximate kNN search to ensure that `k` matching documents are returned.
2. **post-filtering** – filter is applied after the approximate kNN search completes, which results in fewer than k results, even when there are enough matching documents.

Pre-filtering is supported through the `filter` parameter of the `knn` query. Also filters from [aliases](docs-content://manage-data/data-store/aliases.md#filter-alias) are applied as pre-filters.

All other filters found in the Query DSL tree are applied as post-filters. For example, `knn` query finds the top 3 documents with the nearest vectors (k=3), which are combined with  `term` filter, that is post-filtered. The final set of documents will contain only a single document that passes the post-filter.

```console
POST my-image-index/_search
{
  "size" : 10,
  "query" : {
    "bool" : {
      "must" : {
        "knn": {
          "field": "image-vector",
          "query_vector": [-5, 9, -12],
          "k": 3
        }
      },
      "filter" : {
        "term" : { "file-type" : "png" }
      }
    }
  }
}
```
% TEST[continued]


## Hybrid search with knn query [knn-query-in-hybrid-search]

Knn query can be used as a part of hybrid search, where knn query is combined with other lexical queries. For example, the query below finds documents with `title` matching `mountain lake`, and combines them with the top 10 documents that have the closest image vectors to the `query_vector`. The combined documents are then scored and the top 3 top scored documents are returned.


```console
POST my-image-index/_search
{
  "size" : 3,
  "query": {
    "bool": {
      "should": [
        {
          "match": {
            "title": {
              "query": "mountain lake",
              "boost": 1
            }
          }
        },
        {
          "knn": {
            "field": "image-vector",
            "query_vector": [-5, 9, -12],
            "k": 10,
            "boost": 2
          }
        }
      ]
    }
  }
}
```
% TEST[continued]


## Knn query inside a nested query [knn-query-with-nested-query]

The `knn` query can be used inside a nested query. The behaviour here is similar to [top level nested kNN search](docs-content://solutions/search/vector/knn.md#nested-knn-search):

* kNN search over nested `dense_vector`s diversifies the top results over the top-level document
* `filter` both over the top-level document metadata and `nested` is supported and acts as a pre-filter

To ensure correct results: each individual filter must be either over:

- Top-level metadata
- `nested` metadata {applies_to}`stack: ga 9.2`
  :::{note}
  A single knn query supports multiple filters, where some filters can be over the top-level metadata and some over nested.
  :::

### Basic nested knn search

This query performs a basic nested knn search:

```js
{
  "query" : {
    "nested" : {
      "path" : "paragraph",
        "query" : {
          "knn": {
            "query_vector": [0.45, 0.50],
            "field": "paragraph.vector"
        }
      }
    }
  }
}
```
% NOTCONSOLE

### Filter over nested metadata

```{applies_to}
stack: ga 9.2
```

This query filters over nested metadata. For scoring parent documents, this query only considers vectors that
have "paragraph.language" set to "EN":

```js
{
  "query" : {
    "nested" : {
      "path" : "paragraph",
        "query" : {
          "knn": {
            "query_vector": [0.45, 0.50],
            "field": "paragraph.vector",
            "filter": {
              "match": {
                "paragraph.language": "EN"
              }
            }
        }
      }
    }
  }
}
```
% NOTCONSOLE

### Multiple filters (nested and top-level metadata)

```{applies_to}
stack: ga 9.2
```

This query uses multiple filters: one over nested metadata and another over the top level metadata. For scoring parent documents,
this query only considers vectors whose parent's title contain "essay"
word and have "paragraph.language" set to "EN":

```js
{
  "query" : {
    "nested" : {
      "path" : "paragraph",
      "query" : {
        "knn": {
          "query_vector": [0.45, 0.50],
          "field": "paragraph.vector",
          "filter": [
            {
              "match": {
                "paragraph.language": "EN"
              }
            },
            {
              "match": {
                "title": "essay"
              }
            }
          ]
        }
      }
    }
  }
}
```
% NOTCONSOLE

Note that nested `knn` only supports `score_mode=max`.

## Knn query on a semantic_text field [knn-query-with-semantic-text]

Elasticsearch supports knn queries over a [
`semantic_text` field](/reference/elasticsearch/mapping-reference/semantic-text.md).

Here is an example using the `query_vector_builder`:

```js
{
  "query": {
    "knn": {
      "field": "inference_field",
      "k": 10,
      "num_candidates": 100,
      "query_vector_builder": {
        "text_embedding": {
          "model_text": "test"
        }
      }
    }
  }
}
```
% NOTCONSOLE

Note that for `semantic_text` fields, the `model_id` does not have to be
provided as it can be inferred from the `semantic_text` field mapping.

Knn search using query vectors over `semantic_text` fields is also supported,
with no change to the API.

## Build query vectors for knn search

Query vector builders let you generate vectors directly from inputs such as text or base64-encoded images at search time.

{{es}} provides three query vector builders. Each builder generates a query vector from a different type of input or source.

- [`text_embedding`](#knn-query-builder-text-embedding): Generates a query vector from text input. This is useful when your application sends raw text, such as a search query, and you want {{es}} to convert it into an embedding automatically instead of generating the vector in advance.
- [`embedding`](#knn-query-builder-embedding): {applies_to}`stack: preview 9.4` {applies_to}`serverless: preview` Generates a query vector from multimodal input, such as text or base64-encoded images. Use this when you want to generate embeddings dynamically from different types of input without creating them in advance.
- [`lookup`](#knn-query-builder-lookup): {applies_to}`stack: ga 9.4` Retrieves an existing vector from a stored document to use as the query vector. This is useful when you want to find documents similar to an existing document, without generating a new embedding at search time.

Refer to [Query vector builder types](#query-vector-builders-parameters) for parameter details and [Query vector builder examples](#query-vector-builders-overview) for usage examples.

### Query vector builder types [query-vector-builders-parameters]

$$$knn-query-builder-lookup$$$ `lookup` {applies_to}`stack: ga 9.4`
:   Build the query vector by looking up an existing document's vector. For an example, refer to [`lookup`](#lookup-builder).

        **Parameters for `lookup`**:

        `id`
        :   (Required, string) The ID of the document to look up.

        `path`
        :   (Required, string) The name of the vector field in the document to use as the query vector.

        `index`
        :   (Required, string) The name of the index containing the document to look up

        `routing`
        :   (Optional, string) The routing value to use when looking up the document.

$$$knn-query-builder-text-embedding$$$ `text_embedding`
:   Build the query vector by generating an embedding from input text. For an example, refer to [`text_embedding`](#text-embedding-builder).

      **Parameters for `text_embedding`**:

        `model_id`
        :   (Optional, string) Identifier of the text embedding model that generates the query vector. Use the same model that produced vectors in your index.

::::{note}
When you query only [semantic_text](/reference/elasticsearch/mapping-reference/semantic-text.md) fields, you can omit `model_id` because {{es}} uses the `inference_id` from the `semantic_text` field mapping (for example the search-time {{infer}} endpoint configured on the field).

For [`dense_vector`](/reference/elasticsearch/mapping-reference/dense-vector.md) fields or when you need a different model than the one mapped on `semantic_text`, set `model_id` explicitly.
::::

        `model_text`
        :   (Required, string) The query text passed to the model to produce the embedding.

      For an example request, refer to [`text_embedding`](#text-embedding-builder). For a broader overview of semantic kNN search, refer to [Perform semantic search](docs-content://solutions/search/vector/knn.md#knn-semantic-search).

$$$knn-query-builder-embedding$$$ `embedding` {applies_to}`stack: preview 9.4` {applies_to}`serverless: preview`
:   Build the query vector by generating an embedding from text or a base64-encoded image. This enables multimodal search, where different types of input can be used to generate a vector and retrieve similar documents. For an example, refer to [`embedding`](#embedding-builder).

      **Parameters for `embedding`**:

        `inference_id`
        :   (Required, string) The ID of the {{infer}} service used to generate the embedding. Must reference an {{infer}} service configured with the `embedding` task type.

        `input`
        :   (Required, string, object, or array) The input used to generate the query vector. You can provide the input in one of the following formats:

            - Single object: A single multimodal input (text or image). For an example, refer to [single input object](#embedding-example-single-object).

            - Array of objects: Multiple inputs. These are combined into a single embedding. For an example, refer to [multiple inputs](#embedding-example-array-input).

            - String: A single text input. Equivalent to `value` when providing an object with "type": "text". For an example, refer to [string input](#embedding-example-string-input).

::::{dropdown} Parameters for `input` (when `input` is an object or an array of objects)
:open:

`type`
:   (Required, string) The type of the input. For text input, use `text`. For image input, use `image`.

`format`
:   (Optional, string) The format of the input value. If omitted, the default for the `type` is used. For text input, specify `text` or omit (defaults to `text`). For image input, specify `base64` or omit (defaults to `base64`).

`value`
:   (Required, string) The value to generate the embedding from. For text input, a text string. For image input, must be a data URI for a base64-encoded image.

::::

        `timeout`
        :   (Optional, time value) Maximum time to wait for the embedding {{infer}} request to complete. Defaults to `30s` when omitted.

### Query vector builder examples [query-vector-builders-overview]

#### `lookup` [lookup-builder]
```{applies_to}
stack: ga 9.4
```

Use the `lookup` query vector builder to retrieve an existing vector from a stored document and use it as the query vector.

```console
{
  "knn": {
    "field": "dense-vector-field",
    "k": 10,
    "num_candidates": 100,
    "query_vector_builder": {
      "lookup": {
        "index": "my-index",        <1>
        "id": "document-1",         <2>
        "path": "my_vector"         <3>
      }
    }
  }
}
```

1. The name of the index containing the document to look up.
2. The ID of the document to look up.
3. The name of the vector field in the document to use as the query vector.

#### `text_embedding` [text-embedding-builder] 

Use the `text_embedding` query vector builder to generate a query vector from text input.

```console
POST my-index/_search
{
  "knn": {
    "field": "dense-vector-field",
    "k": 10,
    "num_candidates": 100,
    "query_vector_builder": {
      "text_embedding": {
        "model_id": "my-text-embedding-model", <1>
        "model_text": "The opposite of blue" <2>
      }
    }
  }
}
```

1. The ID of the text embedding model or deployment in {{es}} that generates the query vector. Use the same model that produced vectors in your index. When you query only [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) fields, you can omit `model_id` because {{es}} uses the `inference_id` from the [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) field mapping (for example the search-time {{infer}} endpoint configured on the field).
2. The query text passed to the model to produce the embedding.

#### `embedding` [embedding-builder]

```{applies_to}
stack: preview 9.4
serverless: preview
```

Use the `embedding` query vector builder to generate a query vector from multimodal input.

This builder supports both text and base64-encoded image inputs. You can also use multiple inputs to generate a query vector, enabling multimodal search scenarios such as searching with both text and an image.

##### Example: single input object (image) [embedding-example-single-object]

```console
POST my-index/_search
{
  "knn": {
    "field": "dense-vector-field",
    "k": 10,
    "num_candidates": 100,
    "query_vector_builder": {
      "embedding": {
        "inference_id": "my-embedding-endpoint", <1>
        "input": {
          "type": "image", <2>
          "format": "base64", <3>
          "value": "data:image/jpeg;base64,iVBORw0KGgoAAAANSUhEUgAAAAUA\nAAAAFCAIAAAACDbGyAAAAHElEQVQI12P4\n//8/w38GIAXDIBKE0DHxgljNBAAO\n9TXL0Y4OHwAAAABJRU5ErkJggg==" <4>
        }
      }
    }
  }
}
```

1. The ID of the {{infer}} endpoint used to generate the embedding. This must reference an {{infer}} service configured with the `EMBEDDING` task type.
2. The type of the input. Valid values are `text` and `image`.
3. The format of the input value. Defaults to `text` for text input. Defaults to `base64` for image input.
4. The value used to generate the embedding. For image, this must be a data URI for a base64-encoded image. The example above is a small sample base64 string. In real usage, this would be a much longer string generated from an actual image file.

##### Example: multiple inputs (image + text) [embedding-example-array-input]

```console
POST my-index/_search
{
  "knn": {
    "field": "dense-vector-field",
    "k": 10,
    "num_candidates": 100,
    "query_vector_builder": {
      "embedding": {
        "inference_id": "my-embedding-endpoint", <1>
        "input": [
          {
            "type": "text",
            "value": "red shoes"
          },
          {
            "type": "image",
            "format": "base64", <2>
            "value": "data:image/jpeg;base64,iVBORw0KGgoAAAANSUhEUgAAAAUA\nAAAAFCAIAAAACDbGyAAAAHElEQVQI12P4\n//8/w38GIAXDIBKE0DHxgljNBAAO\n9TXL0Y4OHwAAAABJRU5ErkJggg==" <3>
          }
        ]
      }
    }
  }
}
```

1. The ID of the {{infer}} endpoint used to generate the embedding. This must reference an {{infer}} service configured with the `EMBEDDING` task type.
2. The format of the input value. Defaults to `base64` for image input.
3. The value used to generate the embedding. For image, this must be a data URI for a base64-encoded image. The example above is a small sample base64 string. In real usage, this would be a much longer string generated from an actual image file.

##### Example: string input (shorthand) [embedding-example-string-input]

```console
POST my-index/_search
{
  "knn": {
    "field": "dense-vector-field",
    "k": 10,
    "num_candidates": 100,
    "query_vector_builder": {
      "embedding": {
        "inference_id": "my-embedding-endpoint", <1>
        "input": "red shoes" <2>
      }
    }
  }
}
```

1. The ID of the {{infer}} endpoint used to generate the embedding. This must reference an {{infer}} service configured with the `EMBEDDING` task type.
2. A shorthand for a single text input. Equivalent to:

```console
{
  "type": "text",
  "value": "red shoes"
}
```

## Knn query with aggregations [knn-query-aggregations]

`knn` query calculates aggregations on top `k` documents from each shard. Thus, the final results from aggregations contain `k * number_of_shards` documents. This is different from the [top level knn section](docs-content://solutions/search/vector/knn.md) where aggregations are calculated on the global top `k` nearest documents.
