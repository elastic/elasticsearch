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


## Top-level parameters for `knn` [knn-query-top-level-parameters]

`field`
:   (Required, string) The name of the vector field to search against. Must be a [`dense_vector` field with indexing enabled](/reference/elasticsearch/mapping-reference/dense-vector.md#index-vectors-knn-search), or a [`semantic_text` field](/reference/elasticsearch/mapping-reference/semantic-text.md) with a compatible dense vector inference model.


`query_vector`
:   (Optional, array of floats or string) Query vector. Must have the same number of dimensions as the vector field you are searching against. Must be either an array of floats or a hex-encoded byte vector. Either this or `query_vector_builder` must be provided.


`query_vector_builder`
:   (Optional, object) Query vector builder. A configuration object indicating how to build a query_vector before executing the request. You must provide either a `query_vector_builder` or `query_vector`, but not both. Refer to [Perform semantic search](docs-content://solutions/search/vector/knn.md#knn-semantic-search) to learn more.

If all queried fields are of type [semantic_text](/reference/elasticsearch/mapping-reference/semantic-text.md), the inference ID associated with the `semantic_text` field may be inferred.


`k`
:   (Optional, integer) The number of nearest neighbors to return from each shard. {{es}} collects `k` results from each shard, then merges them to find the global top results. This value must be less than or equal to `num_candidates`. Defaults to search request size.


`num_candidates`
:   (Optional, integer) The number of nearest neighbor candidates to consider per shard while doing knn search. Cannot exceed 10,000. Increasing `num_candidates` tends to improve the accuracy of the final results. Defaults to `1.5 * k` if `k` is set, or `1.5 * size` if `k` is not set.


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


`rescore_vector` {applies_to}`stack: preview 9.0, ga 9.1`
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

    See [oversampling and rescoring quantized vectors](docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring) for details.

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

## Knn query with aggregations [knn-query-aggregations]

`knn` query calculates aggregations on top `k` documents from each shard. Thus, the final results from aggregations contain `k * number_of_shards` documents. This is different from the [top level knn section](docs-content://solutions/search/vector/knn.md) where aggregations are calculated on the global top `k` nearest documents.
