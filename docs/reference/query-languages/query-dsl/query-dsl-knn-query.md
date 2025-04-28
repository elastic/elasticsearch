---
navigation_title: "Knn"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-knn-query.html
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


`filter`
:   (Optional, query object) Query to filter the documents that can match. The kNN search will return the top documents that also match this filter. The value can be a single query or a list of queries. If `filter` is not provided, all documents are allowed to match.

The filter is a pre-filter, meaning that it is applied **during** the approximate kNN search to ensure that `num_candidates` matching documents are returned.


`similarity`
:   (Optional, float) The minimum similarity required for a document to be considered a match. The similarity value calculated relates to the raw [`similarity`](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-similarity) used. Not the document score. The matched documents are then scored according to [`similarity`](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-similarity) and the provided `boost` is applied.


`rescore_vector`
:   (Optional, object) Apply oversampling and rescoring to quantized vectors.

::::{note}
Rescoring only makes sense for quantized vectors; when [quantization](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization) is not used, the original vectors are used for scoring. Rescore option will be ignored for non-quantized `dense_vector` fields.
::::


`oversample`
:   (Required, float)

    Applies the specified oversample factor to `k` on the approximate kNN search. The approximate kNN search will:

    * Retrieve `num_candidates` candidates per shard.
    * From these candidates, the top `k * oversample` candidates per shard will be rescored using the original vectors.
    * The top `k` rescored candidates will be returned.
    Must be >= 1f to indicate oversample factor, or exactly `0` to indicate that no oversampling and rescoring should occur.


See [oversampling and rescoring quantized vectors](docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring) for details.


`boost`
:   (Optional, float) Floating point number used to multiply the scores of matched documents. This value cannot be negative. Defaults to `1.0`.


`_name`
:   (Optional, string) Name field to identify the query



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


## Hybrid search with knn query [knn-query-in-hybrid-search]

Knn query can be used as a part of hybrid search, where knn query is combined with other lexical queries. For example, the query below finds documents with `title` matching `mountain lake`, and combines them with the top 10 documents that have the closest image vectors to the `query_vector`. The combined documents are then scored and the top 3 top scored documents are returned.

+

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


## Knn query inside a nested query [knn-query-with-nested-query]

`knn` query can be used inside a nested query. The behaviour here is similar to [top level nested kNN search](docs-content://solutions/search/vector/knn.md#nested-knn-search):

* kNN search over nested dense_vectors diversifies the top results over the top-level document
* `filter`  over the top-level document metadata is supported and acts as a pre-filter
* `filter` over `nested` field metadata is not supported

A sample query can look like below:

```json
{
  "query" : {
    "nested" : {
      "path" : "paragraph",
        "query" : {
          "knn": {
            "query_vector": [
                0.45,
                45
            ],
            "field": "paragraph.vector",
            "num_candidates": 2
        }
      }
    }
  }
}
```

Note that nested `knn` only supports `score_mode=max`.

## Knn query with aggregations [knn-query-aggregations]

`knn` query calculates aggregations on top `k` documents from each shard. Thus, the final results from aggregations contain `k * number_of_shards` documents. This is different from the [top level knn section](docs-content://solutions/search/vector/knn.md) where aggregations are calculated on the global top `k` nearest documents.
