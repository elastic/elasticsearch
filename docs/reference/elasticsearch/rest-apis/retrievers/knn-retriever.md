---
applies_to:
 stack: all
 serverless: ga
---

# kNN retriever [knn-retriever]

A kNN retriever returns top documents from a [k-nearest neighbor search (kNN)](docs-content://solutions/search/vector/knn.md).


## Parameters [knn-retriever-parameters]

`field`
:   (Required, string)

    The name of the vector field to search against. Must be a [`dense_vector` field with indexing enabled](/reference/elasticsearch/mapping-reference/dense-vector.md#index-vectors-knn-search).


`query_vector`
:   (Required if `query_vector_builder` is not defined, array of `float`)

    Query vector. Must have the same number of dimensions as the vector field you are searching against. Must be either an array of floats or a hex-encoded byte vector.


`query_vector_builder`
:   (Required if `query_vector` is not defined, query vector builder object)

    Defines a [model](docs-content://solutions/search/vector/knn.md#knn-semantic-search) to build a query vector.


`k`
:   (Required, integer)

    Number of nearest neighbors to return as top hits. This value must be fewer than or equal to `num_candidates`.


`num_candidates`
:   (Required, integer)

    The number of nearest neighbor candidates to consider per shard. Needs to be greater than `k`, or `size` if `k` is omitted, and cannot exceed 10,000. {{es}} collects `num_candidates` results from each shard, then merges them to find the top `k` results. Increasing `num_candidates` tends to improve the accuracy of the final `k` results. Defaults to `Math.min(1.5 * k, 10_000)`.


`visit_percentage` {applies_to}`stack: ga 9.2`
:   (Optional, float)

    The percentage of vectors to explore per shard while doing knn search with `bbq_disk`. Must be between 0 and 100.  0 will default to using `num_candidates` for calculating the percent visited. Increasing `visit_percentage` tends to improve the accuracy of the final results.  If `visit_percentage` is set for `bbq_disk`, `num_candidates` is ignored. Defaults to ~1% per shard for every 1 million vectors.


`filter`
:   (Optional, [query object or list of query objects](/reference/query-languages/querydsl.md))

    Query to filter the documents that can match. The kNN search will return the top `k` documents that also match this filter. The value can be a single query or a list of queries. If `filter` is not provided, all documents are allowed to match.


`similarity`
:   (Optional, float)

    The minimum similarity required for a document to be considered a match. The similarity value calculated relates to the raw [`similarity`](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-similarity) used. Not the document score. The matched documents are then scored according to [`similarity`](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-similarity) and the provided `boost` is applied.

    The `similarity` parameter is the direct vector similarity calculation.

    * `l2_norm`: also known as Euclidean, will include documents where the vector is within the `dims` dimensional hypersphere with radius `similarity` with origin at `query_vector`.
    * `cosine`, `dot_product`, and `max_inner_product`: Only return vectors where the cosine similarity or dot-product are at least the provided `similarity`.

    Read more here: [knn similarity search](docs-content://solutions/search/vector/knn.md#knn-similarity-search)


`rescore_vector` {applies_to}`stack: preview 9.0, ga 9.1`
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


See [oversampling and rescoring quantized vectors](docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring) for details.


## Restrictions [_restrictions_2]

The parameters `query_vector` and `query_vector_builder` cannot be used together.


## Example [knn-retriever-example]

<!--
```console
PUT /restaurants
{
  "mappings": {
    "properties": {
      "region": { "type": "keyword" },
      "year": { "type": "keyword" },
      "vector": {
        "type": "dense_vector",
        "dims": 3
      }
    }
  }
}

POST /restaurants/_bulk?refresh
{"index":{}}
{"region": "Austria", "year": "2019", "vector": [10, 22, 77]}
{"index":{}}
{"region": "France", "year": "2019", "vector": [10, 22, 78]}
{"index":{}}
{"region": "Austria", "year": "2020", "vector": [10, 22, 79]}
{"index":{}}
{"region": "France", "year": "2020", "vector": [10, 22, 80]}

PUT /movies

PUT /books
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "copy_to": "title_semantic"
      },
      "description": {
        "type": "text",
        "copy_to": "description_semantic"
      },
      "title_semantic": {
        "type": "semantic_text"
      },
      "description_semantic": {
        "type": "semantic_text"
      }
    }
  }
}

PUT _query_rules/my-ruleset
{
    "rules": [
        {
            "rule_id": "my-rule1",
            "type": "pinned",
            "criteria": [
                {
                    "type": "exact",
                    "metadata": "query_string",
                    "values": [ "pugs" ]
                }
            ],
            "actions": {
                "ids": [
                    "id1"
                ]
            }
        }
    ]
}
```
% TESTSETUP

```console
DELETE /restaurants
DELETE /movies
DELETE /books
```
% TEARDOWN
-->

```console
GET /restaurants/_search
{
  "retriever": {
    "knn": { <1>
      "field": "vector", <2>
      "query_vector": [10, 22, 77], <3>
      "k": 10, <4>
      "num_candidates": 10 <5>
    }
  }
}
```

1. Configuration for k-nearest neighbor (knn) search, which is based on vector similarity.
2. Specifies the field name that contains the vectors.
3. The query vector against which document vectors are compared in the `knn` search.
4. The number of nearest neighbors to return as top hits. This value must be fewer than or equal to `num_candidates`.
5. The size of the initial candidate set from which the final `k` nearest neighbors are selected.
