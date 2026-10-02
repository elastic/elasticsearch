---
navigation_title: "Rank vectors"
applies_to:
  stack: preview 9.0
  serverless: preview
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rank-vectors.html
---

# Rank vectors [rank-vectors]

The `rank_vectors` field type enables late-interaction dense vector scoring in Elasticsearch. The number of vectors per field can vary, but they must all share the same number of dimensions and element type.

The purpose of vectors stored in this field is second order ranking documents with max-sim similarity.

Here is a simple example of using this field with `float` elements.

```console
PUT my-rank-vectors-float
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "rank_vectors"
      }
    }
  }
}

PUT my-rank-vectors-float/_doc/1
{
  "my_vector" : [[0.5, 10, 6], [-0.5, 10, 10]]
}
```
% TESTSETUP

In addition to the `float` element type, `bfloat16`, `byte`, and `bit` element types are also supported.

Here is an example of using this field with `bfloat16` elements.
```console
PUT my-rank-vectors-bfloat16
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "rank_vectors",
        "element_type": "bfloat16"
      }
    }
  }
}

PUT my-rank-vectors-bfloat16/_doc/1
{
  "my_vector" : [[0.5, 10, 6], [-0.5, 10, 10]]
}
```

Here is an example of using this field with `byte` elements.

```console
PUT my-rank-vectors-byte
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "rank_vectors",
        "element_type": "byte"
      }
    }
  }
}

PUT my-rank-vectors-byte/_doc/1
{
  "my_vector" : [[1, 2, 3], [4, 5, 6]]
}
```

Here is an example of using this field with `bit` elements.

```console
PUT my-rank-vectors-bit
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "rank_vectors",
        "element_type": "bit"
      }
    }
  }
}

POST /my-rank-vectors-bit/_bulk?refresh
{"index": {"_id" : "1"}}
{"my_vector": [127, -127, 0, 1, 42]}
{"index": {"_id" : "2"}}
{"my_vector": "8100012a7f"}
```

## Parameters for rank vectors fields [rank-vectors-params]

The `rank_vectors` field type supports the following parameters:

$$$rank-vectors-element-type$$$

`element_type`
:   (Optional, string) The data type used to encode vectors. The supported data types are `float` (default), `bfloat16`, `byte`, and `bit`.

::::{dropdown} Valid values for element_type
`float`
:   indexes a 4-byte floating-point value per dimension. This is the default value.

`bfloat16` {applies_to}`stack: ga 9.3`
:   indexes a 2-byte floating-point value per dimension.

`byte`
:   indexes a 1-byte integer value per dimension.

`bit`
:   indexes a single bit per dimension. Useful for very high-dimensional vectors or models that specifically support bit vectors. NOTE: when using `bit`, the number of dimensions must be a multiple of 8 and must represent the number of bits.

::::


`dims`
:   (Optional, integer) Number of vector dimensions. Can’t exceed `4096`. If `dims` is not specified, it will be set to the length of the first vector added to the field.

A single document can hold at most `8192` vectors in a `rank_vectors` field. Indexing a document with more vectors than that fails with an error. The same limit applies to the query vector of a max-sim function: passing more than `8192` query vectors fails the search request.

## Accessing `rank_vectors` fields in search responses
```{applies_to}
stack: ga 9.2
serverless: ga
```

By default, `rank_vectors` fields are **not included in `_source`** in responses from the `_search`, `_msearch`, `_get`, and `_mget` APIs.
This helps reduce response size and improve performance, especially in scenarios where vectors are used solely for similarity scoring and not required in the output.

To retrieve vector values explicitly, you can use:

* The `fields` option to request specific vector fields directly:

```console
POST my-rank-vectors-float/_search
{
  "fields": ["my_vector"]
}
```

- The `_source.exclude_vectors` flag to re-enable vector inclusion in `_source` responses:

```console
POST my-rank-vectors-float/_search
{
  "_source": {
    "exclude_vectors": false
  }
}
```

### Storage behavior and `_source`

By default, `rank_vectors` fields are not stored in `_source` on disk. This is also controlled by the index setting `index.mapping.exclude_source_vectors`.
This setting is enabled by default for newly created indices and can only be set at index creation time.

When enabled:

* `rank_vectors` fields are removed from `_source` and the rest of the `_source` is stored as usual.
* If a request includes `_source` and vector values are needed (e.g., during recovery or reindex), the vectors are rehydrated from their internal format.

This setting is compatible with synthetic `_source`, where the entire `_source` document is reconstructed from columnar storage. In full synthetic mode, no `_source` is stored on disk, and all fields — including vectors — are rebuilt when needed.

### Rehydration and precision

When vector values are rehydrated (e.g., for reindex, recovery, or explicit `_source` requests), they are restored from their internal format. Internally, vectors are stored at float precision, so if they were originally indexed as higher-precision types (e.g., `double` or `long`), the rehydrated values will have reduced precision. This lossy representation is intended to save space while preserving search quality.

### Storing original vectors in `_source`

If you want to preserve the original vector values exactly as they were provided, you can re-enable vector storage in `_source`:

```console
PUT my-index-include-vectors
{
  "settings": {
    "index.mapping.exclude_source_vectors": false
  },
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "rank_vectors",
        "dims": 128
      }
    }
  }
}
```

When this setting is disabled:

* `rank_vectors` fields are stored as part of the `_source`, exactly as indexed.
* The index will store both the original `_source` value and the internal representation used for vector search, resulting in increased storage usage.
* Vectors are once again returned in `_source` by default in all relevant APIs, with no need to use `exclude_vectors` or `fields`.

This configuration is appropriate when full source fidelity is required, such as for auditing or round-tripping exact input values.

## Scoring with rank vectors [rank-vectors-scoring]

Rank vectors can be accessed and used in [`script_score` queries](/reference/query-languages/query-dsl/query-dsl-script-score-query.md).

`rank_vectors` fields are not searchable on their own: they are stored as doc values only, with no approximate nearest neighbor (ANN) index, and the [`knn` query](/reference/query-languages/query-dsl/query-dsl-knn-query.md) does not accept them. All scoring is brute force, which is why this field type is meant for **second order ranking**: candidates must first be retrieved by another query — the inner `query` of the `script_score` — and the max-sim score is then computed for each of those candidates.

Cost therefore scales with the number of candidates × the number of vectors per document × the number of query vectors. Keep the inner query selective, for example by retrieving a first-pass candidate set with a [`knn` query](/reference/query-languages/query-dsl/query-dsl-knn-query.md) over a separate `dense_vector` field, or by using a [`rescorer` retriever](/reference/elasticsearch/rest-apis/retrievers.md) so that max-sim only runs over the top documents.

For example, the following query scores documents based on the maxSim similarity between the query vector and the vectors stored in the `my_vector` field:

```console
GET my-rank-vectors-float/_search
{
  "query": {
    "script_score": {
      "query": {
        "match_all": {}
      },
      "script": {
        "source": "maxSimDotProduct(params.query_vector, 'my_vector')",
        "params": {
          "query_vector": [[0.5, 10, 6], [-0.5, 10, 10]] <1>
        }
      }
    }
  }
}
```

1. The query is itself a list of vectors — two here. For each of them the best dot product against any of the document's vectors is taken, and those two maxima are summed into the final score. A query with a single vector is written as `[[0.5, 10, 6]]`.

Additionally, asymmetric similarity functions can be used to score against `bit` vectors. For example, the following query scores documents based on the maxSimDotProduct similarity between a floating point query vector and bit vectors stored in the `my_vector` field:

```console
PUT my-rank-vectors-bit
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "rank_vectors",
        "element_type": "bit"
      }
    }
  }
}

POST /my-rank-vectors-bit/_bulk?refresh
{"index": {"_id" : "1"}}
{"my_vector": [127, -127, 0, 1, 42]}
{"index": {"_id" : "2"}}
{"my_vector": "8100012a7f"}

GET my-rank-vectors-bit/_search
{
  "query": {
    "script_score": {
      "query": {
        "match_all": {}
      },
      "script": {
        "source": "maxSimDotProduct(params.query_vector, 'my_vector')",
        "params": {
          "query_vector": [
            [0.35, 0.77, 0.95, 0.15, 0.11, 0.08, 0.58, 0.06, 0.44, 0.52, 0.21,
       0.62, 0.65, 0.16, 0.64, 0.39, 0.93, 0.06, 0.93, 0.31, 0.92, 0.0,
       0.66, 0.86, 0.92, 0.03, 0.81, 0.31, 0.2 , 0.92, 0.95, 0.64, 0.19,
       0.26, 0.77, 0.64, 0.78, 0.32, 0.97, 0.84]
           ] <1>
        }
      }
    }
  }
}
```

1. Each query vector has 40 elements, matching the number of bits in the bit vectors. As above, more than one query vector can be supplied; every vector in the list must have the same number of elements.

### Available max-sim functions [rank-vectors-functions]

Both functions take a list of query vectors and reduce them to a single score: for each query vector they take the best match among all of the document's vectors, then sum those per-query-vector maxima.

$$$rank-vectors-max-sim-dot-product$$$

`maxSimDotProduct(query_vector, field)`
:   Sums, over each query vector, the highest dot product between that query vector and any of the document's vectors. Supported for every `element_type`.

$$$rank-vectors-max-sim-inv-hamming$$$

`maxSimInvHamming(query_vector, field)`
:   Sums, over each query vector, the highest bitwise similarity between that query vector and any of the document's vectors, where the similarity of a pair is `(bit_count - hamming_distance) / bit_count`. Each term is in the range `[0, 1]`, so the score is in the range `[0, number_of_query_vectors]` and is never negative.

    Only supported for the `byte` and `bit` element types; calling it on a `float` or `bfloat16` field returns an error. The query vector can be given either as arrays of byte values or as hex-encoded strings.

The following query scores `bit` vectors by inverse Hamming distance, using two hex-encoded query vectors. Each string must decode to the same number of bytes as the document vectors:

```console
GET my-rank-vectors-bit/_search
{
  "query": {
    "script_score": {
      "query": {
        "match_all": {}
      },
      "script": {
        "source": "maxSimInvHamming(params.query_vector, 'my_vector')",
        "params": {
          "query_vector": ["8100012a7f", "0102010101"] <1>
        }
      }
    }
  }
}
```

1. Two query vectors of five bytes each, matching the 40 bits of the document vectors. Because the per-query-vector maxima are summed, a score from two query vectors can exceed `1.0` — the maximum here is `2.0`.

### Scoring behavior to be aware of [rank-vectors-scoring-behavior]

Documents that have no value for the field cause an error
:   Scoring a document whose `rank_vectors` field is missing (or was indexed as `null`) fails the request with `A document doesn't have a value for a multi-vector field!` rather than scoring `0`. This matches `dense_vector` behavior. If some documents may not have a vector, restrict the inner query so that only documents with a value are scored, for example by combining it with an [`exists` query](/reference/query-languages/query-dsl/query-dsl-exists-query.md) on the field.

`maxSimDotProduct` can be negative
:   Dot products are unbounded in both directions, so `maxSimDotProduct` may return a negative value — but [`script_score` cannot produce a negative final score](/reference/query-languages/query-dsl/query-dsl-script-score-query.md). Map the value into a non-negative range before returning it, for example with `Math.max(0, maxSimDotProduct(params.query_vector, 'my_vector'))`, or by adding a constant offset large enough for your vectors. `maxSimInvHamming` is always non-negative and needs no such handling.
