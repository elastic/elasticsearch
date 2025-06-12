---
navigation_title: "Rank Vectors"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rank-vectors.html
  # That link will 404 until 8.18 is current
  # (see https://www.elastic.co/guide/en/elasticsearch/reference/8.18/rank-vectors.html)
---

# Rank Vectors [rank-vectors]


::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


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

In addition to the `float` element type, `byte` and `bit` element types are also supported.

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
:   (Optional, string) The data type used to encode vectors. The supported data types are `float` (default), `byte`, and bit.

::::{dropdown} Valid values for element_type
`float`
:   indexes a 4-byte floating-point value per dimension. This is the default value.

`byte`
:   indexes a 1-byte integer value per dimension.

`bit`
:   indexes a single bit per dimension. Useful for very high-dimensional vectors or models that specifically support bit vectors. NOTE: when using `bit`, the number of dimensions must be a multiple of 8 and must represent the number of bits.

::::


`dims`
:   (Optional, integer) Number of vector dimensions. Can’t exceed `4096`. If `dims` is not specified, it will be set to the length of the first vector added to the field.


## Synthetic `_source` [rank-vectors-synthetic-source]

`rank_vectors` fields support [synthetic `_source`](mapping-source-field.md#synthetic-source) .


## Scoring with rank vectors [rank-vectors-scoring]

Rank vectors can be accessed and used in [`script_score` queries](/reference/query-languages/query-dsl/query-dsl-script-score-query.md).

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
          "query_vector": [[0.5, 10, 6], [-0.5, 10, 10]]
        }
      }
    }
  }
}
```

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

1. Note that the query vector has 40 elements, matching the number of bits in the bit vectors.



