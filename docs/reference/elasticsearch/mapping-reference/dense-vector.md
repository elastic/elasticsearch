---
navigation_title: "Dense vector"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html
---

# Dense vector field type [dense-vector]

The `dense_vector` field type stores dense vectors of numeric values. Dense vector fields are primarily used for [k-nearest neighbor (kNN) search](docs-content://deploy-manage/production-guidance/optimize-performance/approximate-knn-search.md).

The `dense_vector` type does not support aggregations or sorting.

You add a `dense_vector` field as an array of numeric values based on [`element_type`](#dense-vector-params) with `float` by default:

```console
PUT my-index
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 3
      },
      "my_text" : {
        "type" : "keyword"
      }
    }
  }
}

PUT my-index/_doc/1
{
  "my_text" : "text1",
  "my_vector" : [0.5, 10, 6]
}

PUT my-index/_doc/2
{
  "my_text" : "text2",
  "my_vector" : [-0.5, 10, 10]
}
```

::::{note}
Unlike most other data types, dense vectors are always single-valued. It is not possible to store multiple values in one `dense_vector` field.
::::

## Index vectors for kNN search [index-vectors-knn-search]

A *k-nearest neighbor* (kNN) search finds the *k* nearest vectors to a query vector, as measured by a similarity metric.

Dense vector fields can be used to rank documents in [`script_score` queries](/reference/query-languages/query-dsl/query-dsl-script-score-query.md). This lets you perform a brute-force kNN search by scanning all documents and ranking them by similarity.

In many cases, a brute-force kNN search is not efficient enough. For this reason, the `dense_vector` type supports indexing vectors into a specialized data structure to support fast kNN retrieval through the [`knn` option](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) in the search API

Unmapped array fields of float elements with size between 128 and 4096 are dynamically mapped as `dense_vector` with a default similariy of `cosine`. You can override the default similarity by explicitly mapping the field as `dense_vector` with the desired similarity.

Indexing is enabled by default for dense vector fields and indexed as `int8_hnsw`. When indexing is enabled, you can define the vector similarity to use in kNN search:

```console
PUT my-index-2
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 3,
        "similarity": "dot_product"
      }
    }
  }
}
```

::::{note}
Indexing vectors for approximate kNN search is an expensive process. It can take substantial time to ingest documents that contain vector fields with `index` enabled. See [k-nearest neighbor (kNN) search](docs-content://deploy-manage/production-guidance/optimize-performance/approximate-knn-search.md) to learn more about the memory requirements.
::::

You can disable indexing by setting the `index` parameter to `false`:

```console
PUT my-index-2
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 3,
        "index": false
      }
    }
  }
}
```

{{es}} uses the [HNSW algorithm](https://arxiv.org/abs/1603.09320) to support efficient kNN search. Like most kNN algorithms, HNSW is an approximate method that sacrifices result accuracy for improved speed.

## Automatically quantize vectors for kNN search [dense-vector-quantization]

The `dense_vector` type supports quantization to reduce the memory footprint required when [searching](docs-content://solutions/search/vector/knn.md#approximate-knn) `float` vectors. The three following quantization strategies are supported:

* `int8` - Quantizes each dimension of the vector to 1-byte integers. This reduces the memory footprint by 75% (or 4x) at the cost of some accuracy.
* `int4` - Quantizes each dimension of the vector to half-byte integers. This reduces the memory footprint by 87% (or 8x) at the cost of accuracy.
* `bbq` - Better binary quantization which reduces each dimension to a single bit precision. This reduces the memory footprint by 96% (or 32x) at a larger cost of accuracy. Generally, oversampling during query time and reranking can help mitigate the accuracy loss.

When using a quantized format, you may want to oversample and rescore the results to improve accuracy. See [oversampling and rescoring](docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring) for more information.

To use a quantized index, you can set your index type to `int8_hnsw`, `int4_hnsw`, or `bbq_hnsw`. When indexing `float` vectors, the current default index type is `int8_hnsw`.

Quantized vectors can use [oversampling and rescoring](docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring) to improve accuracy on approximate kNN search results.

::::{note}
Quantization will continue to keep the raw float vector values on disk for reranking, reindexing, and quantization improvements over the lifetime of the data. This means disk usage will increase by ~25% for `int8`, ~12.5% for `int4`, and ~3.1% for `bbq` due to the overhead of storing the quantized and raw vectors.
::::

::::{note}
`int4` quantization requires an even number of vector dimensions.
::::

::::{note}
`bbq` quantization only supports vector dimensions that are greater than 64.
::::

Here is an example of how to create a byte-quantized index:

```console
PUT my-byte-quantized-index
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 3,
        "index": true,
        "index_options": {
          "type": "int8_hnsw"
        }
      }
    }
  }
}
```

Here is an example of how to create a half-byte-quantized index:

```console
PUT my-byte-quantized-index
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 4,
        "index": true,
        "index_options": {
          "type": "int4_hnsw"
        }
      }
    }
  }
}
```

Here is an example of how to create a binary quantized index:

```console
PUT my-byte-quantized-index
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 64,
        "index": true,
        "index_options": {
          "type": "bbq_hnsw"
        }
      }
    }
  }
}
```

## Parameters for dense vector fields [dense-vector-params]

The following mapping parameters are accepted:

$$$dense-vector-element-type$$$

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

`index`
:   (Optional, Boolean) If `true`, you can search this field using the [knn query](/reference/query-languages/query-dsl/query-dsl-knn-query.md) or [knn in _search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) . Defaults to `true`.

$$$dense-vector-similarity$$$

`similarity`
:   (Optional[¹](#footnote-1), string) The vector similarity metric to use in kNN search. Documents are ranked by their vector field’s similarity to the query vector. The `_score` of each document will be derived from the similarity, in a way that ensures scores are positive and that a larger score corresponds to a higher ranking. Defaults to `l2_norm` when `element_type: bit` otherwise defaults to `cosine`.

    ¹ $$$footnote-1$$$ This parameter can only be specified when `index` is `true`.

    ::::{note}
    `bit` vectors only support `l2_norm` as their similarity metric.
    ::::

::::{dropdown} Valid values for similarity
`l2_norm`
:   Computes similarity based on the L2 distance (also known as Euclidean distance) between the vectors. The document `_score` is computed as `1 / (1 + l2_norm(query, vector)^2)`.

For `bit` vectors, instead of using `l2_norm`, the `hamming` distance between the vectors is used. The `_score` transformation is `(numBits - hamming(a, b)) / numBits`

`dot_product`
:   Computes the dot product of two unit vectors. This option provides an optimized way to perform cosine similarity. The constraints and computed score are defined by `element_type`.

    When `element_type` is `float`, all vectors must be unit length, including both document and query vectors. The document `_score` is computed as `(1 + dot_product(query, vector)) / 2`.

    When `element_type` is `byte`, all vectors must have the same length including both document and query vectors or results will be inaccurate. The document `_score` is computed as `0.5 + (dot_product(query, vector) / (32768 * dims))` where `dims` is the number of dimensions per vector.

`cosine`
:   Computes the cosine similarity. During indexing {{es}} automatically normalizes vectors with `cosine` similarity to unit length. This allows to internally use `dot_product` for computing similarity, which is more efficient. Original un-normalized vectors can be still accessed through scripts. The document `_score` is computed as `(1 + cosine(query, vector)) / 2`. The `cosine` similarity does not allow vectors with zero magnitude, since cosine is not defined in this case.

`max_inner_product`
:   Computes the maximum inner product of two vectors. This is similar to `dot_product`, but doesn’t require vectors to be normalized. This means that each vector’s magnitude can significantly effect the score. The document `_score` is adjusted to prevent negative values. For `max_inner_product` values `< 0`, the `_score` is `1 / (1 + -1 * max_inner_product(query, vector))`. For non-negative `max_inner_product` results the `_score` is calculated `max_inner_product(query, vector) + 1`.

::::

::::{note}
Although they are conceptually related, the `similarity` parameter is different from [`text`](/reference/elasticsearch/mapping-reference/text.md) field [`similarity`](/reference/elasticsearch/mapping-reference/similarity.md) and accepts a distinct set of options.
::::

$$$dense-vector-index-options$$$

`index_options`
:   (Optional[²](#footnote-2), object) An optional section that configures the kNN indexing algorithm. The HNSW algorithm has two internal parameters that influence how the data structure is built. These can be adjusted to improve the accuracy of results, at the expense of slower indexing speed.

    ² $$$footnote-2$$$ This parameter can only be specified when `index` is `true`.

::::{dropdown} Properties of index_options
`type`
:   (Required, string) The type of kNN algorithm to use. Can be either any of:
    * `hnsw` - This utilizes the [HNSW algorithm](https://arxiv.org/abs/1603.09320) for scalable approximate kNN search. This supports all `element_type` values.
    * `int8_hnsw` - The default index type for float vectors. This utilizes the [HNSW algorithm](https://arxiv.org/abs/1603.09320) in addition to automatically scalar quantization for scalable approximate kNN search with `element_type` of `float`. This can reduce the memory footprint by 4x at the cost of some accuracy. See [Automatically quantize vectors for kNN search](#dense-vector-quantization).
    * `int4_hnsw` - This utilizes the [HNSW algorithm](https://arxiv.org/abs/1603.09320) in addition to automatically scalar quantization for scalable approximate kNN search with `element_type` of `float`. This can reduce the memory footprint by 8x at the cost of some accuracy. See [Automatically quantize vectors for kNN search](#dense-vector-quantization).
    * `bbq_hnsw` - This utilizes the [HNSW algorithm](https://arxiv.org/abs/1603.09320) in addition to automatically binary quantization for scalable approximate kNN search with `element_type` of `float`. This can reduce the memory footprint by 32x at the cost of accuracy. See [Automatically quantize vectors for kNN search](#dense-vector-quantization).
    * `flat` - This utilizes a brute-force search algorithm for exact kNN search. This supports all `element_type` values.
    * `int8_flat` - This utilizes a brute-force search algorithm in addition to automatically scalar quantization. Only supports `element_type` of `float`.
    * `int4_flat` - This utilizes a brute-force search algorithm in addition to automatically half-byte scalar quantization. Only supports `element_type` of `float`.
    * `bbq_flat` - This utilizes a brute-force search algorithm in addition to automatically binary quantization. Only supports `element_type` of `float`.

`m`
:   (Optional, integer) The number of neighbors each node will be connected to in the HNSW graph. Defaults to `16`. Only applicable to `hnsw`, `int8_hnsw`, `int4_hnsw` and `bbq_hnsw` index types.

`ef_construction`
:   (Optional, integer) The number of candidates to track while assembling the list of nearest neighbors for each new node. Defaults to `100`. Only applicable to `hnsw`, `int8_hnsw`, `int4_hnsw` and `bbq_hnsw` index types.

`confidence_interval`
:   (Optional, float) Only applicable to `int8_hnsw`, `int4_hnsw`, `int8_flat`, and `int4_flat` index types. The confidence interval to use when quantizing the vectors. Can be any value between and including `0.90` and `1.0` or exactly `0`. When the value is `0`, this indicates that dynamic quantiles should be calculated for optimized quantization. When between `0.90` and `1.0`, this value restricts the values used when calculating the quantization thresholds. For example, a value of `0.95` will only use the middle 95% of the values when calculating the quantization thresholds (e.g. the highest and lowest 2.5% of values will be ignored). Defaults to `1/(dims + 1)` for `int8` quantized vectors and `0` for `int4` for dynamic quantile calculation.


`rescore_vector`
:   (Optional, object) An optional section that configures automatic vector rescoring on knn queries for the given field. Only applicable to quantized index types.
:::::{dropdown} Properties of rescore_vector
`oversample`
:   (required, float) The amount to oversample the search results by. This value should be greater than `1.0` and less than `10.0` or exactly `0` to indicate no oversampling & rescoring should occur. The higher the value, the more vectors will be gathered and rescored with the raw values per shard.
    :   In case a knn query specifies a `rescore_vector` parameter, the query `rescore_vector` parameter will be used instead.
    :   See [oversampling and rescoring quantized vectors](docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring) for details.
:::::
::::



## Synthetic `_source` [dense-vector-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


`dense_vector` fields support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) .


## Indexing & Searching bit vectors [dense-vector-index-bit]

When using `element_type: bit`, this will treat all vectors as bit vectors. Bit vectors utilize only a single bit per dimension and are internally encoded as bytes. This can be useful for very high-dimensional vectors or models.

When using `bit`, the number of dimensions must be a multiple of 8 and must represent the number of bits. Additionally, with `bit` vectors, the typical vector similarity values are effectively all scored the same, e.g. with `hamming` distance.

Let’s compare two `byte[]` arrays, each representing 40 individual bits.

`[-127, 0, 1, 42, 127]` in bits `1000000100000000000000010010101001111111` `[127, -127, 0, 1, 42]` in bits `0111111110000001000000000000000100101010`

When comparing these two bit, vectors, we first take the [`hamming` distance](https://en.wikipedia.org/wiki/Hamming_distance).

`xor` result:

```
1000000100000000000000010010101001111111
^
0111111110000001000000000000000100101010
=
1111111010000001000000010010101101010101
```

Then, we gather the count of `1` bits in the `xor` result: `18`. To scale for scoring, we subtract from the total number of bits and divide by the total number of bits: `(40 - 18) / 40 = 0.55`. This would be the `_score` betwee these two vectors.

Here is an example of indexing and searching bit vectors:

```console
PUT my-bit-vectors
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 40, <1>
        "element_type": "bit"
      }
    }
  }
}
```

1. The number of dimensions that represents the number of bits


```console
POST /my-bit-vectors/_bulk?refresh
{"index": {"_id" : "1"}}
{"my_vector": [127, -127, 0, 1, 42]} <1>
{"index": {"_id" : "2"}}
{"my_vector": "8100012a7f"} <2>
```
% TEST[continued]

1. 5 bytes representing the 40 bit dimensioned vector
2. A hexidecimal string representing the 40 bit dimensioned vector


Then, when searching, you can use the `knn` query to search for similar bit vectors:

```console
POST /my-bit-vectors/_search?filter_path=hits.hits
{
  "query": {
    "knn": {
      "query_vector": [127, -127, 0, 1, 42],
      "field": "my_vector"
    }
  }
}
```
% TEST[continued]

```console-result
{
    "hits": {
        "hits": [
            {
                "_index": "my-bit-vectors",
                "_id": "1",
                "_score": 1.0,
                "_source": {
                    "my_vector": [
                        127,
                        -127,
                        0,
                        1,
                        42
                    ]
                }
            },
            {
                "_index": "my-bit-vectors",
                "_id": "2",
                "_score": 0.55,
                "_source": {
                    "my_vector": "8100012a7f"
                }
            }
        ]
    }
}
```


## Updatable field type [_updatable_field_type]

To better accommodate scaling and performance needs, updating the `type` setting in `index_options` is possible with the [Update Mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping), according to the following graph (jumps allowed):

```txt
flat --> int8_flat --> int4_flat --> hnsw --> int8_hnsw --> int4_hnsw
```

For updating all HNSW types (`hnsw`, `int8_hnsw`, `int4_hnsw`) the number of connections `m` must either stay the same or increase. For scalar quantized formats  (`int8_flat`, `int4_flat`, `int8_hnsw`, `int4_hnsw`) the `confidence_interval` must always be consistent (once defined, it cannot change).

Updating `type` in `index_options` will fail in all other scenarios.

Switching `types` won’t re-index vectors that have already been indexed (they will keep using their original `type`), vectors being indexed after the change will use the new `type` instead.

For example, it’s possible to define a dense vector field that utilizes the `flat` type (raw float32 arrays) for a first batch of data to be indexed.

```console
PUT my-index-000001
{
    "mappings": {
        "properties": {
            "text_embedding": {
                "type": "dense_vector",
                "dims": 384,
                "index_options": {
                    "type": "flat"
                }
            }
        }
    }
}
```

Changing the `type` to `int4_hnsw` makes sure vectors indexed after the change will use an int4 scalar quantized representation and HNSW (e.g., for KNN queries). That includes new segments created by [merging](/reference/elasticsearch/index-settings/merge.md) previously created segments.

```console
PUT /my-index-000001/_mapping
{
    "properties": {
        "text_embedding": {
            "type": "dense_vector",
            "dims": 384,
            "index_options": {
                "type": "int4_hnsw"
            }
        }
    }
}
```
% TEST[setup:my_index]

Vectors indexed before this change will keep using the `flat` type (raw float32 representation and brute force search for KNN queries).

In order to have all the vectors updated to the new type, either reindexing or force merging should be used.

For debugging purposes, it’s possible to inspect how many segments (and docs) exist for each `type` with the [Index Segments API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-segments).


