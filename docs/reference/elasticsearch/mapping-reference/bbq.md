---
navigation_title: Better Binary Quantization (BBQ)
applies_to:
  stack: all
  serverless: all
---

# Better Binary Quantization (BBQ) [bbq]

Better Binary Quantization (BBQ) is an advanced vector quantization method, designed for large-scale similarity search. BBQ is a form of lossy compression for [`dense_vector` fields](elasticsearch://reference/elasticsearch/mapping-reference/dense-vector.md) that enables efficient storage and retrieval of large numbers of vectors, while keeping results close to those from the original uncompressed vectors.

BBQ offers significant improvements over scalar quantization by relying on optimized `bit` level computations to reduce memory usage and computational costs while maintaining high search relevance using pre-computed corrective factors. BBQ is designed to work in combination with [oversampling](#bbq-oversampling) and reranking, and is compatible with various [vector search algorithms](#bbq-vector-search-algorithms), such as [HNSW](#bbq-hnsw) and [brute force (flat)](#bbq-flat).

## How BBQ works [bbq-how-it-works]

BBQ retains the original vector’s dimensionality but transforms the datatype of the dimensions from the original `float32` to `bit` effectively compressing each vector by 32x plus an additional 14 bytes of corrective data per vector. BBQ uses these pre-computed corrective factors as partial distance calculations to help realize impressively robust approximations of the original vector.

Measuring vector similarity with BBQ vectors requires much less computing effort, allowing more candidates to be considered when using the HNSW algorithm. This often results in better ranking quality and improved relevance compared to the original `float32` vectors.

## Supported vector search algorithms [bbq-vector-search-algorithms]

BBQ currently supports two vector search algorithms, each suited to different scenarios. You can configure them by setting the dense vector field’s `index_type`.

### `bbq_hnsw` [bbq-hnsw]

When you set a dense vector field’s `index_options` parameter to `type: bbq_hnsw`, {{es}} uses the HNSW algorithm for fast [kNN search](docs-content://solutions/search/vector/knn.md) on compressed vectors. With the default [oversampling](#bbq-oversampling) applied, it delivers better cost efficiency, lower latency, and improved relevance ranking, making it the best choice for large-scale similarity search.

:::{note}
Datasets with less than 384 dimensions may see less accuracy and incur a higher overhead cost related to the corrective factors, but we have observed some production datasets perform well even at fairly low dimensions including [tests on e5-small](https://www.elastic.co/search-labs/blog/better-binary-quantization-lucene-elasticsearch).
:::

The following example creates an index with a `dense_vector` field configured to use the `bbq_hnsw` algorithm.

```console
PUT bbq_hnsw-index
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

To change an existing index to use `bbq_hnsw`, update the field mapping:

```console
PUT bbq_hnsw-index/_mapping
{
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
```

After this change, all newly created segments will use the `bbq_hnsw` algorithm. As you add or update documents, the index will gradually convert to `bbq_hnsw`.

To apply `bbq_hnsw` to all vectors at once, reindex them into a new index where the `index_options` parameter's `type` is set to `bbq_hnsw`:

:::::{stepper}
::::{step} Create a destination index
```console
PUT my-index-bbq
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
::::

::::{step} Reindex the data
```console
POST _reindex
{
  "source": { "index": "my-index" }, <1>
  "dest":   { "index": "my-index-bbq" }
}
```
1. The existing index to be reindexed into the newly created index with the `bbq_hnsw` algorithm.
::::

:::::

### `bbq_flat` [bbq-flat]

When you set a dense vector field’s `index_options` parameter to `type: bbq_flat`, {{es}} uses the BBQ algorithm without HNSW. This option generally requires fewer computing resources and works best when the number of vectors being searched is relatively low.

The following example creates an index with a `dense_vector` field configured to use the `bbq_flat` algorithm.

```console
PUT bbq_flat-index
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 64,
        "index": true,
        "index_options": {
          "type": "bbq_flat"
        }
      }
    }
  }
}
```

### `bbq_disk` [bbq-disk]

```{applies_to}
stack: ga 9.2
```
:::{note}
This feature requires an [Enterprise subscription](https://www.elastic.co/subscriptions).

{applies_to}`stack: ga 9.4` {applies_to}`serverless: ga` `bbq_disk` is the default index type for `dense_vector` when it is available under the current license.
:::

When you set a dense vector field’s `index_options` parameter to `type: bbq_disk`, {{es}} uses the DiskBBQ algorithm, a disk-based alternative to HNSW for [kNN search](docs-content://solutions/search/vector/knn.md) on compressed vectors. DiskBBQ stores the vector data on disk instead of in memory, lowering RAM requirements and reducing the overall cost of vector storage and search.

DiskBBQ groups similar vectors into small clusters using [hierarchical K-means](https://www.elastic.co/search-labs/blog/k-means-for-vector-indices). When processing a query, it finds the centroids closest to the query vector and only compares the vectors within those clusters. This targeted approach reduces the number of in-memory operations, making it ideal for large-scale or memory-constrained environments.

DiskBBQ typically performs well for recall levels up to around 95%. For use cases requiring exceptionally high recall (99% or higher), many vector clusters may need to be visited, which can negatively impact performance. In very high recall cases, [bbq_hnsw](#bbq-hnsw), or other HNSW-based formats deliver better performance depending on memory availability.

The following example creates an index with a `dense_vector` field configured to use the `bbq_disk` algorithm.

```console
PUT bbq_disk-index
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 3,
        "similarity": "l2_norm",
        "index": true,
        "index_options": {
          "type": "bbq_disk"
        }
      }
    }
  }
}
```

To change an existing index to use `bbq_disk`, update the field mapping:

```console
PUT bbq_disk-index/_mapping
{
  "properties": {
    "my_vector": {
      "type": "dense_vector",
      "dims": 64,
      "index": true,
      "index_options": {
        "type": "bbq_disk"
      }
    }
  }
}
```

To apply `bbq_disk` to all vectors at once, reindex them into a new index where the `index_options` parameter's `type` is set to `bbq_disk`:

:::::{stepper}
::::{step} Create a destination index
```console
PUT my-index-bbq
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 64,
        "index": true,
        "index_options": {
          "type": "bbq_disk"
        }
      }
    }
  }
}
```
::::

::::{step} Reindex the data
```console
POST _reindex
{
  "source": { "index": "my-index" }, <1>
  "dest":   { "index": "my-index-bbq" }
}
```
1. The existing index to be reindexed into the newly created index with the `bbq_disk` algorithm.
::::

:::::

You can set the `visit_percentage` parameter to define the fraction of vectors visited per shard during search.

```console
POST bbq_disk-index/_search
{
  "query": {
    "knn": {
      "field": "my_vector",
      "query_vector": [0.0127, 0.1230, 0.3929],
      "k": 10,
      "visit_percentage": 10.0
    }
  }
}
```

A lower `visit_percentage` can further reduce memory use and speed up queries, while a higher value can improve recall. Learn more about [top-level parameters for knn](/reference/query-languages/query-dsl/query-dsl-knn-query.md#knn-query-top-level-parameters) queries.

#### Quantize bits [bbq-bits]
```{applies_to}
stack: ga 9.4
```
By default, BBQ performs asymmetric quantization: it performs 1-bit quantization for the indexed vectors and 4-bit quantization for query vectors.
For fields of type `bbq_disk` it is possible to change the level of quantization for indexed vectors by setting the `bits` parameter in `index_options` to `1` (default), `2`, `4` or `7`.

If no `oversampling_factor` is specified, setting `bits` will automatically adjust that as follows:
* `bits = 1` --> `oversampling_factor = 3.0`
* `bits = 2` --> `oversampling_factor = 1.5`
* `bits = 4` --> `oversampling_factor = 0` (no oversampling)
* `bits = 7` --> `oversampling_factor = 0` (no oversampling)

```console
PUT bbq_disk-index/_mapping
{
  "properties": {
    "my_vector": {
      "type": "dense_vector",
      "dims": 64,
      "index": true,
      "index_options": {
        "type": "bbq_disk",
        "bits": 2
      }
    }
  }
}
```

Typically, increasing the number of bits used for indexed vectors is useful in cases where higher fidelity is required for knn search. Note that increasing the number of bits used for indexed vectors will result in a slight reduction in speed and increase in disk space usage.

This setting can be changed at any time without reindexing the data.


## Auto-calibration for `bbq_disk` [bbq-auto-calibration]

```{applies_to}
stack: ga 9.5
```

When auto-calibration is enabled for a `bbq_disk` field, {{es}} automatically determines the best quantization encoding, oversampling (rerank depth), and preconditioning for each merged segment.
Rather than applying the same fixed encoding to every segment, auto-calibration runs a recall-estimation pipeline on a representative sample of the merged corpus and selects the cheapest configuration that meets a 90% recall target at k=10.

You control auto-calibration with the `auto_calibrate` parameter in `index_options`:

- {applies_to}`stack: ga 9.6+` Auto-calibration is enabled by default, using the `iso_sizing` profile, for `bbq_disk` fields in indices created in 9.6 or later.
  To opt out, set `auto_calibrate` to `false`.
  Indices created before 9.6 keep the previous default, which is disabled.
- {applies_to}`stack: ga =9.5` Auto-calibration is disabled by default.
  Set `auto_calibrate` to `true` to enable it.

### How it works

At merge time, for each merge producing at least 10,000 vectors, {{es}} samples up to 17,000 vectors from the merged corpus and sweeps candidate configurations in ascending estimated cost order. For each candidate, it predicts the expected recall at k=10, and picks the cheapest option that meets the 90% target. If no candidate meets the target, the one with the best estimated recall is used instead.

The sweep covers:

- Five quantization encodings (1/1, 1/4, 2/4, 4/4, and 7/7 doc/query bits)
- Six rerank-depth multipliers (1.25×, 1.5×, 1.75×, 2.0×, 2.5×, and 3.0×)
- Preconditioned and non-preconditioned variants of each

If calibration fails for any reason, {{es}} falls back to the codec default encoding for that segment.

:::{note}
{applies_to}`stack: ga 9.6+` The encodings that are considered depend on the [auto-calibration profile](#bbq-auto-calibration-profiles).
:::

### Auto-calibration profiles [bbq-auto-calibration-profiles]

```{applies_to}
stack: ga 9.6
```

A profile limits which candidate configurations the calibration sweep can choose from.
Set the profile with the `auto_calibrate` parameter:

`iso_sizing`
:   Only considers encodings that use 1 bit per document dimension (1/1 and 1/4 doc/query bits).
    The index stays the same size as an uncalibrated `bbq_disk` index, while calibration still tunes the query bits, rerank depth, and preconditioning.
    This is the default profile.

`quality`
:   Considers all encodings.
    Calibration can select more bits per document dimension, which can improve recall at the cost of additional disk space and memory usage.

`disabled`
:   Turns auto-calibration off.
    Equivalent to `false`.

Boolean values are also accepted.
`false` is the same as `disabled`.
`true` selects the default profile for the index, which is:

- `iso_sizing` for indices created in 9.6 or later
- `quality` for indices created in 9.5

:::{note}
You can change the profile on an existing field.
Refer to [Changing `auto_calibrate` on an existing field](#bbq-auto-calibration-update).
:::

### Segments below the calibration threshold

Segments containing fewer than 10,000 vectors after merging are not calibrated. These segments use a default oversampling factor of 3.0× with the encoding determined by the `bits` setting.

### Interaction with other parameters

- **`bits`**: Controls the encoding at flush time for freshly written, pre-merge segments. Auto-calibration operates independently at merge time and may select a different encoding for merged segments.
- **`precondition`**: The calibration sweep always evaluates both preconditioned and non-preconditioned variants and selects whichever achieves better estimated recall per unit cost. The `precondition` mapping parameter controls the encoding applied at flush time and serves as the fallback default if calibration fails. Cannot be changed after the field is created.
- **`rescore_vector.oversample`**: For calibrated merged segments, the oversampling factor is selected automatically and stored per segment. At query time, the effective oversample is resolved in this order:
  1. Query-time `rescore_vector.oversample` on the kNN query or retriever, if set. This value is applied uniformly to all segments and overrides calibrated per-segment values.
  2. Per-segment calibrated oversample, when the segment was calibrated at merge time. Different segments in the same index may use different factors.
  3. Mapping `rescore_vector.oversample`, or the default 3.0× if unset. This applies to unmerged segments, segments below the calibration threshold, and segments where calibration fell back to codec defaults.

     Query-time oversample affects rerank depth only. Preconditioning and quantization encoding continue to follow each segment's stored configuration.

### Changing `auto_calibrate` on an existing field [bbq-auto-calibration-update]

```{applies_to}
stack: ga 9.6
```

You can turn auto-calibration on or off, or switch to a different [profile](#bbq-auto-calibration-profiles), on an existing `bbq_disk` field with the [update mapping API]({{es-apis}}operation/operation-indices-put-mapping).
Existing segments are not rewritten, so the new value applies from the next merge:

- If you turn it on by setting `auto_calibrate` to `true`, `iso_sizing`, or `quality`, later merges that produce at least 10,000 vectors are calibrated.
  Segments written before the change are calibrated when they are merged.
- If you turn it off, later merges use the mapping settings.
  When you search, segments no longer use their calibrated oversampling factor. They use the query-time `rescore_vector.oversample` if set, otherwise the mapping value (default 3.0).
  Encoding and preconditioning are properties of how each segment was written, so a segment that was calibrated continues to be searched with its calibrated encoding and preconditioning until it is merged again.
- If you switch profiles, for example from `iso_sizing` to `quality`, later calibrated merges choose from the new profile's candidate configurations.
  Segments that were already calibrated keep their stored encoding, oversampling, and preconditioning, and are searched with those values until they are merged again.
  The index can contain segments calibrated under different profiles.
  Switching to `quality` can increase disk and memory usage as segments are merged.

### Examples

The following examples show how to configure auto-calibration and inspect the result.

#### Enable or disable auto-calibration

To explicitly enable auto-calibration:

```console
PUT my-auto-calibrated-index
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 1024,
        "index": true,
        "index_options": {
          "type": "bbq_disk",
          "auto_calibrate": true
        }
      }
    }
  }
}
```

To explicitly disable it:

```console
PUT my-auto-calibrated-index
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 1024,
        "index": true,
        "index_options": {
          "type": "bbq_disk",
          "auto_calibrate": false
        }
      }
    }
  }
}
```

#### Using a specific profile

```{applies_to}
stack: ga 9.6
```

```console
PUT my-quality-calibrated-index
{
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "dense_vector",
        "dims": 1024,
        "index": true,
        "index_options": {
          "type": "bbq_disk",
          "auto_calibrate": "quality" <1>
        }
      }
    }
  }
}
```
1. Set the `auto_calibrate` parameter value to the [profile name](#bbq-auto-calibration-profiles).

#### Getting the active configuration

```{applies_to}
stack: ga 9.6
```

You can inspect the active configuration for the index using the [index statistics API]({{es-apis}}operation/operation-indices-stats).
Set the `include_auto_calibration` query parameter to `true`. The details are returned only when `level` is set to `indices` (default) or `shards`.

```console
GET my-auto-calibrated-index/_stats?include_auto_calibration=true
```

The following abbreviated response groups the segments of the `my_vector` field by parameter set:

```console-result
{
  ...
  "indices": {
    "my-auto-calibrated-index": {
      ...
      "primaries": {
        "dense_vector": {
          "value_count": 104500,
          "off_heap": {
            ...
            "fielddata": {
              "my_vector": { <1>
                ...
                "auto_calibration": [ <2>
                  {
                    "calibrated": true, <3>
                    "type": "osq", <4>
                    "number_of_vectors": 55500, <5>
                    "size_in_bytes": 233100000, <6>
                    "number_of_segments": 2, <7>
                    "parameters": { <8>
                      "bits": 1,
                      "query_bits": 4,
                      "precondition": false,
                      "oversample": 3.0
                    }
                  },
                  {
                    "calibrated": true,
                    "type": "osq",
                    "number_of_vectors": 34500,
                    "size_in_bytes": 145000000,
                    "number_of_segments": 1,
                    "parameters": {
                      "bits": 1,
                      "query_bits": 4,
                      "precondition": true,
                      "oversample": 2.0
                    }
                  },
                  {
                    "calibrated": false, <9>
                    "type": "osq",
                    "number_of_vectors": 14500,
                    "size_in_bytes": 60040000,
                    "number_of_segments": 2
                  }
                ]
              }
            }
          }
        }
      }
    }
  }
}
```

1. One entry exists for each DiskBBQ `dense_vector` field in the index, keyed by field name.
2. Each `auto_calibration` entry represents one unique set of auto-calibration parameters.
3. `calibrated` reports whether the segments were calibrated when they were merged.
4. `type` identifies the kind of calibration parameters returned. The only supported value is `osq` (optimized scalar quantization).
5. `number_of_segments` is the number of segments auto-calibrated with this parameter set.
6. `number_of_vectors` is the total number of vectors in the segments.
7. `size_in_bytes` is the on-disk size of this field's vector data in the segments.
8. `parameters` reports the results of auto-calibration: for `osq` it shows the encoding (`bits` and `query_bits`), whether preconditioning is applied, and the oversampling factor.
   Refer to [Interaction with other parameters](#interaction-with-other-parameters) for how these values interact with query-time and mapping `oversample` values.
9. Segments of this field that were not calibrated.
   No `parameters` are returned.
   The segments are searched using the mapping settings.
   A query-time [`rescore_vector.oversample`](/reference/query-languages/query-dsl/query-dsl-knn-query.md#knn-query-top-level-parameters) on the kNN query or retriever overrides the oversampling factor, as described in [Interaction with other parameters](#interaction-with-other-parameters).

To see the details of each segment individually, use the [index segments API]({{es-apis}}operation/operation-indices-segments) with the `include_auto_calibration` parameter set to `true`.

```console
GET my-auto-calibrated-index/_segments?include_auto_calibration=true
```

The following abbreviated response shows two segments of the same shard:

```console-result
{
  ...
  "indices": {
    "my-auto-calibrated-index": {
      "shards": {
        "0": [
          {
            ...
            "segments": {
              "_a": {
                ...
                "auto_calibration": {
                  "my_vector": { <1>
                    "calibrated": true, <2>
                    "type": "osq", <3>
                    "number_of_vectors": 250000, <4>
                    "size_in_bytes": 145000000, <5>
                    "parameters": { <6>
                      "bits": 2,
                      "query_bits": 4,
                      "precondition": true,
                      "oversample": 2.5
                    }
                  }
                }
              },
              "_b": {
                ...
                "auto_calibration": {
                  "my_vector": {
                    "calibrated": false, <7>
                    "type": "osq",
                    "number_of_vectors": 4200,
                    "size_in_bytes": 2400000
                  }
                }
              }
            }
          }
        ]
      }
    }
  }
}
```

1. One entry exists for each DiskBBQ `dense_vector` field in the segment, keyed by field name.
2. `calibrated` reports if the segment was calibrated when it was merged.
3. `type` identifies the kind of calibration parameters returned. It is currently always `osq` (optimized scalar quantization).
4. `number_of_vectors` is the total number of vectors in the segment.
5. `size_in_bytes` is the on-disk size of this field's vector data in the segment.
5. `parameters` reports the results of auto-calibration: for `osq` it shows the encoding (`bits` and `query_bits`), whether preconditioning is applied, and the oversampling factor.
   Refer to [Interaction with other parameters](#interaction-with-other-parameters) for how these values interact with query-time and mapping `oversample` values.
6. An uncalibrated `dense_vector` field in a segment.
   No `parameters` are returned.
   The segment is searched using the mapping settings.
   A query-time [`rescore_vector.oversample`](/reference/query-languages/query-dsl/query-dsl-knn-query.md#knn-query-top-level-parameters) on the kNN query or retriever overrides the oversampling factor, as described in [Interaction with other parameters](#interaction-with-other-parameters).

## Oversampling [bbq-oversampling]

Oversampling is a technique used with BBQ searches to reduce the accuracy loss from compression. Compression lowers the memory footprint by over 95% and improves query latency, at the cost of decreased result accuracy. This decrease can be mitigated by oversampling during query time and reranking the top results using the full vector.

When you run a kNN search on a BBQ-indexed field, {{es}} automatically retrieves more candidate vectors than the number of results you request. This oversampling improves accuracy by giving the system more vectors to re-rank using their full-precision values before returning the top results.

```console
GET bbq-index/_search
{
  "knn": {
    "field": "my_vector",
    "query_vector": [0.12, -0.45, ...],
    "k": 10,
    "num_candidates": 100
  }
}
```

By default, oversampling is set to 3×, meaning if you request k:10, {{es}} retrieves 30 candidates for re-ranking. You don’t need to configure this behavior; it’s applied automatically for BBQ searches.

:::{note}
You can change oversampling from the default 3× to another value. Refer to [Oversampling and rescoring for quantized vectors](docs-content://solutions/search/vector/knn/optimize-performance-accuracy.md#dense-vector-knn-search-rescoring) for details.
:::

## Learn more [bbq-learn-more]

- [Better Binary Quantization (BBQ) in Lucene and {{es}}](https://www.elastic.co/search-labs/blog/better-binary-quantization-lucene-elasticsearch) - Learn how BBQ works, its benefits, and how it reduces memory usage while preserving search accuracy.
- [Introducing a new vector storage format: DiskBBQ](https://www.elastic.co/search-labs/blog/diskbbq-elasticsearch-introduction) - Learn how DiskBBQ improves vector search in low-memory environments and compares to HNSW in speed and cost-effectiveness.
- [Dense vector field type](elasticsearch://reference/elasticsearch/mapping-reference/dense-vector.md) - Find code examples for using `bbq_hnsw` `index_type`.
- [kNN search](docs-content://solutions/search/vector/knn.md) - Learn about the search algorithm that BBQ works with.
