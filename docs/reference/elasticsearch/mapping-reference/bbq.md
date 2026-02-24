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
Starting in version 9.1, `bbq_hnsw` is the default indexing method for new `dense_vector` fields with greater than 384 dimensions, so you typically don’t need to specify it explicitly when creating an index.

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
For fields of type `bbq_disk` it is possible to change the level of quantization for indexed vectors by setting the `bits` parameter in `index_options` to `1` (default), `2` or `4`.

If no `oversampling_factor` is specified, setting `bits` will automatically adjust that as follows:
* `bits = 1` --> `oversampling_factor = 3.0`
* `bits = 2` --> `oversampling_factor = 1.5`
* `bits = 4` --> `oversampling_factor = 0` (no oversampling)

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
        "bits": 2
      }
    }
  }
}
```

Typically, increasing the number of bits used for indexed vectors is useful in cases where higher fidelity is required for knn search. Note that increasing the number of bits used for indexed vectors will result in a slight reduction in speed and increase in disk space usage.

This setting can be changed at any time without reindexing the data.


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
You can change oversampling from the default 3× to another value. Refer to [Oversampling and rescoring for quantized vectors](docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring) for details.
:::

## Learn more [bbq-learn-more]

- [Better Binary Quantization (BBQ) in Lucene and {{es}}](https://www.elastic.co/search-labs/blog/better-binary-quantization-lucene-elasticsearch) - Learn how BBQ works, its benefits, and how it reduces memory usage while preserving search accuracy.
- [Introducing a new vector storage format: DiskBBQ](https://www.elastic.co/search-labs/blog/diskbbq-elasticsearch-introduction) - Learn how DiskBBQ improves vector search in low-memory environments and compares to HNSW in speed and cost-effectiveness.
- [Dense vector field type](elasticsearch://reference/elasticsearch/mapping-reference/dense-vector.md) - Find code examples for using `bbq_hnsw` `index_type`.
- [kNN search](docs-content://solutions/search/vector/knn.md) - Learn about the search algorithm that BBQ works with.
