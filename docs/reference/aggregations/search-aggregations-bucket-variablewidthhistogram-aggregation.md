---
navigation_title: "Variable width histogram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-variablewidthhistogram-aggregation.html
---

# Variable width histogram aggregation [search-aggregations-bucket-variablewidthhistogram-aggregation]


This is a multi-bucket aggregation similar to [Histogram](/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md). However, the width of each bucket is not specified. Rather, a target number of buckets is provided and bucket intervals are dynamically determined based on the document distribution. This is done using a simple one-pass document clustering algorithm that aims to obtain low distances between bucket centroids. Unlike other multi-bucket aggregations, the intervals will not necessarily have a uniform width.

::::{tip}
The number of buckets returned will always be less than or equal to the target number.
::::


Requesting a target of 2 buckets.

```console
POST /sales/_search?size=0
{
  "aggs": {
    "prices": {
      "variable_width_histogram": {
        "field": "price",
        "buckets": 2
      }
    }
  }
}
```

Response:

```console-result
{
  ...
  "aggregations": {
    "prices": {
      "buckets": [
        {
          "min": 10.0,
          "key": 30.0,
          "max": 50.0,
          "doc_count": 2
        },
        {
          "min": 150.0,
          "key": 185.0,
          "max": 200.0,
          "doc_count": 5
        }
      ]
    }
  }
}
```

::::{important}
This aggregation cannot currently be nested under any aggregation that collects from more than a single bucket.
::::


## Clustering Algorithm [_clustering_algorithm]

Each shard fetches the first `initial_buffer` documents and stores them in memory. Once the buffer is full, these documents are sorted and linearly separated into `3/4 * shard_size buckets`. Next each remaining documents is either collected into the nearest bucket, or placed into a new bucket if it is distant from all the existing ones. At most `shard_size` total buckets are created.

In the reduce step, the coordinating node sorts the buckets from all shards by their centroids. Then, the two buckets with the nearest centroids are repeatedly merged until the target number of buckets is achieved. This merging procedure is a form of [agglomerative hierarchical clustering](https://en.wikipedia.org/wiki/Hierarchical_clustering).

::::{tip}
A shard can return fewer than `shard_size` buckets, but it cannot return more.
::::



## Shard size [_shard_size_3]

The `shard_size` parameter specifies the number of buckets that the coordinating node will request from each shard. A higher `shard_size` leads each shard to produce smaller buckets. This reduces the likelihood of buckets overlapping after the reduction step. Increasing the `shard_size` will improve the accuracy of the histogram, but it will also make it more expensive to compute the final result because bigger priority queues will have to be managed on a shard level, and the data transfers between the nodes and the client will be larger.

::::{tip}
Parameters `buckets`, `shard_size`, and `initial_buffer` are optional. By default, `buckets = 10`, `shard_size = buckets * 50`, and `initial_buffer = min(10 * shard_size, 50000)`.
::::



## Initial Buffer [_initial_buffer]

The `initial_buffer` parameter can be used to specify the number of individual documents that will be stored in memory on a shard before the initial bucketing algorithm is run. Bucket distribution is determined using this sample of `initial_buffer` documents. So, although a higher `initial_buffer` will use more memory, it will lead to more representative clusters.


## Bucket bounds are approximate [_bucket_bounds_are_approximate]

During the reduce step, the master node continuously merges the two buckets with the nearest centroids. If two buckets have overlapping bounds but distant centroids, then it is possible that they will not be merged. Because of this, after reduction the maximum value in some interval (`max`) might be greater than the minimum value in the subsequent bucket (`min`). To reduce the impact of this error, when such an overlap occurs the bound between these intervals is adjusted to be `(max + min) / 2`.

::::{tip}
Bucket bounds are very sensitive to outliers
::::



