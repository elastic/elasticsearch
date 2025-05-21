---
navigation_title: "Median absolute deviation"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-median-absolute-deviation-aggregation.html
---

# Median absolute deviation aggregation [search-aggregations-metrics-median-absolute-deviation-aggregation]


This `single-value` aggregation approximates the [median absolute deviation](https://en.wikipedia.org/wiki/Median_absolute_deviation) of its search results.

Median absolute deviation is a measure of variability. It is a robust statistic, meaning that it is useful for describing data that may have outliers, or may not be normally distributed. For such data it can be more descriptive than standard deviation.

It is calculated as the median of each data point’s deviation from the median of the entire sample. That is, for a random variable X, the median absolute deviation is median(|median(X) - Xi|).

## Example [_example_5]

Assume our data represents product reviews on a one to five star scale. Such reviews are usually summarized as a mean, which is easily understandable but doesn’t describe the reviews' variability. Estimating the median absolute deviation can provide insight into how much reviews vary from one another.

In this example we have a product which has an average rating of 3 stars. Let’s look at its ratings' median absolute deviation to determine how much they vary

```console
GET reviews/_search
{
  "size": 0,
  "aggs": {
    "review_average": {
      "avg": {
        "field": "rating"
      }
    },
    "review_variability": {
      "median_absolute_deviation": {
        "field": "rating" <1>
      }
    }
  }
}
```

1. `rating` must be a numeric field


The resulting median absolute deviation of `2` tells us that there is a fair amount of variability in the ratings. Reviewers must have diverse opinions about this product.

```console-result
{
  ...
  "aggregations": {
    "review_average": {
      "value": 3.0
    },
    "review_variability": {
      "value": 2.0
    }
  }
}
```


## Approximation [_approximation]

The naive implementation of calculating median absolute deviation stores the entire sample in memory, so this aggregation instead calculates an approximation. It uses the [TDigest data structure](https://github.com/tdunning/t-digest) to approximate the sample median and the median of deviations from the sample median. For more about the approximation characteristics of TDigests, see [Percentiles are (usually) approximate](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-approximation).

The tradeoff between resource usage and accuracy of a TDigest’s quantile approximation, and therefore the accuracy of this aggregation’s approximation of median absolute deviation, is controlled by the `compression` parameter. A higher `compression` setting provides a more accurate approximation at the cost of higher memory usage. For more about the characteristics of the TDigest `compression` parameter see [Compression](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-compression).

```console
GET reviews/_search
{
  "size": 0,
  "aggs": {
    "review_variability": {
      "median_absolute_deviation": {
        "field": "rating",
        "compression": 100
      }
    }
  }
}
```

The default `compression` value for this aggregation is `1000`. At this compression level this aggregation is usually within 5% of the exact result, but observed performance will depend on the sample data.


## Script [_script_7]

In the example above, product reviews are on a scale of one to five. If you want to modify them to a scale of one to ten, use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md).

```console
GET reviews/_search?filter_path=aggregations
{
  "size": 0,
  "runtime_mappings": {
    "rating.out_of_ten": {
      "type": "long",
      "script": {
        "source": "emit(doc['rating'].value * params.scaleFactor)",
        "params": {
          "scaleFactor": 2
        }
      }
    }
  },
  "aggs": {
    "review_average": {
      "avg": {
        "field": "rating.out_of_ten"
      }
    },
    "review_variability": {
      "median_absolute_deviation": {
        "field": "rating.out_of_ten"
      }
    }
  }
}
```

Which will result in:

```console-result
{
  "aggregations": {
    "review_average": {
      "value": 6.0
    },
    "review_variability": {
      "value": 4.0
    }
  }
}
```


## Missing value [_missing_value_11]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

Let’s be optimistic and assume some reviewers loved the product so much that they forgot to give it a rating. We’ll assign them five stars

```console
GET reviews/_search
{
  "size": 0,
  "aggs": {
    "review_variability": {
      "median_absolute_deviation": {
        "field": "rating",
        "missing": 5
      }
    }
  }
}
```


