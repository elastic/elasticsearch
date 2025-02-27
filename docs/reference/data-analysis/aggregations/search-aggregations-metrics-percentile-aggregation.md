---
navigation_title: "Percentiles"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-aggregation.html
---

# Percentiles aggregation [search-aggregations-metrics-percentile-aggregation]


A `multi-value` metrics aggregation that calculates one or more percentiles over numeric values extracted from the aggregated documents. These values can be extracted from specific numeric or [histogram fields](/reference/elasticsearch/mapping-reference/histogram.md) in the documents.

Percentiles show the point at which a certain percentage of observed values occur. For example, the 95th percentile is the value which is greater than 95% of the observed values.

Percentiles are often used to find outliers. In normal distributions, the 0.13th and 99.87th percentiles represents three standard deviations from the mean. Any data which falls outside three standard deviations is often considered an anomaly.

When a range of percentiles are retrieved, they can be used to estimate the data distribution and determine if the data is skewed, bimodal, etc.

Assume your data consists of website load times. The average and median load times are not overly useful to an administrator. The max may be interesting, but it can be easily skewed by a single slow response.

Let’s look at a range of percentiles representing load time:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_outlier": {
      "percentiles": {
        "field": "load_time" <1>
      }
    }
  }
}
```

1. The field `load_time` must be a numeric field


By default, the `percentile` metric will generate a range of percentiles: `[ 1, 5, 25, 50, 75, 95, 99 ]`. The response will look like this:

```console-result
{
  ...

 "aggregations": {
    "load_time_outlier": {
      "values": {
        "1.0": 10.0,
        "5.0": 30.0,
        "25.0": 170.0,
        "50.0": 445.0,
        "75.0": 720.0,
        "95.0": 940.0,
        "99.0": 980.0
      }
    }
  }
}
```

As you can see, the aggregation will return a calculated value for each percentile in the default range. If we assume response times are in milliseconds, it is immediately obvious that the webpage normally loads in 10-720ms, but occasionally spikes to 940-980ms.

Often, administrators are only interested in outliers — the extreme percentiles. We can specify just the percents we are interested in (requested percentiles must be a value between 0-100 inclusive):

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_outlier": {
      "percentiles": {
        "field": "load_time",
        "percents": [ 95, 99, 99.9 ] <1>
      }
    }
  }
}
```

1. Use the `percents` parameter to specify particular percentiles to calculate


## Keyed Response [_keyed_response_6]

By default the `keyed` flag is set to `true` which associates a unique string key with each bucket and returns the ranges as a hash rather than an array. Setting the `keyed` flag to `false` will disable this behavior:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_outlier": {
      "percentiles": {
        "field": "load_time",
        "keyed": false
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
    "load_time_outlier": {
      "values": [
        {
          "key": 1.0,
          "value": 10.0
        },
        {
          "key": 5.0,
          "value": 30.0
        },
        {
          "key": 25.0,
          "value": 170.0
        },
        {
          "key": 50.0,
          "value": 445.0
        },
        {
          "key": 75.0,
          "value": 720.0
        },
        {
          "key": 95.0,
          "value": 940.0
        },
        {
          "key": 99.0,
          "value": 980.0
        }
      ]
    }
  }
}
```


## Script [_script_10]

If you need to run the aggregation against values that aren’t indexed, use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md). For example, if our load times are in milliseconds but you want percentiles calculated in seconds:

```console
GET latency/_search
{
  "size": 0,
  "runtime_mappings": {
    "load_time.seconds": {
      "type": "long",
      "script": {
        "source": "emit(doc['load_time'].value / params.timeUnit)",
        "params": {
          "timeUnit": 1000
        }
      }
    }
  },
  "aggs": {
    "load_time_outlier": {
      "percentiles": {
        "field": "load_time.seconds"
      }
    }
  }
}
```


## Percentiles are (usually) approximate [search-aggregations-metrics-percentile-aggregation-approximation]

There are many different algorithms to calculate percentiles. The naive implementation simply stores all the values in a sorted array. To find the 50th percentile, you simply find the value that is at `my_array[count(my_array) * 0.5]`.

Clearly, the naive implementation does not scale — the sorted array grows linearly with the number of values in your dataset. To calculate percentiles across potentially billions of values in an Elasticsearch cluster, *approximate* percentiles are calculated.

The algorithm used by the `percentile` metric is called TDigest (introduced by Ted Dunning in [Computing Accurate Quantiles using T-Digests](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf)).

When using this metric, there are a few guidelines to keep in mind:

* Accuracy is proportional to `q(1-q)`. This means that extreme percentiles (e.g. 99%) are more accurate than less extreme percentiles, such as the median
* For small sets of values, percentiles are highly accurate (and potentially 100% accurate if the data is small enough).
* As the quantity of values in a bucket grows, the algorithm begins to approximate the percentiles. It is effectively trading accuracy for memory savings. The exact level of inaccuracy is difficult to generalize, since it depends on your data distribution and volume of data being aggregated

The following chart shows the relative error on a uniform distribution depending on the number of collected values and the requested percentile:

![percentiles error](../../../images/percentiles_error.png "")

It shows how precision is better for extreme percentiles. The reason why error diminishes for large number of values is that the law of large numbers makes the distribution of values more and more uniform and the t-digest tree can do a better job at summarizing it. It would not be the case on more skewed distributions.

::::{warning}
Percentile aggregations are also [non-deterministic](https://en.wikipedia.org/wiki/Nondeterministic_algorithm). This means you can get slightly different results using the same data.

::::



## Compression [search-aggregations-metrics-percentile-aggregation-compression]

Approximate algorithms must balance memory utilization with estimation accuracy. This balance can be controlled using a `compression` parameter:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_outlier": {
      "percentiles": {
        "field": "load_time",
        "tdigest": {
          "compression": 200    <1>
        }
      }
    }
  }
}
```

1. Compression controls memory usage and approximation error


The TDigest algorithm uses a number of "nodes" to approximate percentiles — the more nodes available, the higher the accuracy (and large memory footprint) proportional to the volume of data. The `compression` parameter limits the maximum number of nodes to `20 * compression`.

Therefore, by increasing the compression value, you can increase the accuracy of your percentiles at the cost of more memory. Larger compression values also make the algorithm slower since the underlying tree data structure grows in size, resulting in more expensive operations. The default compression value is `100`.

A "node" uses roughly 32 bytes of memory, so under worst-case scenarios (large amount of data which arrives sorted and in-order) the default settings will produce a TDigest roughly 64KB in size. In practice data tends to be more random and the TDigest will use less memory.


## Execution hint [search-aggregations-metrics-percentile-aggregation-execution-hint]

The default implementation of TDigest is optimized for performance, scaling to millions or even billions of sample values while maintaining acceptable accuracy levels (close to 1% relative error for millions of samples in some cases). There’s an option to use an implementation optimized for accuracy by setting parameter `execution_hint` to value `high_accuracy`:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_outlier": {
      "percentiles": {
        "field": "load_time",
        "tdigest": {
          "execution_hint": "high_accuracy"    <1>
        }
      }
    }
  }
}
```

1. Optimize TDigest for accuracy, at the expense of performance


This option can lead to improved accuracy (relative error close to 0.01% for millions of samples in some cases) but then percentile queries take 2x-10x longer to complete.


## HDR histogram [_hdr_histogram_2]

[HDR Histogram](https://github.com/HdrHistogram/HdrHistogram) (High Dynamic Range Histogram) is an alternative implementation that can be useful when calculating percentiles for latency measurements as it can be faster than the t-digest implementation with the trade-off of a larger memory footprint. This implementation maintains a fixed worse-case percentage error (specified as a number of significant digits). This means that if data is recorded with values from 1 microsecond up to 1 hour (3,600,000,000 microseconds) in a histogram set to 3 significant digits, it will maintain a value resolution of 1 microsecond for values up to 1 millisecond and 3.6 seconds (or better) for the maximum tracked value (1 hour).

The HDR Histogram can be used by specifying the `hdr` parameter in the request:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_outlier": {
      "percentiles": {
        "field": "load_time",
        "percents": [ 95, 99, 99.9 ],
        "hdr": {                                  <1>
          "number_of_significant_value_digits": 3 <2>
        }
      }
    }
  }
}
```

1. `hdr` object indicates that HDR Histogram should be used to calculate the percentiles and specific settings for this algorithm can be specified inside the object
2. `number_of_significant_value_digits` specifies the resolution of values for the histogram in number of significant digits


The HDRHistogram only supports positive values and will error if it is passed a negative value. It is also not a good idea to use the HDRHistogram if the range of values is unknown as this could lead to high memory usage.


## Missing value [_missing_value_14]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "grade_percentiles": {
      "percentiles": {
        "field": "grade",
        "missing": 10       <1>
      }
    }
  }
}
```

1. Documents without a value in the `grade` field will fall into the same bucket as documents that have the value `10`.



