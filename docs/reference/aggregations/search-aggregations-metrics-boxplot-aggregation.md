---
navigation_title: "Boxplot"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-boxplot-aggregation.html
---

# Boxplot aggregation [search-aggregations-metrics-boxplot-aggregation]


A `boxplot` metrics aggregation that computes boxplot of numeric values extracted from the aggregated documents. These values can be generated from specific numeric or [histogram fields](/reference/elasticsearch/mapping-reference/histogram.md) in the documents.

The `boxplot` aggregation returns essential information for making a [box plot](https://en.wikipedia.org/wiki/Box_plot): minimum, maximum, median, first quartile (25th percentile)  and third quartile (75th percentile) values.

## Syntax [_syntax_4]

A `boxplot` aggregation looks like this in isolation:

```js
{
  "boxplot": {
    "field": "load_time"
  }
}
```
% NOTCONSOLE

Let’s look at a boxplot representing load time:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_boxplot": {
      "boxplot": {
        "field": "load_time" <1>
      }
    }
  }
}
```
% TEST[setup:latency]

1. The field `load_time` must be a numeric field


The response will look like this:

```console-result
{
  ...

 "aggregations": {
    "load_time_boxplot": {
      "min": 0.0,
      "max": 990.0,
      "q1": 167.5,
      "q2": 445.0,
      "q3": 722.5,
      "lower": 0.0,
      "upper": 990.0
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

In this case, the lower and upper whisker values are equal to the min and max. In general, these values are the 1.5 * IQR range, which is to say the nearest values to `q1 - (1.5 * IQR)` and `q3 + (1.5 * IQR)`. Since this is an approximation, the given values may not actually be observed values from the data, but should be within a reasonable error bound of them. While the Boxplot aggregation doesn’t directly return outlier points, you can check if `lower > min` or `upper < max` to see if outliers exist on either side, and then query for them directly.


## Script [_script_3]

If you need to create a boxplot for values that aren’t indexed exactly you should create a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md) and get the boxplot of that. For example, if your load times are in milliseconds but you want values calculated in seconds, use a runtime field to convert them:

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
    "load_time_boxplot": {
      "boxplot": { "field": "load_time.seconds" }
    }
  }
}
```
% TEST[setup:latency]
% TEST[s/_search/_search\?filter_path=aggregations/]
% TEST[s/"timeUnit": 1000/"timeUnit": 10/]


## Boxplot values are (usually) approximate [search-aggregations-metrics-boxplot-aggregation-approximation]

The algorithm used by the `boxplot` metric is called TDigest (introduced by Ted Dunning in [Computing Accurate Quantiles using T-Digests](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf)).

::::{warning}
Boxplot as other percentile aggregations are also [non-deterministic](https://en.wikipedia.org/wiki/Nondeterministic_algorithm). This means you can get slightly different results using the same data.

::::



## Compression [search-aggregations-metrics-boxplot-aggregation-compression]

Approximate algorithms must balance memory utilization with estimation accuracy. This balance can be controlled using a `compression` parameter:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_boxplot": {
      "boxplot": {
        "field": "load_time",
        "compression": 200    <1>
      }
    }
  }
}
```
% TEST[setup:latency]

1. Compression controls memory usage and approximation error


The TDigest algorithm uses a number of "nodes" to approximate percentiles — the more nodes available, the higher the accuracy (and large memory footprint) proportional to the volume of data. The `compression` parameter limits the maximum number of nodes to `20 * compression`.

Therefore, by increasing the compression value, you can increase the accuracy of your percentiles at the cost of more memory. Larger compression values also make the algorithm slower since the underlying tree data structure grows in size, resulting in more expensive operations. The default compression value is `100`.

A "node" uses roughly 32 bytes of memory, so under worst-case scenarios (large amount of data which arrives sorted and in-order) the default settings will produce a TDigest roughly 64KB in size. In practice data tends to be more random and the TDigest will use less memory.


## Execution hint [_execution_hint_3]

The default implementation of TDigest is optimized for performance, scaling to millions or even billions of sample values while maintaining acceptable accuracy levels (close to 1% relative error for millions of samples in some cases). There’s an option to use an implementation optimized for accuracy by setting parameter `execution_hint` to value `high_accuracy`:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_boxplot": {
      "boxplot": {
        "field": "load_time",
        "execution_hint": "high_accuracy"    <1>
      }
    }
  }
}
```
% TEST[setup:latency]

1. Optimize TDigest for accuracy, at the expense of performance


This option can lead to improved accuracy (relative error close to 0.01% for millions of samples in some cases) but then percentile queries take 2x-10x longer to complete.


## Missing value [_missing_value_7]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "grade_boxplot": {
      "boxplot": {
        "field": "grade",
        "missing": 10     <1>
      }
    }
  }
}
```
% TEST[setup:latency]

1. Documents without a value in the `grade` field will fall into the same bucket as documents that have the value `10`.



