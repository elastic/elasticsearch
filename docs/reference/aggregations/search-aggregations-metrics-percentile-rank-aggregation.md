---
navigation_title: "Percentile ranks"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-rank-aggregation.html
---

# Percentile ranks aggregation [search-aggregations-metrics-percentile-rank-aggregation]


A `multi-value` metrics aggregation that calculates one or more percentile ranks over numeric values extracted from the aggregated documents. These values can be extracted from specific numeric or [histogram fields](/reference/elasticsearch/mapping-reference/histogram.md) in the documents.

::::{note}
Please see [Percentiles are (usually) approximate](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-approximation), [Compression](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-compression) and [Execution hint](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-execution-hint) for advice regarding approximation, performance and memory use of the percentile ranks aggregation

::::


Percentile rank show the percentage of observed values which are below certain value. For example, if a value is greater than or equal to 95% of the observed values it is said to be at the 95th percentile rank.

Assume your data consists of website load times. You may have a service agreement that 95% of page loads complete within 500ms and 99% of page loads complete within 600ms.

Let’s look at a range of percentiles representing load time:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_ranks": {
      "percentile_ranks": {
        "field": "load_time",   <1>
        "values": [ 500, 600 ]
      }
    }
  }
}
```

1. The field `load_time` must be a numeric field


The response will look like this:

```console-result
{
  ...

 "aggregations": {
    "load_time_ranks": {
      "values": {
        "500.0": 55.0,
        "600.0": 64.0
      }
    }
  }
}
```

From this information you can determine you are hitting the 99% load time target but not quite hitting the 95% load time target

## Keyed Response [_keyed_response_5]

By default the `keyed` flag is set to `true` associates a unique string key with each bucket and returns the ranges as a hash rather than an array. Setting the `keyed` flag to `false` will disable this behavior:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_ranks": {
      "percentile_ranks": {
        "field": "load_time",
        "values": [ 500, 600 ],
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
    "load_time_ranks": {
      "values": [
        {
          "key": 500.0,
          "value": 55.0
        },
        {
          "key": 600.0,
          "value": 64.0
        }
      ]
    }
  }
}
```


## Script [_script_9]

If you need to run the aggregation against values that aren’t indexed, use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md). For example, if our load times are in milliseconds but we want percentiles calculated in seconds:

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
    "load_time_ranks": {
      "percentile_ranks": {
        "values": [ 500, 600 ],
        "field": "load_time.seconds"
      }
    }
  }
}
```


## HDR Histogram [_hdr_histogram]

[HDR Histogram](https://github.com/HdrHistogram/HdrHistogram) (High Dynamic Range Histogram) is an alternative implementation that can be useful when calculating percentile ranks for latency measurements as it can be faster than the t-digest implementation with the trade-off of a larger memory footprint. This implementation maintains a fixed worse-case percentage error (specified as a number of significant digits). This means that if data is recorded with values from 1 microsecond up to 1 hour (3,600,000,000 microseconds) in a histogram set to 3 significant digits, it will maintain a value resolution of 1 microsecond for values up to 1 millisecond and 3.6 seconds (or better) for the maximum tracked value (1 hour).

The HDR Histogram can be used by specifying the `hdr` object in the request:

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_ranks": {
      "percentile_ranks": {
        "field": "load_time",
        "values": [ 500, 600 ],
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


## Missing value [_missing_value_13]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

```console
GET latency/_search
{
  "size": 0,
  "aggs": {
    "load_time_ranks": {
      "percentile_ranks": {
        "field": "load_time",
        "values": [ 500, 600 ],
        "missing": 10           <1>
      }
    }
  }
}
```

1. Documents without a value in the `load_time` field will fall into the same bucket as documents that have the value `10`.



