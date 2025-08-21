---
navigation_title: "Moving percentiles"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-moving-percentiles-aggregation.html
---

# Moving percentiles aggregation [search-aggregations-pipeline-moving-percentiles-aggregation]


Given an ordered series of [percentiles](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md), the Moving Percentile aggregation will slide a window across those percentiles and allow the user to compute the cumulative percentile.

This is conceptually very similar to the [Moving Function](/reference/aggregations/search-aggregations-pipeline-movfn-aggregation.md) pipeline aggregation, except it works on the percentiles sketches instead of the actual buckets values.

## Syntax [_syntax_19]

A `moving_percentiles` aggregation looks like this in isolation:

```js
{
  "moving_percentiles": {
    "buckets_path": "the_percentile",
    "window": 10
  }
}
```

$$$moving-percentiles-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `buckets_path` | Path to the percentile of interest (see [`buckets_path` Syntax](/reference/aggregations/pipeline.md#buckets-path-syntax) for more details | Required |  |
| `window` | The size of window to "slide" across the histogram. | Required |  |
| `shift` | [Shift](/reference/aggregations/search-aggregations-pipeline-movfn-aggregation.md#shift-parameter) of window position. | Optional | 0 |

`moving_percentiles` aggregations must be embedded inside of a `histogram` or `date_histogram` aggregation. They can be embedded like any other metric aggregation:

```console
POST /_search
{
  "size": 0,
  "aggs": {
    "my_date_histo": {                          <1>
        "date_histogram": {
        "field": "date",
        "calendar_interval": "1M"
      },
      "aggs": {
        "the_percentile": {                     <2>
            "percentiles": {
            "field": "price",
            "percents": [ 1.0, 99.0 ]
          }
        },
        "the_movperc": {
          "moving_percentiles": {
            "buckets_path": "the_percentile",   <3>
            "window": 10
          }
        }
      }
    }
  }
}
```

1. A `date_histogram` named "my_date_histo" is constructed on the "timestamp" field, with one-day intervals
2. A `percentile` metric is used to calculate the percentiles of a field.
3. Finally, we specify a `moving_percentiles` aggregation which uses "the_percentile" sketch as its input.


Moving percentiles are built by first specifying a `histogram` or `date_histogram` over a field. You then add a percentile metric inside of that histogram. Finally, the `moving_percentiles` is embedded inside the histogram. The `buckets_path` parameter is then used to "point" at the percentiles aggregation inside of the histogram (see [`buckets_path` Syntax](/reference/aggregations/pipeline.md#buckets-path-syntax) for a description of the syntax for `buckets_path`).

And the following may be the response:

```console-result
{
   "took": 11,
   "timed_out": false,
   "_shards": ...,
   "hits": ...,
   "aggregations": {
      "my_date_histo": {
         "buckets": [
             {
                 "key_as_string": "2015/01/01 00:00:00",
                 "key": 1420070400000,
                 "doc_count": 3,
                 "the_percentile": {
                     "values": {
                       "1.0": 151.0,
                       "99.0": 200.0
                     }
                 }
             },
             {
                 "key_as_string": "2015/02/01 00:00:00",
                 "key": 1422748800000,
                 "doc_count": 2,
                 "the_percentile": {
                     "values": {
                       "1.0": 10.4,
                       "99.0": 49.6
                     }
                 },
                 "the_movperc": {
                   "values": {
                     "1.0": 151.0,
                     "99.0": 200.0
                   }
                 }
             },
             {
                 "key_as_string": "2015/03/01 00:00:00",
                 "key": 1425168000000,
                 "doc_count": 2,
                 "the_percentile": {
                    "values": {
                      "1.0": 175.25,
                      "99.0": 199.75
                    }
                 },
                 "the_movperc": {
                    "values": {
                      "1.0": 11.6,
                      "99.0": 200.0
                    }
                 }
             }
         ]
      }
   }
}
```

The output format of the `moving_percentiles` aggregation is inherited from the format of the referenced [`percentiles`](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md) aggregation.

Moving percentiles pipeline aggregations always run with `skip` gap policy.


## shift parameter [moving-percentiles-shift-parameter]

By default (with `shift = 0`), the window that is offered for calculation is the last `n` values excluding the current bucket. Increasing `shift` by 1 moves starting window position by `1` to the right.

* To include current bucket to the window, use `shift = 1`.
* For center alignment (`n / 2` values before and after the current bucket), use `shift = window / 2`.
* For right alignment (`n` values after the current bucket), use `shift = window`.

If either of window edges moves outside the borders of data series, the window shrinks to include available values only.


