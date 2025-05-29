---
navigation_title: "Change point"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-change-point-aggregation.html
---

# Change point aggregation [search-aggregations-change-point-aggregation]


::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


A sibling pipeline that detects, spikes, dips, and change points in a metric. Given a distribution of values provided by the sibling multi-bucket aggregation, this aggregation indicates the bucket of any spike or dip and/or the bucket at which the largest change in the distribution of values, if they are statistically significant.

::::{tip}
It is recommended to use the change point aggregation to detect changes in time-based data, however, you can use any metric to create buckets.
::::


## Parameters [change-point-agg-syntax]

`buckets_path`
:   (Required, string) Path to the buckets that contain one set of values in which to detect a change point. There must be at least 22 bucketed values. Fewer than 1,000 is preferred. For syntax, see [`buckets_path` Syntax](/reference/aggregations/pipeline.md#buckets-path-syntax).


## Syntax [_syntax_11]

A `change_point` aggregation looks like this in isolation:

```js
{
  "change_point": {
    "buckets_path": "date_histogram>_count" <1>
  }
}
```
% NOTCONSOLE

1. The buckets containing the values to test against.



## Response body [change-point-agg-response]

`bucket`
:   (Optional, object) Values of the bucket that indicates the discovered change point. Not returned if no change point was found. All the aggregations in the bucket are returned as well.

    **Properties of `bucket**:

    `key`
    :   (value) The key of the bucket matched. Could be string or numeric.

    `doc_count`
    :   (number) The document count of the bucket.


`type`
:   (object) The found change point type and its related values. Possible types:

    * `dip`: a significant dip occurs at this change point
    * `distribution_change`: the overall distribution of the values has changed significantly
    * `non_stationary`: there is no change point, but the values are not from a stationary distribution
    * `spike`: a significant spike occurs at this point
    * `stationary`: no change point found
    * `step_change`: the change indicates a statistically significant step up or down in value distribution
    * `trend_change`: there is an overall trend change occurring at this point



## Example [_example_7]

The following example uses the Kibana sample data logs data set.

```js
GET kibana_sample_data_logs/_search
{
  "aggs": {
    "date":{ <1>
      "date_histogram": {
        "field": "@timestamp",
        "fixed_interval": "1d"
      },
      "aggs": {
        "avg": { <2>
          "avg": {
            "field": "bytes"
          }
        }
      }
    },
    "change_points_avg": { <3>
      "change_point": {
        "buckets_path": "date>avg" <4>
      }
    }
  }
}
```
% NOTCONSOLE

1. A date histogram aggregation that creates buckets with one day long interval.
2. A sibling aggregation of the `date` aggregation that calculates the average value of the `bytes` field within every bucket.
3. The change point detection aggregation configuration object.
4. The path of the aggregation values to detect change points. In this case, the input of the change point aggregation is the value of `avg` which is a sibling aggregation of `date`.


The request returns a response that is similar to the following:

```js
    "change_points_avg" : {
      "bucket" : {
        "key" : "2023-04-29T00:00:00.000Z", <1>
        "doc_count" : 329, <2>
        "avg" : { <3>
          "value" : 4737.209726443769
        }
      },
      "type" : { <4>
        "dip" : {
          "p_value" : 3.8999455212466465e-10, <5>
          "change_point" : 41 <6>
        }
      }
    }
```
% NOTCONSOLE

1. The bucket key that is the change point.
2. The number of documents in that bucket.
3. Aggregated values in the bucket.
4. Type of change found.
5. The `p_value` indicates how extreme the change is; lower values indicate greater change.
6. The specific bucket where the change occurs (indexing starts at `0`).



