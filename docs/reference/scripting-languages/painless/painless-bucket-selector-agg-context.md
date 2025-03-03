---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-bucket-selector-agg-context.html
---

# Bucket selector aggregation context [painless-bucket-selector-agg-context]

Use a Painless script in an [`bucket_selector` aggregation](/reference/data-analysis/aggregations/search-aggregations-pipeline-bucket-selector-aggregation.md) to determine if a bucket should be retained or filtered out.

## Variables [_variables_2]

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query. The parameters include values defined as part of the `buckets_path`.


## Return [_return_2]

boolean
:   True if the bucket should be retained, false if the bucket should be filtered out.


## API [_api_2]

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.


## Example [_example_2]

To run this example, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

The painless context in a `bucket_selector` aggregation provides a `params` map. This map contains both user-specified custom values, as well as the values from other aggregations specified in the `buckets_path` property.

Unlike some other aggregation contexts, the `bucket_selector` context must return a boolean `true` or `false`.

This example finds the max of each bucket, adds a user-specified base_cost, and retains all of the buckets that are greater than `10`.

```painless
params.max + params.base_cost > 10
```

Note that the values are extracted from the `params` map. The script is in the form of an expression that returns `true` or `false`. In context, the aggregation looks like this:

```console
GET /seats/_search
{
  "size": 0,
  "aggs": {
    "theatres": {
      "terms": {
        "field": "theatre",
        "size": 10
      },
      "aggs": {
        "max_cost": {
          "max": {
            "field": "cost"
          }
        },
        "filtering_agg": {
          "bucket_selector": {
            "buckets_path": { <1>
              "max": "max_cost"
            },
            "script": {
              "params": {
                "base_cost": 5 <2>
              },
              "source": "params.max + params.base_cost > 10"
            }
          }
        }
      }
    }
  }
}
```

1. The `buckets_path` points to the max aggregations (`max_cost`) and adds `max` variables to the `params` map
2. The user-specified `base_cost` is also added to the `params` map



