---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-bucket-script-agg-context.html
products:
  - id: painless
---

# Bucket script aggregation context [painless-bucket-script-agg-context]

Use a Painless script in an [`bucket_script` pipeline aggregation](/reference/aggregations/search-aggregations-pipeline-bucket-script-aggregation.md) to calculate a value as a result in a bucket.

## Variables [_variables]

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query. The parameters include values defined as part of the `buckets_path`.


## Return [_return]

numeric
:   The calculated value as the result.


## API [_api]

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.


## Example [_example]

To run this example, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

The painless context in a `bucket_script` aggregation provides a `params` map. This map contains both user-specified custom values, as well as the values from other aggregations specified in the `buckets_path` property.

This example takes the values from a min and max aggregation, calculates the difference, and adds the user-specified base_cost to the result:

```painless
(params.max - params.min) + params.base_cost
```

Note that the values are extracted from the `params` map. In context, the aggregation looks like this:

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
        "min_cost": {
          "min": {
            "field": "cost"
          }
        },
        "max_cost": {
          "max": {
            "field": "cost"
          }
        },
        "spread_plus_base": {
          "bucket_script": {
            "buckets_path": { <1>
              "min": "min_cost",
              "max": "max_cost"
            },
            "script": {
              "params": {
                "base_cost": 5 <2>
              },
              "source": "(params.max - params.min) + params.base_cost"
            }
          }
        }
      }
    }
  }
}
```

1. The `buckets_path` points to two aggregations (`min_cost`, `max_cost`) and adds `min`/`max` variables to the `params` map
2. The user-specified `base_cost` is also added to the script’s `params` map



