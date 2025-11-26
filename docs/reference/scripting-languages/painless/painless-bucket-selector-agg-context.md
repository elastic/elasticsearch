---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-bucket-selector-agg-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Bucket selector aggregation context [painless-bucket-selector-agg-context]

Use a Painless script in an [`bucket_selector` aggregation](/reference/aggregations/search-aggregations-pipeline-bucket-selector-aggregation.md) to determine if a bucket should be retained or filtered out.

## Variables [_variables_2]

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query. The parameters include values defined as part of the `buckets_path`.


## Return [_return_2]

boolean
:   True if the bucket should be retained, false if the bucket should be filtered out.


## API [_api_2]

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.


## Example [_example_2]

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

The following request filters out low-performing manufacturers and focuses only on brands with significant sales volume. The query groups orders by manufacturer, counts total orders for each brand, then uses a bucket selector to show only manufacturers with 50 or more orders. 

```json
GET kibana_sample_data_ecommerce/_search
{
  "size": 0,
  "aggs": {
    "manufacturers": {
      "terms": {
        "field": "manufacturer.keyword"
      },
      "aggs": {
        "total_orders": {
          "value_count": {
            "field": "order_id"
          }
        },
        "high_volume_filter": {
          "bucket_selector": {
            "buckets_path": {
              "order_count": "total_orders"
            },
            "script": {
              "source": "params.order_count >= 50"
            }
          }
        }
      }
    }
  }
}
```
