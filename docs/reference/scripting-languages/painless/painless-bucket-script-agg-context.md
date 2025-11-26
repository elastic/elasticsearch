---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-bucket-script-agg-context.html
applies_to:
  stack: ga
  serverless: ga
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

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

The following request is useful for identifying high-value markets by comparing average order values across countries. It groups orders by country and calculates metrics for each country, including `total_revenue` and `order_count`, then uses a bucket script to compute `revenue_per_order` for performance analysis.

```json
GET kibana_sample_data_ecommerce/_search
{
  "size": 0,
  "aggs": {
    "countries": {
      "terms": {
        "field": "geoip.country_iso_code"
      },
      "aggs": {
        "total_revenue": {
          "sum": {
            "field": "taxful_total_price"
          }
        },
        "order_count": {
          "value_count": {
            "field": "order_id"
          }
        },
        "revenue_per_order": {
          "bucket_script": {
            "buckets_path": {
              "revenue": "total_revenue",
              "orders": "order_count"
            },
            "script": {
              "source": "params.revenue / params.orders"
            }
          }
        }
      }
    }
  }
}
```
