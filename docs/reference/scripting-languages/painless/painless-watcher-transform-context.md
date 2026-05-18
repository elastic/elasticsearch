---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-watcher-transform-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Watcher transform context [painless-watcher-transform-context]

Use a Painless script as a [watch transform](docs-content://explore-analyze/alerts-cases/watcher/transform-script.md) to transform a payload into a new payload for further use in the watch. Transform scripts return an Object value of the new payload.

The following variables are available in all watcher contexts.

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`ctx['watch_id']` (`String`, read-only)
:   The id of the watch.

`ctx['id']` (`String`, read-only)
:   The server generated unique identifier for the run watch.

`ctx['metadata']` (`Map`, read-only)
:   Metadata can be added to the top level of the watch definition. This is user defined and is typically used to consolidate duplicate values in a watch.

`ctx['execution_time']` (`ZonedDateTime`, read-only)
:   The time the watch began execution.

`ctx['trigger']['scheduled_time']` (`ZonedDateTime`, read-only)
:   The scheduled trigger time for the watch. This is the time the watch should be executed.

`ctx['trigger']['triggered_time']` (`ZonedDateTime`, read-only)
:   The actual trigger time for the watch. This is the time the watch was triggered for execution.

`ctx['payload']` (`Map`, read-only)
:   The accessible watch data based upon the [watch input](docs-content://explore-analyze/alerts-cases/watcher/input.md).

## Return

`Object`
:   The new payload.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

This request creates an automated sales monitoring system that checks your eCommerce data every hour and sends alerts when certain product categories are performing well.

The transform script processes the aggregation results from the search input, extracting `bucket.key` (category name) and `bucket.revenue.value` (sales amount) from each category. It uses `ctx.metadata.alert_threshold` (50 USD) to determine which categories trigger alerts, creating a structured output with category details and alert flags for the logging action. 

```json
POST _watcher/watch/_execute
{
  "watch": {
    "metadata": {
      "alert_threshold": 50
    },
    "trigger": {
      "schedule": {
        "interval": "1h"
      }
    },
    "input": {
      "search": {
        "request": {
          "indices": ["kibana_sample_data_ecommerce"],
          "body": {
            "query": {
              "range": {
                "order_date": {
                  "gte": "now-24h"
                }
              }
            },
            "aggs": {
              "by_category": {
                "terms": {
                  "field": "category.keyword"
                },
                "aggs": {
                  "revenue": {
                    "sum": {
                      "field": "taxful_total_price"
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "condition": {
      "script": {
        "source": """
          return ctx.payload.aggregations.by_category.buckets.size() > 0;
        """
      }
    },
    "transform": {
      "script": {
        "source": """
/*
          Process the aggregation buckets to:
          1. Calculate total revenue across all categories.
          2. Build a list of categories with:
             - name
             - sales amount (rounded to 2 decimals)
             - alert flag if revenue exceeds threshold from ctx.metadata.
          3. Return the processed data along with the watch execution time.
        */

          def categories = [];
          def total = 0.0;
          
          for (bucket in ctx.payload.aggregations.by_category.buckets) {
            def revenue = bucket.revenue.value;
            total += revenue;
            
            categories.add([
              'name': bucket.key,
              'sales': Math.round(revenue * 100) / 100.0,
              'alert': revenue > ctx.metadata.alert_threshold
            ]);
          }
          
          return [
            'total_sales': Math.round(total * 100) / 100.0,
            'categories': categories,
            'execution_time': ctx.execution_time
          ];
        """
      }
    },
    "actions": {
      "notify": {
        "logging": {
          "text": """
              Daily Sales Report - {{ctx.payload.execution_time}}
              Total Revenue: ${{ctx.payload.total_sales}}

              Categories:
              {{#ctx.payload.categories}}
              - {{name}}: ${{sales}} {{#alert}}⚠️ HIGH{{/alert}}
              {{/ctx.payload.categories}}
          """
        }
      }
    }
  }
}
```
