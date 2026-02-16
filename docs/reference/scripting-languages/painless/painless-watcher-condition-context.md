---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-watcher-condition-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Watcher condition context [painless-watcher-condition-context]

Use a Painless script as a [watch condition](docs-content://explore-analyze/alerts-cases/watcher/condition-script.md) that determines whether to execute a watch or a particular action within a watch. Condition scripts return a Boolean value to indicate the status of the condition.

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

`boolean`
:   Expects `true` if the condition is met, and `false` if it is not.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the examples, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

**Manufacturer revenue anomaly detection**  
The following script creates a watcher that runs daily to monitor manufacturer revenue anomalies by querying the last seven days of documents and calculating `total_revenue` per `manufacturer.keyword`.

The condition in the script filters manufacturers with `total_revenue.value` either below 200 or above 2000, triggering an alert log when any anomalous manufacturers are detected.

```json
POST _watcher/watch/_execute
{
  "watch": {
    "trigger": {
      "schedule": {
        "interval": "24h"
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
                  "gte": "now-7d"
                }
              }
            },
            "size": 0,
            "aggs": {
              "manufacturers": {
                "terms": {
                  "field": "manufacturer.keyword"
                },
                "aggs": {
                  "total_revenue": {
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
      "script": """
        return ctx.payload.aggregations.manufacturers.buckets.stream()
          .filter(manufacturer -> manufacturer.total_revenue.value < 200 ||
                                 manufacturer.total_revenue.value > 2000)
          .count() > 0
      """
    },
    "actions": {
      "alert_log": {
        "logging": {
          "text": "ALERT: Manufacturers with anomalous sales detected: {{ctx.payload.aggregations.manufacturers.buckets}}"
        }
      }
    }
  }
}
```

**High-value order detection**  
This example runs hourly to detect high-value orders by filtering orders from the last hour when `taxful_total_price` is more than 150. The script generates a log message when it finds high-value orders.

```json
POST _watcher/watch/_execute
{
  "watch": {
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
              "bool": {
                "filter": [
                  {
                    "range": {
                      "order_date": {
                        "gte": "now-1h"
                      }
                    }
                  },
                  {
                    "range": {
                      "taxful_total_price": {
                        "gte": 150
                      }
                    }
                  }
                ]
              }
            },
            "size": 0
          }
        }
      }
    },
    "condition": {
      "script": """
        return ctx.payload.hits.total > 0
      """
    },
    "actions": {
      "high_value_notification": {
        "logging": {
          "text": "ALERT: {{ctx.payload.hits.total}} high-value orders (over 150 EUR) detected in the last hour"
        }
      }
    }
  }
}
```
