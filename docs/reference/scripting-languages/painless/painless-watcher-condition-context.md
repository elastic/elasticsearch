---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-watcher-condition-context.html
products:
  - id: painless
---

# Watcher condition context [painless-watcher-condition-context]

Use a Painless script as a [watch condition](docs-content://explore-analyze/alerts-cases/watcher/condition-script.md) that determines whether to execute a watch or a particular action within a watch. Condition scripts return a Boolean value to indicate the status of the condition.

The following variables are available in all watcher contexts.

**Variables**

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

**Return**

`boolean`
:   Expects `true` if the condition is met, and `false` if it is not.

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

**Example**

To run the examples, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

```console
POST _watcher/watch/_execute
{
  "watch" : {
    "trigger" : { "schedule" : { "interval" : "24h" } },
    "input" : {
      "search" : {
        "request" : {
          "indices" : [ "seats" ],
          "body" : {
            "query" : {
              "term": { "sold": "true"}
            },
            "aggs" : {
              "theatres" : {
                "terms" : { "field" : "play" },
                "aggs" : {
                  "money" : {
                    "sum": { "field" : "cost" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "condition" : {
      "script" :
      """
        return ctx.payload.aggregations.theatres.buckets.stream()       <1>
          .filter(theatre -> theatre.money.value < 15000 ||
                             theatre.money.value > 50000)               <2>
          .count() > 0                                                  <3>
      """
    },
    "actions" : {
      "my_log" : {
        "logging" : {
          "text" : "The output of the search was : {{ctx.payload.aggregations.theatres.buckets}}"
        }
      }
    }
  }
}
```

1. The Java Stream API is used in the condition. This API allows manipulation of the elements of the list in a pipeline.
2. The stream filter removes items that do not meet the filter criteria.
3. If there is at least one item in the list, the condition evaluates to true and the watch is executed.


The following action condition script controls execution of the my_log action based on the value of the seats sold for the plays in the data set. The script aggregates the total sold seats for each play and returns true if there is at least one play that has sold over $10,000.

```console
POST _watcher/watch/_execute
{
  "watch" : {
    "trigger" : { "schedule" : { "interval" : "24h" } },
    "input" : {
      "search" : {
        "request" : {
          "indices" : [ "seats" ],
          "body" : {
            "query" : {
              "term": { "sold": "true"}
            },
            "size": 0,
            "aggs" : {
              "theatres" : {
                "terms" : { "field" : "play" },
                "aggs" : {
                  "money" : {
                    "sum": {
                      "field" : "cost",
                      "script": {
                       "source": "doc.cost.value * doc.number.value"
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "actions" : {
      "my_log" : {
        "condition": {                                                <1>
          "script" :
          """
            return ctx.payload.aggregations.theatres.buckets.stream()
              .anyMatch(theatre -> theatre.money.value > 10000)       <2>
          """
        },
        "logging" : {
          "text" : "At least one play has grossed over $10,000: {{ctx.payload.aggregations.theatres.buckets}}"
        }
      }
    }
  }
}
```

This example uses a nearly identical condition as the previous example. The differences below are subtle and are worth calling out.

1. The location of the condition is no longer at the top level, but is within an individual action.
2. Instead of a filter, `anyMatch` is used to return a boolean value


