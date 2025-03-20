---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-watcher-transform-context.html
---

# Watcher transform context [painless-watcher-transform-context]

Use a Painless script as a [watch transform](docs-content://explore-analyze/alerts-cases/watcher/transform-script.md) to transform a payload into a new payload for further use in the watch. Transform scripts return an Object value of the new payload.

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

`Object`
:   The new payload.

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
            "query" : { "term": { "sold": "true"} },
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
    "transform" : {
      "script":
      """
        return [
          'money_makers': ctx.payload.aggregations.theatres.buckets.stream()  <1>
            .filter(t -> {                                                    <2>
                return t.money.value > 50000
            })
            .map(t -> {                                                       <3>
                return ['play': t.key, 'total_value': t.money.value ]
            }).collect(Collectors.toList()),                                  <4>
          'duds' : ctx.payload.aggregations.theatres.buckets.stream()         <5>
            .filter(t -> {
                return t.money.value < 15000
            })
            .map(t -> {
                return ['play': t.key, 'total_value': t.money.value ]
            }).collect(Collectors.toList())
          ]
      """
    },
    "actions" : {
      "my_log" : {
        "logging" : {
          "text" : "The output of the payload was transformed to {{ctx.payload}}"
        }
      }
    }
  }
}
```

1. The Java Stream API is used in the transform. This API allows manipulation of the elements of the list in a pipeline.
2. The stream filter removes items that do not meet the filter criteria.
3. The stream map transforms each element into a new object.
4. The collector reduces the stream to a `java.util.List`.
5. This is done again for the second set of values in the transform.


The following action transform changes each value in the mod_log action into a `String`. This transform does not change the values in the unmod_log action.

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
    "actions" : {
      "mod_log" : {
        "transform": {                                                                <1>
          "script" :
          """
          def formatter = NumberFormat.getCurrencyInstance();
          return [
            'msg': ctx.payload.aggregations.theatres.buckets.stream()
              .map(t-> formatter.format(t.money.value) + ' for the play ' + t.key)
              .collect(Collectors.joining(", "))
          ]
          """
        },
        "logging" : {
          "text" : "The output of the payload was transformed to: {{ctx.payload.msg}}"
        }
      },
      "unmod_log" : {                                                                 <2>
        "logging" : {
          "text" : "The output of the payload was not transformed and this value should not exist: {{ctx.payload.msg}}"
        }
      }
    }
  }
}
```

This example uses the streaming API in a very similar manner. The differences below are subtle and worth calling out.

1. The location of the transform is no longer at the top level, but is within an individual action.
2. A second action that does not transform the payload is given for reference.


The following example shows scripted watch and action transforms within the context of a complete watch. This watch also uses a scripted [condition](/reference/scripting-languages/painless/painless-watcher-condition-context.md).

```console
POST _watcher/watch/_execute
{
  "watch" : {
    "metadata" : { "high_threshold": 4000, "low_threshold": 1000 },
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
    "condition" : {
      "script" :
      """
        return ctx.payload.aggregations.theatres.buckets.stream()
          .anyMatch(theatre -> theatre.money.value < ctx.metadata.low_threshold ||
                               theatre.money.value > ctx.metadata.high_threshold)
      """
    },
    "transform" : {
      "script":
      """
        return [
          'money_makers': ctx.payload.aggregations.theatres.buckets.stream()
            .filter(t -> {
                return t.money.value > ctx.metadata.high_threshold
            })
            .map(t -> {
                return ['play': t.key, 'total_value': t.money.value ]
            }).collect(Collectors.toList()),
          'duds' : ctx.payload.aggregations.theatres.buckets.stream()
            .filter(t -> {
                return t.money.value < ctx.metadata.low_threshold
            })
            .map(t -> {
                return ['play': t.key, 'total_value': t.money.value ]
            }).collect(Collectors.toList())
          ]
      """
    },
    "actions" : {
      "log_money_makers" : {
        "condition": {
          "script" : "return ctx.payload.money_makers.size() > 0"
        },
        "transform": {
          "script" :
          """
          def formatter = NumberFormat.getCurrencyInstance();
          return [
            'plays_value': ctx.payload.money_makers.stream()
              .map(t-> formatter.format(t.total_value) + ' for the play ' + t.play)
              .collect(Collectors.joining(", "))
          ]
          """
        },
        "logging" : {
          "text" : "The following plays contain the highest grossing total income: {{ctx.payload.plays_value}}"
        }
      },
      "log_duds" : {
        "condition": {
          "script" : "return ctx.payload.duds.size() > 0"
        },
        "transform": {
          "script" :
          """
          def formatter = NumberFormat.getCurrencyInstance();
          return [
            'plays_value': ctx.payload.duds.stream()
              .map(t-> formatter.format(t.total_value) + ' for the play ' + t.play)
              .collect(Collectors.joining(", "))
          ]
          """
        },
        "logging" : {
          "text" : "The following plays need more advertising due to their low total income: {{ctx.payload.plays_value}}"
        }
      }
    }
  }
}
```

The following example shows the use of metadata and transforming dates into a readable format.

```console
POST _watcher/watch/_execute
{
  "watch" : {
    "metadata" : { "min_hits": 10 },
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
        return ctx.payload.hits.total > ctx.metadata.min_hits
      """
    },
    "transform" : {
      "script" :
      """
        def theDate = ZonedDateTime.ofInstant(ctx.execution_time.toInstant(), ctx.execution_time.getZone());
        return ['human_date': DateTimeFormatter.RFC_1123_DATE_TIME.format(theDate),
                'aggregations': ctx.payload.aggregations]
      """
    },
    "actions" : {
      "my_log" : {
        "logging" : {
          "text" : "The watch was successfully executed on {{ctx.payload.human_date}} and contained {{ctx.payload.aggregations.theatres.buckets.size}} buckets"
        }
      }
    }
  }
}
```

