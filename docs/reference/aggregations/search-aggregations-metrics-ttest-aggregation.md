---
navigation_title: "T-test"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-ttest-aggregation.html
---

# T-test aggregation [search-aggregations-metrics-ttest-aggregation]


A `t_test` metrics aggregation that performs a statistical hypothesis test in which the test statistic follows a Student’s t-distribution under the null hypothesis on numeric values extracted from the aggregated documents. In practice, this will tell you if the difference between two population means are statistically significant and did not occur by chance alone.

## Syntax [_syntax_6]

A `t_test` aggregation looks like this in isolation:

```js
{
  "t_test": {
    "a": "value_before",
    "b": "value_after",
    "type": "paired"
  }
}
```
% NOTCONSOLE

Assuming that we have a record of node start up times before and after upgrade, let’s look at a t-test to see if upgrade affected the node start up time in a meaningful way.

```console
GET node_upgrade/_search
{
  "size": 0,
  "aggs": {
    "startup_time_ttest": {
      "t_test": {
        "a": { "field": "startup_time_before" },  <1>
        "b": { "field": "startup_time_after" },   <2>
        "type": "paired"                          <3>
      }
    }
  }
}
```
% TEST[setup:node_upgrade]

1. The field `startup_time_before` must be a numeric field.
2. The field `startup_time_after` must be a numeric field.
3. Since we have data from the same nodes, we are using paired t-test.


The response will return the p-value or probability value for the test. It is the probability of obtaining results at least as extreme as the result processed by the aggregation, assuming that the null hypothesis is correct (which means there is no difference between population means). Smaller p-value means the null hypothesis is more likely to be incorrect and population means are indeed different.

```console-result
{
  ...

 "aggregations": {
    "startup_time_ttest": {
      "value": 0.1914368843365979 <1>
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

1. The p-value.



## T-Test Types [_t_test_types]

The `t_test` aggregation supports unpaired and paired two-sample t-tests. The type of the test can be specified using the `type` parameter:

`"type": "paired"`
:   performs paired t-test

`"type": "homoscedastic"`
:   performs two-sample equal variance test

`"type": "heteroscedastic"`
:   performs two-sample unequal variance test (this is default)


## Filters [_filters]

It is also possible to run unpaired t-test on different sets of records using filters. For example, if we want to test the difference of startup times before upgrade between two different groups of nodes, we use the same field `startup_time_before` by separate groups of nodes using terms filters on the group name field:

```console
GET node_upgrade/_search
{
  "size": 0,
  "aggs": {
    "startup_time_ttest": {
      "t_test": {
        "a": {
          "field": "startup_time_before",         <1>
          "filter": {
            "term": {
              "group": "A"                        <2>
            }
          }
        },
        "b": {
          "field": "startup_time_before",         <3>
          "filter": {
            "term": {
              "group": "B"                        <4>
            }
          }
        },
        "type": "heteroscedastic"                 <5>
      }
    }
  }
}
```
% TEST[setup:node_upgrade]

1. The field `startup_time_before` must be a numeric field.
2. Any query that separates two groups can be used here.
3. We are using the same field
4. but we are using different filters.
5. Since we have data from different nodes, we cannot use paired t-test.


```console-result
{
  ...

 "aggregations": {
    "startup_time_ttest": {
      "value": 0.2981858007281437 <1>
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

1. The p-value.


Populations don’t have to be in the same index. If data sets are located in different indices, the term filter on the [`_index`](/reference/elasticsearch/mapping-reference/mapping-index-field.md) field can be used to select populations.


## Script [_script_15]

If you need to run the `t_test` on values that aren’t represented cleanly by a field you should, run the aggregation on a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md). For example, if you want to adjust out load times for the before values:

```console
GET node_upgrade/_search
{
  "size": 0,
  "runtime_mappings": {
    "startup_time_before.adjusted": {
      "type": "long",
      "script": {
        "source": "emit(doc['startup_time_before'].value - params.adjustment)",
        "params": {
          "adjustment": 10
        }
      }
    }
  },
  "aggs": {
    "startup_time_ttest": {
      "t_test": {
        "a": {
          "field": "startup_time_before.adjusted"
        },
        "b": {
          "field": "startup_time_after"
        },
        "type": "paired"
      }
    }
  }
}
```
% TEST[setup:node_upgrade]
% TEST[s/_search/_search\?filter_path=aggregations/]


