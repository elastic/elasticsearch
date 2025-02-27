---
navigation_title: "Scripted metric"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-scripted-metric-aggregation.html
---

# Scripted metric aggregation [search-aggregations-metrics-scripted-metric-aggregation]


A metric aggregation that executes using scripts to provide a metric output.

::::{warning}
`scripted_metric` is not available in {{serverless-full}}.
::::


::::{warning}
Using scripts can result in slower search speeds. See [Scripts, caching, and search speed](docs-content://explore-analyze/scripting/scripts-search-speed.md).  When using a scripted metric aggregation, its intermediate state is serialized into an in-memory byte array for transmission to other nodes during the aggregation process. Consequently, a complex scripted metric aggregation may also encounter the 2GB limitation imposed on Java arrays.
::::


Example:

```console
POST ledger/_search?size=0
{
  "query": {
    "match_all": {}
  },
  "aggs": {
    "profit": {
      "scripted_metric": {
        "init_script": "state.transactions = []", <1>
        "map_script": "state.transactions.add(doc.type.value == 'sale' ? doc.amount.value : -1 * doc.amount.value)",
        "combine_script": "double profit = 0; for (t in state.transactions) { profit += t } return profit",
        "reduce_script": "double profit = 0; for (a in states) { profit += a } return profit"
      }
    }
  }
}
```

1. `init_script` is an optional parameter, all other scripts are required.


The above aggregation demonstrates how one would use the script aggregation compute the total profit from sale and cost transactions.

The response for the above aggregation:

```console-result
{
  "took": 218,
  ...
  "aggregations": {
    "profit": {
      "value": 240.0
    }
  }
}
```

The above example can also be specified using stored scripts as follows:

```console
POST ledger/_search?size=0
{
  "aggs": {
    "profit": {
      "scripted_metric": {
        "init_script": {
          "id": "my_init_script"
        },
        "map_script": {
          "id": "my_map_script"
        },
        "combine_script": {
          "id": "my_combine_script"
        },
        "params": {
          "field": "amount"           <1>
        },
        "reduce_script": {
          "id": "my_reduce_script"
        }
      }
    }
  }
}
```

1. script parameters for `init`, `map` and `combine` scripts must be specified in a global `params` object so that it can be shared between the scripts.


For more details on specifying scripts see [script documentation](docs-content://explore-analyze/scripting.md).

## Allowed return types [scripted-metric-aggregation-return-types]

Whilst any valid script object can be used within a single script, the scripts must return or store in the `state` object only the following types:

* primitive types
* String
* Map (containing only keys and values of the types listed here)
* Array (containing elements of only the types listed here)


## Scope of scripts [scripted-metric-aggregation-scope]

The scripted metric aggregation uses scripts at 4 stages of its execution:

init_script
:   Executed prior to any collection of documents. Allows the aggregation to set up any initial state.

    In the above example, the `init_script` creates an array `transactions` in the `state` object.


map_script
:   Executed once per document collected. This is a required script.

    In the above example, the `map_script` checks the value of the type field. If the value is *sale* the value of the amount field is added to the transactions array. If the value of the type field is not *sale* the negated value of the amount field is added to transactions.


combine_script
:   Executed once on each shard after document collection is complete. This is a required script. Allows the aggregation to consolidate the state returned from each shard.

    In the above example, the `combine_script` iterates through all the stored transactions, summing the values in the `profit` variable and finally returns `profit`.


reduce_script
:   Executed once on the coordinating node after all shards have returned their results. This is a required script. The script is provided with access to a variable `states` which is an array of the result of the combine_script on each shard.

    In the above example, the `reduce_script` iterates through the `profit` returned by each shard summing the values before returning the final combined profit which will be returned in the response of the aggregation.



## Worked example [scripted-metric-aggregation-example]

Imagine a situation where you index the following documents into an index with 2 shards:

```console
PUT /transactions/_bulk?refresh
{"index":{"_id":1}}
{"type": "sale","amount": 80}
{"index":{"_id":2}}
{"type": "cost","amount": 10}
{"index":{"_id":3}}
{"type": "cost","amount": 30}
{"index":{"_id":4}}
{"type": "sale","amount": 130}
```

Lets say that documents 1 and 3 end up on shard A and documents 2 and 4 end up on shard B. The following is a breakdown of what the aggregation result is at each stage of the example above.

### Before init_script [_before_init_script]

`state` is initialized as a new empty object.

```js
"state" : {}
```


### After init_script [_after_init_script]

This is run once on each shard before any document collection is performed, and so we will have a copy on each shard:

Shard A
:   ```js
"state" : {
    "transactions" : []
}
```


Shard B
:   ```js
"state" : {
    "transactions" : []
}
```



### After map_script [_after_map_script]

Each shard collects its documents and runs the map_script on each document that is collected:

Shard A
:   ```js
"state" : {
    "transactions" : [ 80, -30 ]
}
```


Shard B
:   ```js
"state" : {
    "transactions" : [ -10, 130 ]
}
```



### After combine_script [_after_combine_script]

The combine_script is executed on each shard after document collection is complete and reduces all the transactions down to a single profit figure for each shard (by summing the values in the transactions array) which is passed back to the coordinating node:

Shard A
:   50

Shard B
:   120


### After reduce_script [_after_reduce_script]

The reduce_script receives a `states` array containing the result of the combine script for each shard:

```js
"states" : [
    50,
    120
]
```

It reduces the responses for the shards down to a final overall profit figure (by summing the values) and returns this as the result of the aggregation to produce the response:

```js
{
  ...

  "aggregations": {
    "profit": {
      "value": 170
    }
  }
}
```



## Other parameters [scripted-metric-aggregation-parameters]

params
:   Optional. An object whose contents will be passed as variables to the  `init_script`, `map_script` and `combine_script`. This can be useful to allow the user to control the behavior of the aggregation and for storing state between the scripts. If this is not specified, the default is the equivalent of providing:

    ```js
    "params" : {}
    ```



## Empty buckets [scripted-metric-aggregation-empty-buckets]

If a parent bucket of the scripted metric aggregation does not collect any documents an empty aggregation response will be returned from the shard with a `null` value. In this case the `reduce_script`'s `states` variable will contain `null` as a response from that shard. `reduce_script`'s should therefore expect and deal with `null` responses from shards.


