---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-metric-agg-map-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Metric aggregation map context [painless-metric-agg-map-context]

Use a Painless script to [map](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) values for use in a scripted metric aggregation. A map script is run once per collected document following an optional [initialization script](/reference/scripting-languages/painless/painless-metric-agg-init-context.md) and is required as part of a full metric aggregation.

:::{warning}  
`scripted_metric` is not available in {{serverless-full}}.  
:::


## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`state` (`Map`)
:   `Map` used to add values for processing in a [combine script](./painless-metric-agg-combine-context.md) or to be returned from the aggregation.

`doc` (`Map`, read-only)
:   Contains the fields of the current document where each field is a `List` of values.

`_score` (`double` read-only)
:   The similarity score of the current document.

## Side Effects

`state` (`Map`)
:   Use this `Map` to add values for processing in a combine script. Additional values must be of the type `Map`, `List`, `String` or primitive. The same `state` `Map` is shared between all aggregated documents on a given shard. If an initialization script is provided as part of the aggregation then values added from the initialization script are available. If no combine script is specified, values must be directly stored in `state` in a usable form. If no combine script and no [reduce script](/reference/scripting-languages/painless/painless-metric-agg-reduce-context.md) are specified, the `state` values are used as the result.

## Return

`void`
:   No expected return value.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

:::{tip}
You are viewing Phase 2 of 4 in the scripted metric aggregation pipeline. This combined script runs once per shard to prepare results for the final reduce phase. This is the same complete example shown across all metric aggregation contexts.
:::

In the following example, we build a query that analyzes the data to calculate the total number of products sold across all orders, using the map-reduce pattern where each shard processes documents locally and results are combined into a final total.

**Initialization phase (sets up data structures):**  
The first code snippet is part of the `init_script` that initializes an empty array to collect quantity values from each document. It runs once per shard.

```java
state.quantities = []
```

**>Map phase (this context - processes each document):**  
The code in the `map_script` section runs for each document. It extracts the total quantity of products in each order and adds it to the shard's collection array.

```java
state.quantities.add(doc['total_quantity'].value)
```

**Combine phase (returns shard results):**  
The `combine_script` processes all the quantities collected in this shard by iterating through the array and summing all values. This reduces the data sent to the reduce phase from an array of individual quantities to a single total per shard.

```java
int shardTotal = 0;
 
for (qty in state.quantities) {
  shardTotal += qty;
}
 
return shardTotal;
```

**Reduce phase (merges all shard results):**  
Finally, the `reduce_script` merges results from all shards by iterating through each shard's total, and adds the results together to get the grand total of products sold across the entire dataset.

```java
int grandTotal = 0;
 
for (shardTotal in states) {
  grandTotal += shardTotal;
}
 
return grandTotal;
```

The complete request looks like this:

```json
GET kibana_sample_data_ecommerce/_search
{
  "size": 0,
  "aggs": {
	"total_quantity_sold": {
  	"scripted_metric": {
    	"init_script": "state.quantities = []",
    	"map_script": "state.quantities.add(doc['total_quantity'].value)",
    	"combine_script": """
          int shardTotal = 0;
 
          for (qty in state.quantities) {
            shardTotal += qty;
          }
 
          return shardTotal;
        """,
    	"reduce_script": """
          int grandTotal = 0;
 
          for (shardTotal in states) {
            grandTotal += shardTotal;
          }
          
          return grandTotal;
        """
      }
    }
  }
}
```
