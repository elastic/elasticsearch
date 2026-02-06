---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-metric-agg-init-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Metric aggregation initialization context [painless-metric-agg-init-context]

Use a Painless script to [initialize](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) values for use in a scripted metric aggregation. An initialization script is run prior to document collection once per shard and is optional as part of the full metric aggregation.

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`state` (`Map`)
:   Empty `Map` used to add values for use in a [map script](/reference/scripting-languages/painless/painless-metric-agg-map-context.md).

## Side Effects

`state` (`Map`)
:   Add values to this `Map` to for use in a map. Additional values must be of the type `Map`, `List`, `String` or primitive.

## Return

`void`
:   No expected return value.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

:::{tip}
This example demonstrates a complete scripted metric aggregation pipeline that works across all four metric aggregation contexts (initialization -> map -> combine -> reduce). You’ll find this same example in the other metric aggregation contexts, with each highlighting its specific phase. You are viewing Phase 1 of 4 in the scripted metric aggregation pipeline.
:::

In the following example, we build a query that analyzes the data to calculate the total number of products sold across all orders, using the map-reduce pattern where each shard processes documents locally and results are combined into a final total.

**>Initialization phase (this context \- sets up data structures):**  
The first code snippet is part of the `init_script` that initializes an empty array to collect quantity values from each document. It runs once per shard.

```java
state.quantities = []
```

**Map phase (processes each document):**  
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
