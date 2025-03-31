---
navigation_title: "Global"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-global-aggregation.html
---

# Global aggregation [search-aggregations-bucket-global-aggregation]


Defines a single bucket of all the documents within the search execution context. This context is defined by the indices and the document types you’re searching on, but is **not** influenced by the search query itself.

::::{note}
Global aggregators can only be placed as top level aggregators because it doesn’t make sense to embed a global aggregator within another bucket aggregator.
::::


Example:

$$$global-aggregation-example$$$

```console
POST /sales/_search?size=0
{
  "query": {
    "match": { "type": "t-shirt" }
  },
  "aggs": {
    "all_products": {
      "global": {}, <1>
      "aggs": {     <2>
      "avg_price": { "avg": { "field": "price" } }
      }
    },
    "t_shirts": { "avg": { "field": "price" } }
  }
}
```

1. The `global` aggregation has an empty body
2. The sub-aggregations that are registered for this `global` aggregation


The above aggregation demonstrates how one would compute aggregations (`avg_price` in this example) on all the documents in the search context, regardless of the query (in our example, it will compute the average price over all products in our catalog, not just on the "shirts").

The response for the above aggregation:

```console-result
{
  ...
  "aggregations": {
    "all_products": {
      "doc_count": 7, <1>
      "avg_price": {
        "value": 140.71428571428572 <2>
      }
    },
    "t_shirts": {
      "value": 128.33333333333334 <3>
    }
  }
}
```

1. The number of documents that were aggregated (in our case, all documents within the search context)
2. The average price of all products in the index
3. The average price of all t-shirts


