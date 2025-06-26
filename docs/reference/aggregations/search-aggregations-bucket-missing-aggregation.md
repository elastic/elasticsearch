---
navigation_title: "Missing"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-missing-aggregation.html
---

# Missing aggregation [search-aggregations-bucket-missing-aggregation]


A field data based single bucket aggregation, that creates a bucket of all documents in the current document set context that are missing a field value (effectively, missing a field or having the configured NULL value set). This aggregator will often be used in conjunction with other field data bucket aggregators (such as ranges) to return information for all the documents that could not be placed in any of the other buckets due to missing field data values.

Example:

$$$missing-aggregation-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "products_without_a_price": {
      "missing": { "field": "price" }
    }
  }
}
```

In the above example, we get the total number of products that do not have a price.

Response:

```console-result
{
  ...
  "aggregations": {
    "products_without_a_price": {
      "doc_count": 0
    }
  }
}
```

