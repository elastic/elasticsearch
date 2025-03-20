---
navigation_title: "Filter"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-filter-aggregation.html
---

# Filter aggregation [search-aggregations-bucket-filter-aggregation]


A single bucket aggregation that narrows the set of documents to those that match a [query](/reference/query-languages/querydsl.md).

Example:

$$$filter-aggregation-example$$$

```console
POST /sales/_search?size=0&filter_path=aggregations
{
  "aggs": {
    "avg_price": { "avg": { "field": "price" } },
    "t_shirts": {
      "filter": { "term": { "type": "t-shirt" } },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } }
      }
    }
  }
}
```

The previous example calculates the average price of all sales as well as the average price of all T-shirt sales.

Response:

```console-result
{
  "aggregations": {
    "avg_price": { "value": 140.71428571428572 },
    "t_shirts": {
      "doc_count": 3,
      "avg_price": { "value": 128.33333333333334 }
    }
  }
}
```

## Use a top-level `query` to limit all aggregations [use-top-level-query-to-limit-all-aggs]

To limit the documents on which all aggregations in a search run, use a top-level `query`. This is faster than a single `filter` aggregation with sub-aggregations.

For example, use this:

$$$filter-aggregation-top-good$$$

```console
POST /sales/_search?size=0&filter_path=aggregations
{
  "query": { "term": { "type": "t-shirt" } },
  "aggs": {
    "avg_price": { "avg": { "field": "price" } }
  }
}
```

Instead of this:

$$$filter-aggregation-top-bad$$$

```console
POST /sales/_search?size=0&filter_path=aggregations
{
  "aggs": {
    "t_shirts": {
      "filter": { "term": { "type": "t-shirt" } },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } }
      }
    }
  }
}
```


## Use the `filters` aggregation for multiple filters [use-filters-agg-for-multiple-filters]

To group documents using multiple filters, use the [`filters` aggregation](/reference/aggregations/search-aggregations-bucket-filters-aggregation.md). This is faster than multiple `filter` aggregations.

For example, use this:

$$$filter-aggregation-many-good$$$

```console
POST /sales/_search?size=0&filter_path=aggregations
{
  "aggs": {
    "f": {
      "filters": {
        "filters": {
          "hats": { "term": { "type": "hat" } },
          "t_shirts": { "term": { "type": "t-shirt" } }
        }
      },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } }
      }
    }
  }
}
```

Instead of this:

$$$filter-aggregation-many-bad$$$

```console
POST /sales/_search?size=0&filter_path=aggregations
{
  "aggs": {
    "hats": {
      "filter": { "term": { "type": "hat" } },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } }
      }
    },
    "t_shirts": {
      "filter": { "term": { "type": "t-shirt" } },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } }
      }
    }
  }
}
```


