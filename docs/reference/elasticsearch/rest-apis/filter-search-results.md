---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/filter-search-results.html
applies_to:
  stack: all
---

# Filter search results [filter-search-results]

The [search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) lets you filter results using either boolean `filter` clauses or the `post_filter` parameter.

You can use two methods to filter search results:

* Use a boolean query with a `filter` clause. Search requests apply [boolean filters](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to both search hits and [aggregations](/reference/aggregations/index.md).
* Use the search API’s `post_filter` parameter. Search requests apply [post filters](#post-filter) only to search hits, not aggregations. You can use a post filter to calculate aggregations based on a broader result set, and then further narrow the results.

## Post filter [post-filter]

When you use the `post_filter` parameter to filter search results, the search hits are filtered after the aggregations are calculated. A post filter has no impact on the aggregation results.

For example, you are selling shirts that have the following properties:

```console
PUT /shirts
{
  "mappings": {
    "properties": {
      "brand": { "type": "keyword"},
      "color": { "type": "keyword"},
      "model": { "type": "keyword"}
    }
  }
}

PUT /shirts/_doc/1?refresh
{
  "brand": "gucci",
  "color": "red",
  "model": "slim"
}
```
% TESTSETUP

Imagine a user has specified two filters:

`color:red` and `brand:gucci`. You only want to show them red shirts made by Gucci in the search results. Normally you would do this with a [`bool` query](/reference/query-languages/query-dsl/query-dsl-bool-query.md):

```console
GET /shirts/_search
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "color": "red"   }},
        { "term": { "brand": "gucci" }}
      ]
    }
  }
}
```

However, you would also like to use *faceted navigation* to display a list of other options that the user could click on. Perhaps you have a `model` field that would allow the user to limit their search results to red Gucci `t-shirts` or `dress-shirts`.

This can be done with a [`terms` aggregation](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md):

```console
GET /shirts/_search
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "color": "red"   }},
        { "term": { "brand": "gucci" }}
      ]
    }
  },
  "aggs": {
    "models": {
      "terms": { "field": "model" } <1>
    }
  }
}
```

1. Returns the most popular models of red shirts by Gucci.


But perhaps you would also like to tell the user how many Gucci shirts are available in **other colors**. If you just add a `terms` aggregation on the `color` field, you will only get back the color `red`, because your query returns only red shirts by Gucci.

Instead, you want to include shirts of all colors during aggregation, then apply the `colors` filter only to the search results. This is the purpose of the `post_filter`:

```console
GET /shirts/_search
{
  "query": {
    "bool": {
      "filter": {
        "term": { "brand": "gucci" } <1>
      }
    }
  },
  "aggs": {
    "colors": {
      "terms": { "field": "color" } <2>
    },
    "color_red": {
      "filter": {
        "term": { "color": "red" } <3>
      },
      "aggs": {
        "models": {
          "terms": { "field": "model" } <3>
        }
      }
    }
  },
  "post_filter": { <4>
    "term": { "color": "red" }
  }
}
```

1. The main query now finds all shirts by Gucci, regardless of color.
2. The `colors` agg returns popular colors for shirts by Gucci.
3. The `color_red` agg limits the `models` sub-aggregation to **red** Gucci shirts.
4. Finally, the `post_filter` removes colors other than red from the search `hits`.
