---
navigation_title: "Boosting"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html
---

# Boosting query [query-dsl-boosting-query]


Returns documents matching a `positive` query while reducing the [relevance score](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of documents that also match a `negative` query.

You can use the `boosting` query to demote certain documents without excluding them from the search results.

## Example request [boosting-query-ex-request]

```console
GET /_search
{
  "query": {
    "boosting": {
      "positive": {
        "term": {
          "text": "apple"
        }
      },
      "negative": {
        "term": {
          "text": "pie tart fruit crumble tree"
        }
      },
      "negative_boost": 0.5
    }
  }
}
```


## Top-level parameters for `boosting` [boosting-top-level-params]

`positive`
:   (Required, query object) Query you wish to run. Any returned documents must match this query.

`negative`
:   (Required, query object) Query used to decrease the [relevance score](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of matching documents.

If a returned document matches the `positive` query and this query, the `boosting` query calculates the final [relevance score](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) for the document as follows:

1. Take the original relevance score from the `positive` query.
2. Multiply the score by the `negative_boost` value.


`negative_boost`
:   (Required, float) Floating point number between `0` and `1.0` used to decrease the [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of documents matching the `negative` query.


