---
navigation_title: "Constant score"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-constant-score-query.html
---

# Constant score query [query-dsl-constant-score-query]


Wraps a [filter query](/reference/query-languages/query-dsl/query-dsl-bool-query.md) and returns every matching document with a [relevance score](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) equal to the `boost` parameter value.

```console
GET /_search
{
  "query": {
    "constant_score": {
      "filter": {
        "term": { "user.id": "kimchy" }
      },
      "boost": 1.2
    }
  }
}
```

## Top-level parameters for `constant_score` [constant-score-top-level-params]

`filter`
:   (Required, query object) [Filter query](/reference/query-languages/query-dsl/query-dsl-bool-query.md) you wish to run. Any returned documents must match this query.

Filter queries do not calculate [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores). To speed up performance, {{es}} automatically caches frequently used filter queries.


`boost`
:   (Optional, float) Floating point number used as the constant [relevance score](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) for every document matching the `filter` query. Defaults to `1.0`.


