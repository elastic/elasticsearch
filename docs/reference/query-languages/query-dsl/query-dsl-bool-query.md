---
navigation_title: "Boolean"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html
---

# Boolean query [query-dsl-bool-query]


A query that matches documents matching boolean combinations of other queries. The bool query maps to Lucene `BooleanQuery`. It is built using one or more boolean clauses, each clause with a typed occurrence. The occurrence types are:

| Occur | Description |
| --- | --- |
| `must` | The clause (query) must appear in matching documents and will contribute to the score. Each query defined under a `must` acts as a logical "AND", returning only documents that match *all* the specified queries. |
| `should` | The clause (query) should appear in the matching document. Each query defined under a `should` acts as a logical "OR", returning documents that match *any* of the specified queries. |
| `filter` | The clause (query) must appear in matching documents. However unlike`must` the score of the query will be ignored. Filter clauses are executedin [filter context](/reference/query-languages/query-dsl/query-filter-context.md), meaning that scoring is ignoredand clauses are considered for caching. Each query defined under a `filter` acts as a logical "AND", returning only documents that match *all* the specified queries. |
| `must_not` | The clause (query) must not appear in the matchingdocuments. Clauses are executed in [filter context](/reference/query-languages/query-dsl/query-filter-context.md) meaningthat scoring is ignored and clauses are considered for caching. Because scoring isignored, a score of `0` for all documents is returned. Each query defined under a `must_not` acts as a logical "NOT", returning only documents that do not match any of the specified queries. |

The `must` and `should` clauses function as logical AND, OR operators, contributing to the scoring of results. However, these results are not cached, which means repeated queries won't benefit from faster retrieval. In contrast, the `filter` and `must_not` clauses are used to include or exclude results without impacting the score, unless used within a `constant_score` query.

The `bool` query takes a *more-matches-is-better* approach, so the score from each matching `must` or `should` clause will be added together to provide the final `_score` for each document.

```console
POST _search
{
  "query": {
    "bool" : {
      "must" : {
        "term" : { "user.id" : "kimchy" }
      },
      "filter": {
        "term" : { "tags" : "production" }
      },
      "must_not" : {
        "range" : {
          "age" : { "gte" : 10, "lte" : 20 }
        }
      },
      "should" : [
        { "term" : { "tags" : "env1" } },
        { "term" : { "tags" : "deployed" } }
      ],
      "minimum_should_match" : 1,
      "boost" : 1.0
    }
  }
}
```

## Using `minimum_should_match` [bool-min-should-match]

You can use the `minimum_should_match` parameter to specify the number or percentage of `should` clauses returned documents *must* match.

If the `bool` query includes at least one `should` clause and no `must` or `filter` clauses, the default value is `1`. Otherwise, the default value is `0`.

For other valid values, see the [`minimum_should_match` parameter](/reference/query-languages/query-dsl/query-dsl-minimum-should-match.md).

## Nested bool queries [nested-bool-queries]

You can nest `bool` queries within other `bool` queries to create complex logical constructs. This allows you to build sophisticated search conditions by combining multiple levels of boolean logic.

For example:

```console
GET /_search
{
  "query": {
    "bool": {
      "must": [ <1>
        {
          "bool": {
            "should": [
              { "match": { "user.id": "kimchy" }},
              { "match": { "user.id": "banon" }}
            ]
          }
        },
        { "match": { "tags": "production" }}
      ],
      "should": [ <2>
        {
          "bool": {
            "must": [
              { "match": { "status": "active" }},
              { "match": { "title": "quick brown fox" }}
            ]
          }
        }
      ]
    }
  }
}
```
1. Only documents that match all conditions in the must section will be returned in the results. This means documents must match either "kimchy" OR "banon" in the user.id field AND "production" in the tags field. It is semantically equivalent to (user.id="kimchy" OR user.id="banon") AND tags="production"

2. Matches in the `should` clauses are optional. They will only boost the relevance scores of documents that already match the required `must` criteria and don't add new documents to the result set. It is semantically equivalent to (status="active" AND title="quick brown fox")

You can use the `minimum_should_match` parameter to require matches from the `should` clauses.

::::{note}
While nesting `bool` queries can be powerful, it can also lead to complex and slow queries. Try to keep your queries as flat as possible for the best performance.
::::


## Scoring with `bool.filter` [score-bool-filter]

Queries specified under the `filter` element have no effect on scoring — scores are returned as `0`. Scores are only affected by the query that has been specified. For instance, all three of the following queries return all documents where the `status` field contains the term `active`.

This first query assigns a score of `0` to all documents, as no scoring query has been specified:

```console
GET _search
{
  "query": {
    "bool": {
      "filter": {
        "term": {
          "status": "active"
        }
      }
    }
  }
}
```

This `bool` query has a `match_all` query, which assigns a score of `1.0` to all documents.

```console
GET _search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "term": {
          "status": "active"
        }
      }
    }
  }
}
```

This `constant_score` query behaves in exactly the same way as the second example above. The `constant_score` query assigns a score of `1.0` to all documents matched by the filter.

```console
GET _search
{
  "query": {
    "constant_score": {
      "filter": {
        "term": {
          "status": "active"
        }
      }
    }
  }
}
```


## Named queries [named-queries]

Each query accepts a `_name` in its top level definition. You can use named queries to track which queries matched returned documents. If named queries are used, the response includes a `matched_queries` property for each hit.

::::{note}
Supplying duplicate `_name` values in the same request results in undefined behavior. Queries with duplicate names may overwrite each other. Query names are assumed to be unique within a single request.
::::


```console
GET /_search
{
  "query": {
    "bool": {
      "should": [
        { "match": { "name.first": { "query": "shay", "_name": "first" } } },
        { "match": { "name.last": { "query": "banon", "_name": "last" } } }
      ],
      "filter": {
        "terms": {
          "name.last": [ "banon", "kimchy" ],
          "_name": "test"
        }
      }
    }
  }
}
```

The request parameter named `include_named_queries_score` controls whether scores associated with the matched queries are returned or not. When set, the response includes a `matched_queries` map that contains the name of the query that matched as a key and its associated score as the value.

::::{warning}
Note that the score might not have contributed to the final score of the document, for instance named queries that appear in a filter or must_not contexts, or inside a clause that ignores or modifies the score like `constant_score` or `function_score_query`.
::::


```console
GET /_search?include_named_queries_score
{
  "query": {
    "bool": {
      "should": [
        { "match": { "name.first": { "query": "shay", "_name": "first" } } },
        { "match": { "name.last": { "query": "banon", "_name": "last" } } }
      ],
      "filter": {
        "terms": {
          "name.last": [ "banon", "kimchy" ],
          "_name": "test"
        }
      }
    }
  }
}
```

::::{note}
This functionality reruns each named query on every hit in a search response. Typically, this adds a small overhead to a request. However, using computationally expensive named queries on a large number of hits may add significant overhead. For example, named queries in combination with a `top_hits` aggregation on many buckets may lead to longer response times.
::::



