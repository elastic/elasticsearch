---
navigation_title: "Rank feature"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rank-feature.html
---

# Rank feature field type [rank-feature]


A `rank_feature` field can index numbers so that they can later be used to boost documents in queries with a [`rank_feature`](/reference/query-languages/query-dsl/query-dsl-rank-feature-query.md) query.

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "pagerank": {
        "type": "rank_feature" <1>
      },
      "url_length": {
        "type": "rank_feature",
        "positive_score_impact": false <2>
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "pagerank": 8,
  "url_length": 22
}

GET my-index-000001/_search
{
  "query": {
    "rank_feature": {
      "field": "pagerank"
    }
  }
}
```

1. Rank feature fields must use the `rank_feature` field type
2. Rank features that correlate negatively with the score need to declare it


::::{note}
`rank_feature` fields only support single-valued fields and strictly positive values. Multi-valued fields and negative values will be rejected.
::::


::::{note}
`rank_feature` fields do not support querying, sorting or aggregating. They may only be used within [`rank_feature`](/reference/query-languages/query-dsl/query-dsl-rank-feature-query.md) queries.
::::


::::{note}
`rank_feature` fields only preserve 9 significant bits for the precision, which translates to a relative error of about 0.4%.
::::


Rank features that correlate negatively with the score should set `positive_score_impact` to `false` (defaults to `true`). This will be used by the [`rank_feature`](/reference/query-languages/query-dsl/query-dsl-rank-feature-query.md) query to modify the scoring formula in such a way that the score decreases with the value of the feature instead of increasing. For instance in web search, the url length is a commonly used feature which correlates negatively with scores.

