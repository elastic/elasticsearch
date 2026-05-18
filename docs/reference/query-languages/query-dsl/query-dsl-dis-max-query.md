---
navigation_title: "Disjunction max"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-dis-max-query.html
---

# Disjunction max query [query-dsl-dis-max-query]


Returns documents matching one or more wrapped queries, called query clauses or clauses.

If a returned document matches multiple query clauses, the `dis_max` query assigns the document the highest relevance score from any matching clause, plus a tie breaking increment for any additional matching subqueries.

## Example request [query-dsl-dis-max-query-ex-request]

```console
GET /_search
{
  "query": {
    "dis_max": {
      "queries": [
        { "term": { "title": "Quick pets" } },
        { "term": { "body": "Quick pets" } }
      ],
      "tie_breaker": 0.7
    }
  }
}
```


## Top-level parameters for `dis_max` [query-dsl-dis-max-query-top-level-params]

`queries`
:   (Required, array of query objects) Contains one or more query clauses. Returned documents **must match one or more** of these queries. If a document matches multiple queries, {{es}} uses the highest [relevance score](/reference/query-languages/query-dsl/query-filter-context.md).

`tie_breaker`
:   (Optional, float) Floating point number between `0` and `1.0` used to increase the [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of documents matching multiple query clauses. Defaults to `0.0`.

You can use the `tie_breaker` value to assign higher relevance scores to documents that contain the same term in multiple fields than documents that contain this term in only the best of those multiple fields, without confusing this with the better case of two different terms in the multiple fields.

If a document matches multiple clauses, the `dis_max` query calculates the relevance score for the document as follows:

1. Take the relevance score from a matching clause with the highest score.
2. Multiply the score from any other matching clauses by the `tie_breaker` value.
3. Add the highest score to the multiplied scores.

If the `tie_breaker` value is greater than `0.0`, all matching clauses count, but the clause with the highest score counts most.
