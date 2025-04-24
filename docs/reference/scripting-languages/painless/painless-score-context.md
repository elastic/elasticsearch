---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-score-context.html
---

# Score context [painless-score-context]

Use a Painless script in a [function score](/reference/query-languages/query-dsl/query-dsl-function-score-query.md) to apply a new score to documents returned from a query.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the current document. For single-valued fields, the value can be accessed via `doc['fieldname'].value`. For multi-valued fields, this returns the first value; other values can be accessed via `doc['fieldname'].get(index)`

`_score` (`double` read-only)
:   The similarity score of the current document.

**Return**

`double`
:   The score for the current document.

**API**

Both the standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) and [Specialized Score API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-score.html) are available.

**Example**

To run this example, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

The following query finds all unsold seats, with lower *row* values scored higher.

```console
GET /seats/_search
{
  "query": {
    "function_score": {
      "query": {
        "match": {
          "sold": "false"
        }
      },
      "script_score": {
        "script": {
          "source": "1.0 / doc['row'].value"
        }
      }
    }
  }
}
```

