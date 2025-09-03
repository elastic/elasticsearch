---
navigation_title: "Span field masking"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-field-masking-query.html
---

# Span field masking query [query-dsl-span-field-masking-query]


Wrapper to allow span queries to participate in composite single-field span queries by *lying* about their search field.

This can be used to support queries like `span-near` or `span-or` across different fields, which is not ordinarily permitted.

Span field masking query is invaluable in conjunction with **multi-fields** when same content is indexed with multiple analyzers. For instance, we could index a field with the standard analyzer which breaks text up into words, and again with the english analyzer which stems words into their root form.

Example:

```console
GET /_search
{
  "query": {
    "span_near": {
      "clauses": [
        {
          "span_term": {
            "text": "quick brown"
          }
        },
        {
          "span_field_masking": {
            "query": {
              "span_term": {
                "text.stems": "fox" <1>
              }
            },
            "field": "text" <2>
          }
        }
      ],
      "slop": 5,
      "in_order": false
    }
  },
  "highlight": {
    "require_field_match" : false, <3>
    "fields": {
      "*": {}
    }
  }
}
```

1. Original field on which we do the search
2. Masked field, which we are masking with the original field
3. Use "require_field_match" : false to highlight the masked field


Note: `span_field_masking` query may have unexpected scoring and highlighting behaviour. This is because the query returns and highlights the masked field, but scoring and highlighting are done using the terms statistics and offsets of the original field.

Note: For highlighting to work the parameter: `require_field_match` should be set to `false` on the highlighter.

