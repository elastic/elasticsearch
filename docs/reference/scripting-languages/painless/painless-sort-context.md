---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-sort-context.html
---

# Sort context [painless-sort-context]

Use a Painless script to [sort](/reference/elasticsearch/rest-apis/sort-search-results.md) the documents in a query.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the current document. For single-valued fields, the value can be accessed via `doc['fieldname'].value`. For multi-valued fields, this returns the first value; other values can be accessed via `doc['fieldname'].get(index)`

`_score` (`double` read-only)
:   The similarity score of the current document.

**Return**

`double` or `String`
:   The sort key. The return type depends on the value of the `type` parameter in the script sort config (`"number"` or `"string"`).

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

**Example**

To run this example, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

To sort results by the length of the `theatre` field, submit the following query:

```console
GET /_search
{
  "query": {
    "term": {
      "sold": "true"
    }
  },
  "sort": {
    "_script": {
      "type": "number",
      "script": {
        "lang": "painless",
        "source": "doc['theatre'].value.length() * params.factor",
        "params": {
          "factor": 1.1
        }
      },
      "order": "asc"
    }
  }
}
```

