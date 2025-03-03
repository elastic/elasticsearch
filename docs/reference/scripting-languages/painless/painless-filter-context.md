---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-filter-context.html
---

# Filter context [painless-filter-context]

Use a Painless script as a [filter](/reference/query-languages/query-dsl-script-query.md) in a query to include and exclude documents.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the current document where each field is a `List` of values.

**Return**

`boolean`
:   Return `true` if the current document should be returned as a result of the query, and `false` otherwise.

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

**Example**

To run this example, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

This script finds all unsold documents that cost less than $25.

```painless
doc['sold'].value == false && doc['cost'].value < 25
```

Defining `cost` as a script parameter enables the cost to be configured in the script query request. For example, the following request finds all available theatre seats for evening performances that are under $25.

```console
GET seats/_search
{
  "query": {
    "bool": {
      "filter": {
        "script": {
          "script": {
            "source": "doc['sold'].value == false && doc['cost'].value < params.cost",
            "params": {
              "cost": 25
            }
          }
        }
      }
    }
  }
}
```

