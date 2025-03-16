---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-update-by-query-context.html
---

# Update by query context [painless-update-by-query-context]

Use a Painless script in an [update by query](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query) operation to add, modify, or delete fields within each of a set of documents collected as the result of query.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`ctx['op']` (`String`)
:   The name of the operation.

[`ctx['_routing']`](/reference/elasticsearch/mapping-reference/mapping-routing-field.md) (`String`, read-only)
:   The value used to select a shard for document storage.

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md) (`String`, read-only)
:   The name of the index.

[`ctx['_id']`](/reference/elasticsearch/mapping-reference/mapping-id-field.md) (`String`, read-only)
:   The unique document id.

`ctx['_version']` (`int`, read-only)
:   The current version of the document.

[`ctx['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) (`Map`)
:   Contains extracted JSON in a `Map` and `List` structure for the fields existing in a stored document.

**Side Effects**

`ctx['op']`
:   Use the default of `index` to update a document. Set to `none` to specify no operation or `delete` to delete the current document from the index.

[`ctx['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md)
:   Modify the values in the `Map/List` structure to add, modify, or delete the fields of a document.

**Return**

`void`
:   No expected return value.

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

**Example**

To run this example, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

The following query finds all seats in a specific section that have not been sold and lowers the price by 2:

```console
POST /seats/_update_by_query
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "row": {
              "lte": 3
            }
          }
        },
        {
          "match": {
            "sold": false
          }
        }
      ]
    }
  },
  "script": {
    "source": "ctx._source.cost -= params.discount",
    "lang": "painless",
    "params": {
      "discount": 2
    }
  }
}
```

