---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-reindex-context.html
---

# Reindex context [painless-reindex-context]

Use a Painless script in a [reindex](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex) operation to add, modify, or delete fields within each document in an original index as its reindexed into a target index.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`ctx['op']` (`String`)
:   The name of the operation.

[`ctx['_routing']`](/reference/elasticsearch/mapping-reference/mapping-routing-field.md) (`String`)
:   The value used to select a shard for document storage.

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md) (`String`)
:   The name of the index.

[`ctx['_id']`](/reference/elasticsearch/mapping-reference/mapping-id-field.md) (`String`)
:   The unique document id.

`ctx['_version']` (`int`)
:   The current version of the document.

[`ctx['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) (`Map`)
:   Contains extracted JSON in a `Map` and `List` structure for the fields existing in a stored document.

**Side Effects**

`ctx['op']`
:   Use the default of `index` to update a document. Set to `noop` to specify no operation or `delete` to delete the current document from the index.

[`ctx['_routing']`](/reference/elasticsearch/mapping-reference/mapping-routing-field.md)
:   Modify this to change the routing value for the current document.

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md)
:   Modify this to change the destination index for the current document.

[`ctx['_id']`](/reference/elasticsearch/mapping-reference/mapping-id-field.md)
:   Modify this to change the id for the current document.

`ctx['_version']` (`int`)
:   Modify this to modify the version for the current document.

[`ctx['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md)
:   Modify the values in the `Map/List` structure to add, modify, or delete the fields of a document.

**Return**

`void`
:   No expected return value.

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

