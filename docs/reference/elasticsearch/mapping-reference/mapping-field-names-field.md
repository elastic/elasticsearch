---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-field-names-field.html
---

# _field_names field [mapping-field-names-field]

The `_field_names` field used to index the names of every field in a document that contains any value other than `null`. This field was used by the [`exists`](/reference/query-languages/query-dsl/query-dsl-exists-query.md) query to find documents that either have or don’t have any non-`null` value for a particular field.

Now the `_field_names` field only indexes the names of fields that have `doc_values` and `norms` disabled. For fields which have either `doc_values` or `norm` enabled the [`exists`](/reference/query-languages/query-dsl/query-dsl-exists-query.md) query will still be available but will not use the `_field_names` field.

## Disabling `_field_names` [disable-field-names]

Disabling `_field_names` is no longer possible. It is now enabled by default because it no longer carries the index overhead it once did.

::::{note}
Support for disabling `_field_names` has been removed. Using it on new indices will throw an error. Using it in pre-8.0 indices is still allowed but issues a deprecation warning.
::::



