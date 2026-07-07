---
applies_to:
  stack: preview 9.3
  serverless: preview
navigation_title: "Unmapped fields"
description: How ES|QL queries fields that aren't in the index mapping, using the SET unmapped_fields directive.
---

# Query unmapped fields without reindexing [esql-unmapped-fields]

{{esql}} can query fields that are not defined in your index mapping, with no reindex and no change to the mapping. The [`SET unmapped_fields`](directives/set.md#esql-unmapped_fields) directive controls how each query handles them.

An unmapped field is a field in indexed documents that the index' mapping does not define. By default, {{esql}} treats such fields as `null` and returns an error if a referenced field is not mapped in any index.

Without this capability, the usual fix is to add the field to your mapping and reindex your data before you can query it. On a large dataset, that reindex can take hours.

## Use cases [esql-unmapped-fields-use-cases]

Querying unmapped fields helps in several common situations where the mapping does not yet match the data you want to explore:

- **Explore new fields before mapping them**: query the values of a field as soon as it appears, whether it arrives unexpectedly from an integration or an upstream change, then decide later whether the performance gains of formal mapping justify a reindex.
- **Read the real values of a partially mapped field**: when a field is mapped in some indices but not others, get its values everywhere in a single query, including the indices where it is unmapped.
- **Run reusable queries across datasets**: let a query continue when a field is not mapped in the data it runs against. This is useful for shared or saved queries and for data streams whose mappings change between rollovers. You can either ignore the missing data by returning `null` or try to load its values from `_source`.

:::{tip}
To extract a value from a JSON string or directly from `_source`, use [`JSON_EXTRACT`](functions-operators/string-functions/json_extract.md) {applies_to}`stack: preview =9.4, ga 9.5+`.

To extract a subfield from a [`flattened`](/reference/elasticsearch/mapping-reference/flattened.md) field, use [`FIELD_EXTRACT`](functions-operators/string-functions/field_extract.md) {applies_to}`stack: preview 9.5` {applies_to}`serverless: preview`.
:::

## How {{esql}} handles unmapped fields [esql-unmapped-fields-how-it-works]

The `unmapped_fields` setting accepts three values, which range from strict to permissive. These values also control how {{esql}} resolves a partially unmapped field, one that exists in the mapping of some indices but not others, across a multi-index query.

| Value | What it does | When to use it |
| --- | --- | --- |
| `DEFAULT` | The query fails when it references a field that is not mapped in any queried index. For a partially mapped field, documents from indices where the field is not mapped return `null`. This is the default behavior. | You want strict schema enforcement and prefer an error when a field is completely unmapped. |
| `NULLIFY` | Fields that are not mapped in any queried index return `null`. Partially mapped fields behave as they do in `DEFAULT`: documents from indices where the field is not mapped return `null`. | You want a reusable query to continue when a field is not available in a dataset. |
| `LOAD` {applies_to}`stack: preview 9.4` | {{esql}} loads a fully unmapped field from the stored [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) as `keyword`. For a partially mapped field, it loads values from `_source` where the field is unmapped. Fields absent from `_source` return `null`.<br><br>{applies_to}`stack: preview 9.5` For a partially mapped field with a non-`keyword` type, `LOAD` converts the loaded values to the field's mapped type where possible. | You need the real values of a fully or partially unmapped field, so that you can filter or aggregate on it. |

For the full syntax, refer to the [`SET unmapped_fields`](directives/set.md#esql-unmapped_fields) reference.

:::{tip}
:applies_to: stack: preview 9.4

Reading the real values of an unmapped field with `LOAD` is newer than the other behaviors. Earlier versions can nullify unmapped fields (`NULLIFY`) but can't load their real values.
:::

## Limitations [esql-unmapped-fields-limitations]

`LOAD` does not support every command, function, and field type. For the current restrictions, refer to the [`SET unmapped_fields`](directives/set.md#esql-unmapped_fields) reference.

One restriction worth planning around is performance: reading from the stored [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) is slower than querying a mapped field, because the values aren't stored in a data structure optimized for fast access. The trade-off is that the data is available immediately without a full reindex.

## Unmapped fields and runtime fields [esql-unmapped-fields-vs-runtime-fields]

Unmapped fields differ from runtime fields. A runtime field is a computed field that can be defined in the index mapping, while an unmapped field is not in the mapping at all but can be present in documents. If a runtime field is part of the index mapping, {{esql}} treats it like any other mapped field. In {{esql}}, you create computed columns with the [`EVAL`](commands/eval.md) command. To learn more, refer to [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md).

## Related resources [esql-unmapped-fields-related]

To go deeper on unmapped fields and related capabilities, refer to these pages:

- [`SET unmapped_fields`](directives/set.md#esql-unmapped_fields): the directive syntax, accepted values, and `LOAD` limitations.
- [`JSON_EXTRACT`](functions-operators/string-functions/json_extract.md): extract values from JSON strings and `_source`.
- [Retrieve unmapped fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#retrieve-unmapped-fields): the equivalent `include_unmapped` option in the search `fields` API.
- [ES|QL unmapped fields on Elastic Search Labs](https://www.elastic.co/search-labs/blog/esql-unmapped-fields): the blog post that introduces the feature.
