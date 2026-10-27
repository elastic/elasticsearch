---
applies_to:
  stack: preview 9.3-9.4, ga 9.5+
  serverless: ga
navigation_title: "Unmapped fields"
description: How ES|QL queries fields that aren't in the index mapping, using the SET unmapped_fields directive.
---

# Query unmapped fields without reindexing [esql-unmapped-fields]

{{esql}} can query fields that are not defined in your index mapping, with no reindex and no change to the mapping. The [`SET unmapped_fields`](directives/set.md#esql-unmapped_fields) directive controls how each query handles them.

An unmapped field is a field in indexed documents that the index' mapping does not define. By default, {{esql}} treats such fields as `null` and returns an error if a referenced field is not mapped in any index.

For more about how `NULL` behaves in expressions, filters, sorting, and aggregation, refer to [Working with NULL values in {{esql}}](/reference/query-languages/esql/esql-null-values.md).

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

The `unmapped_fields` setting accepts the following values, which range from strict to permissive. These values also control how {{esql}} resolves a partially unmapped field, one that exists in the mapping of some indices but not others, across a multi-index query.

| Value | What it does | When to use it
| --- | --- | --- |
| `DEFAULT` | The query fails when it references a field that is not mapped in any queried index. For a partially mapped field, documents from indices where the field is not mapped return `null`. This is the default behavior. | You want strict schema enforcement and prefer an error when a field is completely unmapped. |
| `NULLIFY` | When a query references a field that is not mapped in any queried index, that field returns `null`. Fully unmapped fields that the query never mentions do not appear in the output. Partially mapped fields behave as they do in `DEFAULT`: documents from indices where the field is not mapped return `null`. | You want a reusable query to continue working when a field is not available in a dataset. |
| `LOAD` {applies_to}`stack: preview =9.4, ga 9.5+` | Loads the unmapped fields that the query names from [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) as `keyword`, or `null` if absent. Also loads partially mapped fields in the indices where they're unmapped. | You need the real values of a fully or partially unmapped field, so that you can filter or aggregate on it. |
| `LOAD_ALL` {applies_to}`stack: preview 9.6+` {applies_to}`serverless: preview` | Does everything `LOAD` does. In addition, returns every unmapped field that the query does not name in a single `_unmapped_fields` column, so you can discover fields without knowing their names. | You want to explore documents whose fields you don't know yet. |

For the full syntax, refer to the [`SET unmapped_fields`](directives/set.md#esql-unmapped_fields) reference.

:::{tip}
:applies_to: stack: preview =9.4, ga 9.5+

Reading the real values of an unmapped field with `LOAD` is newer than the other behaviors. Earlier versions can nullify unmapped fields (`NULLIFY`) but can't load their real values.
:::

## Load specific unmapped fields [esql-unmapped-fields-load]

```{applies_to}
stack: preview =9.4, ga 9.5+
```

With `LOAD`, {{esql}} reads the unmapped fields that your query names from [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) and returns them as `keyword` columns. A named field that is absent from `_source` returns `null`.

:::{include} _snippets/generated/x-pack-esql/commands/examples/unmapped-load.csv-spec/unmapped-load-named-field.md
:::

`message` is mapped and `unmapped_message` isn't, but both are returned as regular columns.

For a partially mapped field, one that is mapped in some queried indices but not others, `LOAD` reads values from `_source` in the indices where it's unmapped, even if the query doesn't name the field. For example, in `FROM idx1, idx2`, a field mapped only in `idx1` is loaded from `idx2`. {applies_to}`stack: ga 9.5+` If the field's mapped type isn't `keyword`, `LOAD` converts the loaded values to that type where possible.

## Load all unmapped fields [esql-unmapped-fields-load-all]

```{applies_to}
stack: preview 9.6+
serverless: preview
```

`LOAD` only loads the unmapped fields that your query names. With `LOAD_ALL`, {{esql}} also reads the rest of the unmapped fields from [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md), so you can see what is in your documents without listing every field.

:::{include} _snippets/generated/x-pack-esql/commands/examples/unmapped-load-all.csv-spec/unmapped-load-all-sample.md
:::

Mapped fields are returned as columns as usual. Every unmapped field is returned in the extra `_unmapped_fields` column, which has the type `_source`.

### How `_unmapped_fields` works [esql-unmapped-fields-load-all-rules]

- **One column, one JSON object per row.** `_unmapped_fields` holds the unmapped fields of the row. It is `null` for a row that has none.
- **Flattened, like a [`flattened`](/reference/elasticsearch/mapping-reference/flattened.md) field.** Nested objects are flattened into dotted paths such as `unmapped.nested`, in the order the fields appear in `_source`. Values keep their JSON form from `_source`, so a field with several values is a JSON array. They become typed `keyword` columns only when the query names them.
- **Empty values are left out.** Explicit `null` values, empty arrays, and empty objects don't appear in the column.
- **Fields you name become their own column.** A field that the query refers to, for example in `WHERE`, `KEEP`, or `EVAL`, is loaded the same way as with `LOAD`, as a `keyword` column, and is no longer part of `_unmapped_fields`. The same applies if a command creates a column with the same name as an unmapped field, for example with `EVAL` or `RENAME`.
- **`KEEP` and `DROP` with wildcards apply to the unmapped fields.** `DROP unmapped_*` removes the matching fields from `_unmapped_fields`. `KEEP @timestamp, unmapped_*` keeps only the matching fields in `_unmapped_fields`. A `KEEP` with only exact names doesn't return `_unmapped_fields` at all.
- **Commands and functions that take field patterns don't see them.** For example, full-text search functions such as `KQL` and `QSTR` don't search the fields in `_unmapped_fields`.
- **Partially mapped fields behave as in `LOAD`.** Where a field is mapped in some of the queried indices only, {{esql}} reads it from the mapping where it exists and from `_source` where it doesn't. These fields are columns, not part of `_unmapped_fields`.

:::{note}
`_unmapped_fields` isn't a reserved name. If your query refers to or creates a column named `_unmapped_fields`, that column is used instead and the unmapped fields aren't returned in it.
:::

`LOAD_ALL` is a technical preview. For what it doesn't support yet, and what it costs, refer to [Limitations specific to `LOAD_ALL`](#esql-unmapped-fields-load-all-limitations).

## Limitations [esql-unmapped-fields-limitations]

### Performance [esql-unmapped-fields-performance]

`LOAD` reads values from the stored [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md). That is slower than querying a mapped field, because the values aren't stored in a data structure optimized for fast access. The trade-off is that the data is available immediately without a full reindex.

Latency is especially high when a filter or sort on an unmapped field cannot be pushed down to Lucene and forces a full scan of the index. For example:

```esql
SET unmapped_fields="load";
FROM idx
| SORT unmapped
| LIMIT 10
```

```esql
SET unmapped_fields="load";
FROM idx
| WHERE unmapped == "foo"
| LIMIT 10
```

In both cases, {{esql}} cannot use a Lucene index on `unmapped`, so it must read documents (and parse `_source` for the loaded field) across the queried data. Large `_source` documents make this even slower. Prefer mapped fields for selective filters and sorts on large indices; use `LOAD` for exploration and for fields you haven't mapped yet. `LOAD_ALL` reads every unmapped field, so it is even more expensive.

### Scope of loaded fields [esql-unmapped-fields-scope]

`LOAD` does not actually load every field from `_source`. It loads:

- **Partially mapped fields**: {{esql}} loads them from `_source` in the indices where they are unmapped, even when the query never mentions them. Refer to [Load specific unmapped fields](#esql-unmapped-fields-load).
- **Fully unmapped fields**: fields that are not mapped in any queried index are loaded only when the query references them.

`LOAD_ALL` also loads the fully unmapped fields that the query doesn't reference. It returns them in the `_unmapped_fields` column.

### Other restrictions [esql-unmapped-fields-other-restrictions]

`LOAD` and `LOAD_ALL` do not support every command, function, and field type. Notable restrictions include:

- [`PROMQL`](commands/promql.md) is not supported with `LOAD` or `LOAD_ALL`.
- Referencing subfields of [`flattened`](/reference/elasticsearch/mapping-reference/flattened.md) parents is not supported.
- Partially mapped fields whose type has no implicit conversion from `keyword` (for example `text` or `aggregate_metric_double`) keep their mapped type but are not loaded from `_source` (i.e., they're set as `null` where unmapped). Using explicit cast, e.g., `to_text` allows loading from `_source`. {applies_to}`stack: ga 9.5+`

For the full list of restrictions, refer to the [`SET unmapped_fields`](directives/set.md#esql-unmapped_fields) reference.

### Limitations specific to `LOAD_ALL` [esql-unmapped-fields-load-all-limitations]

```{applies_to}
stack: preview 9.6+
serverless: preview
```

In addition to the restrictions above, `LOAD_ALL` has these limitations:

- It supports only a subset of the processing commands, and a query that uses another command fails with an error that lists the supported ones. Notably, the [`TS`](commands/ts.md) source command is not supported.
- It doesn't apply inside [views](/reference/query-languages/esql/esql-views.md). The setting stops at a view's boundary: a view doesn't return an `_unmapped_fields` column, and the query returns `null` with a warning for an unmapped field that the view doesn't output. To load such fields, the view definition must itself `SET unmapped_fields="load"` and reference them. A view definition can't use `LOAD_ALL`. Indices and subqueries next to a view are loaded as usual.
- Reading every unmapped field uses more memory than `LOAD`, especially on documents with many fields or large arrays. If you know which fields you need, name them and use `LOAD`.

## Unmapped fields and runtime fields [esql-unmapped-fields-vs-runtime-fields]

Unmapped fields differ from runtime fields. A runtime field is a computed field that can be defined in the index mapping, while an unmapped field is not in the mapping at all but can be present in documents. If a runtime field is part of the index mapping, {{esql}} treats it like any other mapped field. In {{esql}}, you create computed columns with the [`EVAL`](commands/eval.md) command. To learn more, refer to [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md).

## Related resources [esql-unmapped-fields-related]

To go deeper on unmapped fields and related capabilities, refer to these pages:

- [`SET unmapped_fields`](directives/set.md#esql-unmapped_fields): the directive syntax, accepted values, and `LOAD` limitations.
- [`JSON_EXTRACT`](functions-operators/string-functions/json_extract.md): extract values from JSON strings and `_source`.
- [Retrieve unmapped fields](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#retrieve-unmapped-fields): the equivalent `include_unmapped` option in the search `fields` API.
- [ES|QL unmapped fields on Elastic Search Labs](https://www.elastic.co/search-labs/blog/esql-unmapped-fields): the blog post that introduces the feature.
