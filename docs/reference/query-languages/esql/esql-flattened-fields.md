---
applies_to:
  stack: preview 9.5+
  serverless: preview
navigation_title: "Flattened fields"
description: How ES|QL queries flattened fields and extracts sub-fields using FIELD_EXTRACT.
---

# {{esql}} and flattened fields [esql-flattened-fields]

{{esql}} can read [`flattened`](/reference/elasticsearch/mapping-reference/flattened.md) fields directly
and extract their sub-fields using the [`FIELD_EXTRACT`](functions-operators/string-functions/field_extract.md) function.

A `flattened` field maps an entire object as a single field, indexing all leaf values as [`keywords`](/reference/elasticsearch/mapping-reference/keyword.md).
It is commonly used for objects with a large or unpredictable set of keys, for example OpenTelemetry
`resource.attributes`, where each service contributes its own set of attributes.

::::{note}
Currently {{esql}} can access only the root flattened field directly and you must use `FIELD_EXTRACT` to
get the subfield. This will change in a future release. See [elasticsearch/issues/152537](https://github.com/elastic/elasticsearch/issues/152537).
::::

## Extract a sub-field [esql-flattened-fields-extract]

Use `FIELD_EXTRACT` to pull a single sub-field out of a flattened root as a `keyword` column.
The function takes the flattened field and the path as its arguments:

:::{include} _snippets/generated/x-pack-esql/commands/examples/field_extract.csv-spec/field_extract_basic.md
:::

The second argument is the dotted path within the flattened field.

## Key behaviors [esql-flattened-fields-behaviors]

- [Flattened fields only contain keywords](#esql-flattened-fields-keywords): `FIELD_EXTRACT` always returns `keyword` values, so numbers and booleans come back as their string representation.
- [Keys are always in the collapsed dotted form](#esql-flattened-fields-dotted-keys): nested objects are stored as dotted keys, and `FIELD_EXTRACT` resolves a key the same way regardless of how the document was written.
- [Extracting an object returns null](#esql-flattened-fields-object-null): pointing `FIELD_EXTRACT` at an object instead of a leaf returns `null`.
- Missing keys and JSON `null` return `null`: `FIELD_EXTRACT` returns `null` if the key does not exist, if the stored value is JSON `null`, or if either argument is `null`.

## Flattened fields only contain keywords [esql-flattened-fields-keywords]

Numbers and booleans come back as their string representation, for example: `"184896"`, `"true"`.
Use casts to get other types. Create an index, index a document, and run a query that casts the extracted values:

```console
PUT /flattened-cast-demo
{
  "mappings": {
    "properties": {
      "attrs": { "type": "flattened" }
    }
  }
}
POST /flattened-cast-demo/_doc?refresh
{
  "attrs": { "b": false, "d": 1.2, "l": 123 } <1>
}
POST /_query
{
  "query": """
FROM flattened-cast-demo
| EVAL b = FIELD_EXTRACT(attrs, "b")::BOOLEAN, <2>
       d = FIELD_EXTRACT(attrs, "d")::DOUBLE,
       l = FIELD_EXTRACT(attrs, "l")::LONG
"""
}
```
1. The document stores a boolean, a double, and a long.
2. `FIELD_EXTRACT` returns each value as a `keyword`, so cast it to the type you want.

This query returns the following:

```console-result
{
  "columns": [
    {"name": "attrs", "type": "flattened"},
    {"name": "b",     "type": "boolean"},
    {"name": "d",     "type": "double"},
    {"name": "l",     "type": "long"}
  ],
  "values": [
    [{"b": "false", "d": "1.2", "l": "123"}, false, 1.2, 123]
  ]
}
```

## Keys are always in the collapsed dotted form [esql-flattened-fields-dotted-keys]

When you index a `flattened` field {{es}} "flattens" it. `{"a": {"b": "v"}}` becomes
`{"a.b": "v"}`. When you load the `flattened` with {{esql}}, you get the flattened result:

```console
PUT /flattened-keys-demo
{
  "mappings": {
    "properties": {
      "name":  { "type": "keyword" },
      "attrs": { "type": "flattened" }
    }
  }
}

POST /flattened-keys-demo/_doc?refresh
{ "name": "nested", "attrs": { "a": { "b": "something" } } } <1>

POST /_query
{
  "query": "FROM flattened-keys-demo"
}
```
1. The document nests `b` inside `a`. {{es}} stores it as the dotted key `a.b`.

This query returns the following:

```console-result
{
  "columns": [
    {"name": "attrs", "type": "flattened"},
    {"name": "name",  "type": "keyword"}
  ],
  "values": [
    [{"a.b": "something"}, "nested"]
  ]
}
```

The `path` parameter of `FIELD_EXTRACT` operates on *exactly* that normalized path.
So `{ "a.b": "something" }` is the same as `{ "a": { "b": "something" } }`:

```console
POST /flattened-keys-demo/_doc?refresh
{ "name": "pre-dotted","attrs": { "a.b": "something" } } <1>

POST /_query
{
  "query": """
    FROM flattened-keys-demo
    | EVAL ab = FIELD_EXTRACT(attrs, "a.b") <2>
    | SORT name ASC
  """
}
```
1. This document uses a literal dotted key instead of a nested object.
2. The same path resolves both documents, since they collapse to the same key.

This query returns the following:

```console-result
{
  "columns": [
    {"name": "attrs", "type": "flattened"},
    {"name": "name",  "type": "keyword"},
    {"name": "ab",    "type": "keyword"}
  ],
  "values": [
    [{"a.b": "something"}, "nested",    "something"],
    [{"a.b": "something"}, "pre-dotted","something"]
  ]
}
```

## Extracting an object with FIELD_EXTRACT returns NULL [esql-flattened-fields-object-null]

`FIELD_EXTRACT` can only extract *leaf* fields. If you point it at an object, it returns `null`.
Address the leaf directly. For example, use `"http.request.body.size"` rather than `"http.request"`.
Create an index, index a document whose value is an object, and extract that object key:

```console
PUT /flattened-object-demo
{
  "mappings": {
    "properties": {
      "attrs": { "type": "flattened" }
    }
  }
}

POST /flattened-object-demo/_doc?refresh
{ "attrs": { "a": { "b": "something" } } } <1>

POST /_query
{
  "query": """
FROM flattened-object-demo
| EVAL a = FIELD_EXTRACT(attrs, "a") <2>
"""
}
```
1. `a` holds an object, not a leaf value.
2. Extracting the object returns `null`. Extract the leaf `a.b` instead.

This query returns the following, with `a` set to `null` because `a` is an object:

```console-result
{
  "columns": [
    {"name": "attrs", "type": "flattened"},
    {"name": "a",     "type": "keyword"}
  ],
  "values": [
    [{"a.b": "something"}, null]
  ]
}
```


## Related resources [esql-flattened-fields-related]

- [`FIELD_EXTRACT`](functions-operators/string-functions/field_extract.md): the function reference, including null handling and JSONPath restrictions.
- [`flattened` field type](/reference/elasticsearch/mapping-reference/flattened.md): mapping parameters, typed sub-fields with `properties`, and `passthrough`.
- [Supported field types](limitations.md#esql-supported-types): the full list of field types {{esql}} can read.
- [Multi-value functions](functions-operators/mv-functions.md): functions for working with multi-valued columns.
