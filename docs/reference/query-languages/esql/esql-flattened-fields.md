---
applies_to:
  stack: preview 9.5.0
  serverless: preview
navigation_title: "Flattened fields"
description: How ES|QL queries flattened fields and extracts sub-fields using FIELD_EXTRACT.
---

# {{esql}} and flattened fields [esql-flattened-fields]

{{esql}} can read [`flattened`](/reference/elasticsearch/mapping-reference/flattened.md) fields directly
and extract their sub-fields using the [`FIELD_EXTRACT`](functions-operators/string-functions/field_extract.md) function.

A `flattened` field maps an entire object as a single field, indexing all leaf values as [`keywords`](/reference/elasticsearch/mapping-reference/keyword.md).
It is commonly used for objects with a large or unpredictable set of keys — for example, OpenTelemetry
`resource.attributes`, where each service contributes its own set of attributes.

::::{note}
Right now {{esql}} only "sees" the "root" flattened field, and you *must* use `FIELD_EXTRACT` to
get the subfield. We have [big plans](https://github.com/elastic/elasticsearch/issues/152537) to
make it nicer.
::::

## Extracting a sub-field [esql-flattened-fields-extract]

Use `FIELD_EXTRACT` to pull a single sub-field out of a flattened root as a `keyword` column.
The function takes the flattened field and the path as its arguments:

```esql
FROM flattened_otel_logs
| WHERE @timestamp == "2020-01-01T00:02:48.461Z"
| EVAL host.name = field_extract(resource.attributes, "host.name")
| KEEP @timestamp, host.name
```

| @timestamp | host.name |
|---|---|
| 2020-01-01T00:02:48.461Z | infra-filebeat-6vjxr |

The second argument is the dotted path within the flattened field.

## Key behaviors [esql-flattened-fields-behaviors]

`flattened` only contains `keyword`s
:   So `FIELD_EXTRACT` will always return `keyword` fields. Numbers and booleans come
    back as their string representation — `"184896"`, `"true"`.

Keys are always in the collapsed dotted form
:   Flattened field indexing collapses nested objects into dotted keys: `{"http": {"status_code": "200"}}`
    is stored as `http.status_code`. `FIELD_EXTRACT(attributes, "http.status_code")` retrieves that value
    regardless of whether the original document used nested objects or a literal dotted key.

`FIELD_EXTRACT`ing an object returns `null`
:   If the sub-field's value is itself a JSON object rather than a scalar, `FIELD_EXTRACT` returns `null`.
    Address the leaf directly — for example, `"http.request.body.size"` rather than `"http.request"`.

`null` for absent or JSON-null values
:   `FIELD_EXTRACT` returns `null` if the key does not exist, if the stored value is JSON `null`,
    or if either argument is `null`.

## `flattened` only contains `keyword`s

Numbers and booleans come back as their string representation — `"184896"`, `"true"`.
Use casts to get different values:

```console
PUT /flattened-demo
{
  "mappings": {
    "properties": {
      "attrs": { "type": "flattened" }
    }
  }
}
POST /flattened-demo/_doc?refresh
{
  "attrs": { "b": false, "d": 1.2, "l": 123 }
}
POST /_query
{
  "query": """
FROM flattened-demo
| EVAL b = FIELD_EXTRACT(attrs, "b")::BOOLEAN,
       d = FIELD_EXTRACT(attrs, "d")::DOUBLE,
       l = FIELD_EXTRACT(attrs, "l")::LONG
"""
}
```

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

## Keys are always in the collapsed dotted form

When you index a `flattened` field {{es}} "flattens" it. `{"a": {"b": "v"}}` becomes
`{"a.b": "v"}`. When you load the `flattened` with {{esql}} you get the flattened result:

```console
PUT /flattened-demo
{
  "mappings": {
    "properties": {
      "name":  { "type": "keyword" },
      "attrs": { "type": "flattened" }
    }
  }
}

POST /flattened-demo/_doc?refresh
{ "name": "nested", "attrs": { "a": { "b": "something" } } }

POST /_query
{
  "query": "FROM flattened-demo"
}
```

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
POST /flattened-demo/_doc?refresh
{ "name": "pre-dotted","attrs": { "a.b": "something" } }

POST /_query
{
  "query": """
    FROM flattened-demo
    | EVAL ab = FIELD_EXTRACT(attrs, "a.b")
    | SORT name ASC
  """
}
```

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

## `FIELD_EXTRACT`ing an object returns `null`

`FIELD_EXTRACT` can only extract *leaf* fields. If you point it an object, it'll return `null`.
Address the leaf directly — for example, `"http.request.body.size"` rather than `"http.request"`.

```console
PUT /flattened-demo
{
  "mappings": {
    "properties": {
      "name":  { "type": "keyword" },
      "attrs": { "type": "flattened" }
    }
  }
}

POST /flattened-demo/_doc?refresh
{ "attrs": { "a": { "b": "something" } } }

POST /_query
{
  "query": """
FROM flattened-demo
| EVAL a = FIELD_EXTRACT(attrs, "a")
"""
}
```

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
