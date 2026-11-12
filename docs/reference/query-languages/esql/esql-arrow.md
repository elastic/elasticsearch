---
navigation_title: Arrow response format
applies_to:
  stack: experimental
  serverless: experimental
products:
  - id: elasticsearch
---

# ES|QL Arrow response format [esql-arrow]

:::{warning}
The Arrow response format is experimental and is not generally available (GA).
Its type mappings and field metadata may change. These schemas describe the current implementation; use the schema in each response when reading query results.
:::

The {{esql}} REST API can return results in the [Apache Arrow IPC streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
Request `format=arrow` or send an `Accept: application/vnd.apache.arrow.stream` header.
The response is a binary Arrow stream containing a schema and record batches, rather than a JSON response.

## How to read the schemas [esql-arrow-reading-schemas]

Each result column becomes an Arrow field with the column's name.
The field metadata entry `elastic:type` records its ES|QL output type.
Several ES|QL types share an Arrow physical type, so retain this metadata when interpreting the result.

The reference below shows each field's name, Arrow type, nullability, and metadata.
Expand a type to inspect its schema, then use the tree expanders to show or hide child fields.
Type labels use the Arrow Java representation: for example, `Int(64, true)` is a signed 64-bit integer,
while `Int(64, false)` is unsigned.
Nullability describes the field's values, independently at each nesting level.

A column with multivalue positions uses a nullable `List` field.
Its element field is named `$data$` and is non-nullable because ES|QL multivalue positions cannot contain null elements.
The children of a struct element can still be nullable.
The same ES|QL type can use a scalar or list representation depending on the query results; inspect the response schema rather than assuming one form from the index mapping.

## Encoding notes [esql-arrow-encoding-notes]

* `date` and `date_nanos` use timestamps with millisecond and nanosecond precision, respectively, without an Arrow timezone annotation.
* `ip` uses 4 bytes for IPv4 addresses and 16 bytes for IPv6 addresses, in network byte order.
* `geo_point`, `geo_shape`, `cartesian_point`, and `cartesian_shape` use well-known binary (WKB).
* `_source` and `flattened` use JSON text. `version` uses the decoded version string.

:::{include} _snippets/generated/x-pack-esql/syntax/arrow-types.md
:::
