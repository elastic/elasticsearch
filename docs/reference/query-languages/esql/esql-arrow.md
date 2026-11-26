---
navigation_title: Arrow response format
applies_to:
  stack: experimental
  serverless: experimental
products:
  - id: elasticsearch
---

# ES|QL Arrow response format [esql-arrow]

The {{esql}} REST API returns results in the [Apache Arrow IPC streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) when the
`format=arrow` request parameter or the `Accept: application/vnd.apache.arrow.stream` header are present.

The response is a binary Arrow stream containing a schema and record batches, rather than a JSON response.

## How to read the schemas [esql-arrow-reading-schemas]

Each {{esql}} result column becomes an Arrow field with the column's name.
The field metadata entry `elastic:type` records its {{esql}} type.
Several {{esql}} types share an Arrow physical type, so this metadata can be used to
interpret the result.

The table below provides a quick overview of the mapping of all types.
The reference below shows each field's name, Arrow type, nullability, and metadata.
Expand a type to inspect its schema, then use the tree expanders to show or hide child fields.
Type labels name the Arrow type, followed by its parameters in parentheses where the type has any.
For example, `int(bitWidth: 64, isSigned: true)` is a signed 64-bit integer, while `int(bitWidth: 64, isSigned: false)` is unsigned.
Nullability describes the field's values, independently at each nesting level.

A column with multivalue positions uses a nullable `List` field.
Its element field is named `$data$` and is non-nullable because {{esql}} multivalue positions cannot contain null elements.
The children of a struct element can still be nullable.
The same {{esql}} type can use a scalar or list representation depending on the query results; inspect the response schema rather than assuming one form from the index mapping.

## Encoding notes [esql-arrow-encoding-notes]

### Multivalued columns

Elasticsearch accepts both single values and multiple values for
most field types. Since this isn't part of the index mapping definition, the Arrow encoding
adapts its schema to the data present in the {{esql}} response. If a result column contains
multivalued entries, the Arrow schema will be a `list`. Otherwise,
it will be the type indicated in the mapping table.

Depending on the data present in your indices, you may have to inspect the column schema to determine
if a column is singlevalued or multivalued.

Note that some types however are never multivalued, and some others are always multivalued. This is
indicated in the table in the "May be multivalued" column.

### Type-specific notes
* `date` and `date_nanos` use timestamps with millisecond and nanosecond precision, respectively, without an Arrow timezone annotation.
* `ip` uses 4 bytes for IPv4 addresses and 16 bytes for IPv6 addresses, in network byte order.
* `geo_point`, `geo_shape`, `cartesian_point`, and `cartesian_shape` use the well-known binary (WKB) format.
* `_source` and `flattened` use JSON text.
* `version` uses the decoded version string.

:::{include} _snippets/generated/x-pack-esql/syntax/arrow-types.md
:::
