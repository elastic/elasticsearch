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

The response is then a binary Arrow stream starting with a schema, followed by record batches.

Each {{esql}} result column becomes an Arrow field with the column's name.
The field metadata entry `elastic:type` records its {{esql}} type.
Several {{esql}} types share an Arrow physical type, so this metadata can be used to
interpret the result.

The table below provides a quick overview of the mapping of all types.
The reference list then shows each field's name, Arrow type, nullability, and metadata.

## Encoding notes [esql-arrow-encoding-notes]

### Multivalued columns

Elasticsearch accepts both single values and multiple values for
most field types. Since this isn't part of the index mapping definition, the Arrow encoding
adapts its schema to the data present in the {{esql}} response. If a result column contains
multivalued entries, the Arrow schema will be a `list`. Otherwise,
it will be of the type indicated in the mapping table.

A column with multivalued entries is a nullable `List` field. Its child field is non-nullable
because {{esql}} multivalue positions cannot contain null elements.

This means that, depending on the data present in your indices, you may have to inspect the
column schema to determine if a column is single valued or multivalued.

Note also that some types are never multivalued, and some others are always multivalued. This is
indicated in the table below with the "May be multivalued" column.

### Type-specific notes
* `date` and `date_nanos` use timestamps with millisecond and nanosecond precision, respectively, without a timezone annotation.
* `ip` uses 4 bytes for IPv4 addresses and 16 bytes for IPv6 addresses, in network byte order.
* `geo_point`, `geo_shape`, `cartesian_point`, and `cartesian_shape` use the well-known binary (WKB) format.
* `_source` and `flattened` use JSON text.
* `version` uses the decoded version string.

:::{include} _snippets/generated/x-pack-esql/syntax/arrow-types.md
:::
