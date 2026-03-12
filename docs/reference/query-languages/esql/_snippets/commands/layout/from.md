```yaml {applies_to}
serverless: ga
stack: ga
```

The `FROM` source command returns a table with data from a data stream, index,
or alias.

::::{tip}
For time series data, use the [`TS`](/reference/query-languages/esql/commands/ts.md) source command instead of `FROM`. `TS` enables time series aggregation functions and is optimized for processing time series indices.
::::

## Syntax

```esql
FROM index_pattern [METADATA fields]
```

## Parameters

`index_pattern`
:   A list of indices, data streams or aliases. Supports wildcards and date math.

`fields`
:   A comma-separated list of [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md) to retrieve.

## Description

The `FROM` source command returns a table with data from a data stream, index,
or alias. Each row in the resulting table represents a document. Each column
corresponds to a field, and can be accessed by the name of that field.

::::{note}
By default, an {{esql}} query without an explicit [`LIMIT`](#esql-limit) uses an implicit
limit of 1000. This applies to `FROM` too. A `FROM` command without `LIMIT`:

```esql
FROM employees
```

is executed as:

```esql
FROM employees
| LIMIT 1000
```

::::


## Examples

The following examples show common `FROM` patterns.

### Query an index

```esql
FROM employees
```

### Use date math in index names

Use [date math](/reference/elasticsearch/rest-apis/api-conventions.md#api-date-math-index-names) to refer to indices, aliases,
and data streams. This can be useful for time series data, for example to access
today’s index:

```esql
FROM <logs-{now/d}>
```

### Query multiple indices

Use comma-separated lists or wildcards to
[query multiple data streams, indices, or aliases](/reference/query-languages/esql/esql-multi-index.md):

```esql
FROM employees-00001,other-employees-*
```

### Query remote clusters

Use the format `<remote_cluster_name>:<target>` to
[query data streams and indices on remote clusters](/reference/query-languages/esql/esql-cross-clusters.md):

```esql
FROM cluster_one:employees-00001,cluster_two:other-employees-*
```

### Include metadata fields

Use the optional `METADATA` directive to enable
[metadata fields](/reference/query-languages/esql/esql-metadata-fields.md):

```esql
FROM employees METADATA _id
```

### Escape index names with special characters

Use enclosing double quotes (`"`) or three enclosing double quotes (`"""`) to escape index names
that contain special characters:

```esql
FROM "this=that", """this[that"""
```
