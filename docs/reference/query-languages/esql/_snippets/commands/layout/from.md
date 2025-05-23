## `FROM` [esql-from]

The `FROM` source command returns a table with data from a data stream, index,
or alias.

**Syntax**

```esql
FROM index_pattern [METADATA fields]
```

**Parameters**

`index_pattern`
:   A list of indices, data streams or aliases. Supports wildcards and date math.

`fields`
:   A comma-separated list of [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md) to retrieve.

**Description**

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


**Examples**

```esql
FROM employees
```

You can use [date math](/reference/elasticsearch/rest-apis/api-conventions.md#api-date-math-index-names) to refer to indices, aliases
and data streams. This can be useful for time series data, for example to access
today’s index:

```esql
FROM <logs-{now/d}>
```

Use comma-separated lists or wildcards to
[query multiple data streams, indices, or aliases](docs-content://explore-analyze/query-filter/languages/esql-multi-index.md):

```esql
FROM employees-00001,other-employees-*
```

Use the format `<remote_cluster_name>:<target>` to
[query data streams and indices on remote clusters](docs-content://explore-analyze/query-filter/languages/esql-cross-clusters.md):

```esql
FROM cluster_one:employees-00001,cluster_two:other-employees-*
```

Use the optional `METADATA` directive to enable
[metadata fields](/reference/query-languages/esql/esql-metadata-fields.md):

```esql
FROM employees METADATA _id
```

Use enclosing double quotes (`"`) or three enclosing double quotes (`"""`) to escape index names
that contain special characters:

```esql
FROM "this=that", """this[that"""
```
