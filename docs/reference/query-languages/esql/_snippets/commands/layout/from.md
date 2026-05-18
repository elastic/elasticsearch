```yaml {applies_to}
serverless: ga
stack: ga
```

The `FROM` source command returns a table with data from an
[index](docs-content://manage-data/data-store/index-basics.md),
[data stream](docs-content://manage-data/data-store/data-streams.md),
[alias](docs-content://manage-data/data-store/aliases.md),
[view](/reference/query-languages/esql/esql-views.md) or
[subquery](/reference/query-languages/esql/esql-subquery.md).

::::{tip}
For time series data, use the [`TS`](/reference/query-languages/esql/commands/ts.md) source command instead of `FROM`. `TS` enables time series aggregation functions and is optimized for processing time series indices.
::::

## Syntax

```esql
FROM index_pattern [METADATA fields]
FROM index_pattern [, (FROM index_pattern [METADATA fields] [| processing_commands])]* [METADATA fields]
FROM (FROM index_pattern [METADATA fields] [| processing_commands]) [, (FROM index_pattern [METADATA fields] [| processing_commands])]* [METADATA fields]
```

## Parameters

`index_pattern`
:   A list of indices, data streams, aliases or views. Supports wildcards and date math.

`fields`
:   A comma-separated list of [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md) to retrieve.

## Description

The `FROM` source command returns a table with data from a data stream, index,
alias or view. Each row in the resulting table represents a document. Each column
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


### Subqueries

The `FROM` command supports [subqueries](/reference/query-languages/esql/esql-subquery.md),
which are complete ES|QL queries wrapped in parentheses. Each subquery starts
with a `FROM` source command followed by zero or more piped processing commands.
Multiple subqueries and regular index patterns can be combined in a single
`FROM` clause, separated by commas.

```esql
FROM
    employees,
    (FROM sample_data | WHERE client_ip == "172.21.3.15")
```

## Examples

The following examples show common `FROM` patterns.

### Query an index

```esql
FROM employees
```

### Use date math in index names

Use [date math](/reference/elasticsearch/rest-apis/api-conventions.md#api-date-math-index-names) to refer to indices, aliases, views,
and data streams. This can be useful for time series data, for example to access
today’s index:

```esql
FROM <logs-{now/d}>
```

### Query multiple indices

Use comma-separated lists or wildcards to
[query multiple data streams, indices, aliases or views](/reference/query-languages/esql/esql-multi-index.md):

```esql
FROM employees-00001,other-employees-*
```

### Query remote clusters
```{applies_to}
stack: ga
```

Use the format `<remote_cluster_name>:<target>` to
[query data streams and indices on remote clusters](/reference/query-languages/esql/esql-cross-clusters.md):

```esql
FROM cluster_one:employees-00001,cluster_two:other-employees-*
```

### Query across serverless projects

```{applies_to}
serverless: preview
```

By default, queries run across the origin project and all linked projects. To learn more, refer to [query across serverless projects](/reference/query-languages/esql/esql-cross-serverless-projects.md#use-index-expressions).

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
