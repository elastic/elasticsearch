```yaml {applies_to}
serverless: preview
stack: preview 9.1.0
```

The `FORK` processing command creates multiple execution branches to operate
on the same input data and combines the results in a single output table.

## Syntax

```esql
FORK ( <processing_commands> ) ( <processing_commands> ) ... ( <processing_commands> )
```

## Description

The `FORK` processing command creates multiple execution branches to operate
on the same input data and combines the results in a single output table. A discriminator column (`_fork`) is added to identify which branch each row came from.

Together with the [`FUSE`](/reference/query-languages/esql/commands/fuse.md) command, `FORK` enables hybrid search to combine and score results from multiple queries. To learn more about using {{esql}} for search, refer to [ES|QL for search](docs-content://solutions/search/esql-for-search.md).

::::{note}
`FORK` branches default to `LIMIT 1000` if no `LIMIT` is provided.
::::

## Output behavior

`FORK` merges its branch outputs into a single table, adding a `_fork` discriminator
column to each row to indicate which branch it came from.

### Branch identification

The `_fork` column takes values like `fork1`, `fork2`, `fork3`, corresponding to the
order branches are defined.

### Column handling

Branches can output different columns. Columns with the same name must have the same
data type across all branches; missing columns are filled with `null` values.

### Row ordering

Row order is preserved within each branch, but rows from different branches may be
interleaved. Use `SORT _fork` to group results by branch.

## Limitations

- `FORK` supports at most 8 execution branches.
- In versions older than 9.3.0 using remote cluster references and `FORK` is not supported.
- Using more than one `FORK` command in a query is not supported.

## Examples

The following examples show how to run parallel branches and combine their results.

### Run two branches and identify rows by branch

Each `FORK` branch returns one row. `FORK` adds a `_fork` column that indicates
which branch each row came from:

:::{include} ../examples/fork.csv-spec/simpleFork.md
:::

### Return row count alongside top results

Returns the total number of rows that match the query along with
the top five rows sorted by score:

:::{include} ../examples/fork.csv-spec/simpleForkWithStats.md
:::
