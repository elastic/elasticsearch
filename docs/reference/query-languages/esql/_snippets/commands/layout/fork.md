## `FORK` [esql-fork]

```yaml {applies_to}
serverless: preview
stack: preview 9.1.0
```

The `FORK` processing command creates multiple execution branches to operate
on the same input data and combines the results in a single output table.

**Syntax**

```esql
FORK ( <processing_commands> ) ( <processing_commands> ) ... ( <processing_commands> )
```

**Description**

The `FORK` processing command creates multiple execution branches to operate
on the same input data and combines the results in a single output table. A discriminator column (`_fork`) is added to identify which branch each row came from.

**Branch identification:**
- The `_fork` column identifies each branch with values like `fork1`, `fork2`, `fork3`
- Values correspond to the order branches are defined
- `fork1` always indicates the first branch

**Column handling:**
- `FORK` branches can output different columns
- Columns with the same name must have the same data type across all branches  
- Missing columns are filled with `null` values

**Row ordering:**
- `FORK` preserves row order within each branch
- Rows from different branches may be interleaved
- Use `SORT _fork` to group results by branch

::::{note}
`FORK` branches default to `LIMIT 1000` if no `LIMIT` is provided.
::::

**Limitations**

- `FORK` supports at most 8 execution branches.
- Using remote cluster references and `FORK` is not supported.
- Using more than one `FORK` command in a query is not supported.

**Examples**

In the following example, each `FORK` branch returns one row.
Notice how `FORK` adds a `_fork` column that indicates which row the branch originates from:

:::{include} ../examples/fork.csv-spec/simpleFork.md

The next example, returns total number of rows that match the query along with
the top five rows sorted by score.

:::{include} ../examples/fork.csv-spec/simpleForkWithStats.md
