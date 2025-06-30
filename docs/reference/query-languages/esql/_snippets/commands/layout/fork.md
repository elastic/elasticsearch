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

The `FORK` processing command feeds the input rows into multiple execution
branches and outputs the results into a single table, with a discriminator column (`_fork`) to identify which branch each row came from.

The values of the discriminator column `_fork` are `fork1`, `fork2`, etc. and
they designate which `FORK` branch the current row comes from.
The values of the `_fork` column always start with `fork1`, which indicates that
the row comes from the first branch.

`FORK` branches can output different columns, but columns with the
same name must have the same data type across all branches.

When a column does not exist in a `FORK` branch, but it exists in the output of
other branches, `FORK` will add `null` values to the rows that have missing
columns.

`FORK` preserves the order of the rows from each subset, but it does not
guarantee that the rows will follow the same order in which the `FORK` branches
are defined. The rows from the first branch can be interleaved with the rows
from subsequent branches. Use `SORT _fork` after `FORK` if you need to change
this behaviour.

`FORK` branches receive an implicit `LIMIT 1000` if no `LIMIT` is provided.

Limitations and usage:

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
