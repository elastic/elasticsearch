## `FORK` [esql-fork]

```yaml {applies_to}
serverless: preview
stack: preview 9.1.0
```

The `FORK` processing command runs multiple execution branches and outputs the
results back into a single table.

**Syntax**

```esql
FORK ( <processing_commands> ) ( <processing_commands> ) ... ( <processing_commands> )
```

**Description**

The `FORK` processing command feeds the input rows into multiple execution
branches and outputs the results into a single table, enhanced with a
discriminator column called `_fork`.

The values of the discriminator column `_fork` are `fork1`, `fork2`, ... and
they designate which `FORK` branch the current row is coming from.
The values of the `_fork` column always start with `fork1`, which indicates that
the row is coming from the first branch.

The `FORK` branches can output different columns as long as there exists no
column with the same name and different data types in two different `FORK`
branches.

When a column does not exist in a `FORK` branch, but it exists in the output of
other branches, `FORK` will add `null` values to the rows that have missing
columns.

`FORK` preserves the order of the rows from each subset, but it does not
guarantee that the rows will follow the same order in which the `FORK` branches
are defined. The rows from the first branch can be interleaved with the rows
from subsequent branches. Use `SORT _fork` after `FORK` if you need to change
this behaviour.

`FORK` branches receive an implicit `LIMIT 1000` if no `LIMIT` is provided.

::::{note}
`FORK` supports at most 8 execution branches.
::::

::::{note}
Using remote cluster references and `FORK` is not supported.
::::

::::{note}
Using more than one `FORK` command in a query is not supported.
::::

**Examples**

In the following example, each `FORK` branch returns one row.
Notice how `FORK` adds a `_fork` that indicates from which row the branch is
coming:

:::{include} ../examples/fork.csv-spec/simpleFork.md

The next example, returns total number of rows that match the query along with
the top five rows sorted by score.

:::{include} ../examples/fork.csv-spec/simpleForkWithStats.md
