```yaml {applies_to}
stack: preview 9.0-9.3, ga 9.4+
serverless: ga
```

The `MV_EXPAND` processing command expands multivalued columns into one row per
value, duplicating other columns.

## Syntax

```esql
MV_EXPAND column
```

## Parameters

`column`
:   The multivalued column to expand.

::::{warning}
`MV_EXPAND` does not preserve the sort order from a preceding `SORT`. The output
rows can be in any order. To guarantee a certain ordering, place a `SORT` after
`MV_EXPAND`.

Additionally, if a `WHERE` clause follows `MV_EXPAND`, it may prevent the query
planner from merging a preceding `SORT` with a trailing `LIMIT` into an
efficient `TopN` operation. In such cases, the query will fail with a
validation exception: `MV_EXPAND [MV_EXPAND field] cannot yet have an unbounded SORT [SORT field] before it: either move the SORT after it, or add a LIMIT after the SORT`.
To work around this, add a `LIMIT` before `MV_EXPAND`:

```esql
FROM index | SORT field | LIMIT 1000 | MV_EXPAND mv_field | WHERE mv_field == "value"
```
::::

## Example

:::{include} ../examples/mv_expand.csv-spec/simple.md
:::
