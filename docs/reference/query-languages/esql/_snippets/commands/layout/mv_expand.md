```yaml {applies_to}
serverless: preview
stack: preview
```

The `MV_EXPAND` processing command expands multivalued columns into one row per
value, duplicating other columns.

**Syntax**

```esql
MV_EXPAND column
```

**Parameters**

`column`
:   The multivalued column to expand.

::::{warning}
The output rows produced by `MV_EXPAND` can be in any order and may not respect
preceding `SORT`s. To guarantee a certain ordering, place a `SORT` after any
`MV_EXPAND`s.
::::

**Example**

:::{include} ../examples/mv_expand.csv-spec/simple.md
:::
