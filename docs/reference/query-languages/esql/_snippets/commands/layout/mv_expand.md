## `MV_EXPAND` [esql-mv_expand]

::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The `MV_EXPAND` processing command expands multivalued columns into one row per value, duplicating other columns.

**Syntax**

```esql
MV_EXPAND column
```

**Parameters**

`column`
:   The multivalued column to expand.

**Example**

```esql
ROW a=[1,2,3], b="b", j=["a","b"]
| MV_EXPAND a
```

| a:integer | b:keyword | j:keyword |
| --- | --- | --- |
| 1 | b | ["a", "b"] |
| 2 | b | ["a", "b"] |
| 3 | b | ["a", "b"] |


