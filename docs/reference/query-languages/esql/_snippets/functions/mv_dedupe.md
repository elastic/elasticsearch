## `MV_DEDUPE` [esql-mv_dedupe]

**Syntax**

:::{image} ../../../../../images/mv_dedupe.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Remove duplicate values from a multivalued field.

::::{note}
`MV_DEDUPE` may, but won’t always, sort the values in the column.
::::


**Supported types**

| field | result |
| --- | --- |
| boolean | boolean |
| cartesian_point | cartesian_point |
| cartesian_shape | cartesian_shape |
| date | date |
| date_nanos | date_nanos |
| double | double |
| geo_point | geo_point |
| geo_shape | geo_shape |
| integer | integer |
| ip | ip |
| keyword | keyword |
| long | long |
| text | keyword |
| unsigned_long | unsigned_long |
| version | version |

**Example**

```esql
ROW a=["foo", "foo", "bar", "foo"]
| EVAL dedupe_a = MV_DEDUPE(a)
```

| a:keyword | dedupe_a:keyword |
| --- | --- |
| ["foo", "foo", "bar", "foo"] | ["foo", "bar"] |


