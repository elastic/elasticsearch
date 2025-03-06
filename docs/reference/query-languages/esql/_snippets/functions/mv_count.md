## `MV_COUNT` [esql-mv_count]

**Syntax**

:::{image} ../../../../../images/mv_count.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Converts a multivalued expression into a single valued column containing a count of the number of values.

**Supported types**

| field | result |
| --- | --- |
| boolean | integer |
| cartesian_point | integer |
| cartesian_shape | integer |
| date | integer |
| date_nanos | integer |
| double | integer |
| geo_point | integer |
| geo_shape | integer |
| integer | integer |
| ip | integer |
| keyword | integer |
| long | integer |
| text | integer |
| unsigned_long | integer |
| version | integer |

**Example**

```esql
ROW a=["foo", "zoo", "bar"]
| EVAL count_a = MV_COUNT(a)
```

| a:keyword | count_a:integer |
| --- | --- |
| ["foo", "zoo", "bar"] | 3 |


