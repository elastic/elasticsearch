## `MV_APPEND` [esql-mv_append]

**Syntax**

:::{image} ../../../../../images/mv_append.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

Concatenates values of two multi-value fields.

**Supported types**

| field1 | field2 | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| cartesian_point | cartesian_point | cartesian_point |
| cartesian_shape | cartesian_shape | cartesian_shape |
| date | date | date |
| date_nanos | date_nanos | date_nanos |
| double | double | double |
| geo_point | geo_point | geo_point |
| geo_shape | geo_shape | geo_shape |
| integer | integer | integer |
| ip | ip | ip |
| keyword | keyword | keyword |
| keyword | text | keyword |
| long | long | long |
| text | keyword | keyword |
| text | text | keyword |
| unsigned_long | unsigned_long | unsigned_long |
| version | version | version |


