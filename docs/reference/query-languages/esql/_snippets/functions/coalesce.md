## `COALESCE` [esql-coalesce]

**Syntax**

:::{image} ../../../../../images/coalesce.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`first`
:   Expression to evaluate.

`rest`
:   Other expression to evaluate.

**Description**

Returns the first of its arguments that is not null. If all arguments are null, it returns `null`.

**Supported types**

| first | rest | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| boolean |  | boolean |
| cartesian_point | cartesian_point | cartesian_point |
| cartesian_shape | cartesian_shape | cartesian_shape |
| date | date | date |
| date_nanos | date_nanos | date_nanos |
| geo_point | geo_point | geo_point |
| geo_shape | geo_shape | geo_shape |
| integer | integer | integer |
| integer |  | integer |
| ip | ip | ip |
| keyword | keyword | keyword |
| keyword |  | keyword |
| long | long | long |
| long |  | long |
| text | text | keyword |
| text |  | keyword |
| version | version | version |

**Example**

```esql
ROW a=null, b="b"
| EVAL COALESCE(a, b)
```

| a:null | b:keyword | COALESCE(a, b):keyword |
| --- | --- | --- |
| null | b | b |


