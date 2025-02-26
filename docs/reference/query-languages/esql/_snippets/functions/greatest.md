## `GREATEST` [esql-greatest]

**Syntax**

:::{image} ../../../../../images/greatest.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`first`
:   First of the columns to evaluate.

`rest`
:   The rest of the columns to evaluate.

**Description**

Returns the maximum value from multiple columns. This is similar to [`MV_MAX`](../../esql-functions-operators.md#esql-mv_max) except it is intended to run on multiple columns at once.

::::{note}
When run on `keyword` or `text` fields, this returns the last string in alphabetical order. When run on `boolean` columns this will return `true` if any values are `true`.
::::


**Supported types**

| first | rest | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| boolean |  | boolean |
| date | date | date |
| date_nanos | date_nanos | date_nanos |
| double | double | double |
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
ROW a = 10, b = 20
| EVAL g = GREATEST(a, b)
```

| a:integer | b:integer | g:integer |
| --- | --- | --- |
| 10 | 20 | 20 |


