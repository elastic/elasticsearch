## `LEAST` [esql-least]

**Syntax**

:::{image} ../../../../../images/least.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`first`
:   First of the columns to evaluate.

`rest`
:   The rest of the columns to evaluate.

**Description**

Returns the minimum value from multiple columns. This is similar to [`MV_MIN`](../../esql-functions-operators.md#esql-mv_min) except it is intended to run on multiple columns at once.

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
| EVAL l = LEAST(a, b)
```

| a:integer | b:integer | l:integer |
| --- | --- | --- |
| 10 | 20 | 10 |
