## `RTRIM` [esql-rtrim]

**Syntax**

:::{image} ../../../../../images/rtrim.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Removes trailing whitespaces from a string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "   some text  ",  color = " red "
| EVAL message = RTRIM(message)
| EVAL color = RTRIM(color)
| EVAL message = CONCAT("'", message, "'")
| EVAL color = CONCAT("'", color, "'")
```

| message:keyword | color:keyword |
| --- | --- |
| '   some text' | ' red' |


