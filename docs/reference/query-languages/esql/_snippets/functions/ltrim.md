## `LTRIM` [esql-ltrim]

**Syntax**

:::{image} ../../../../../images/ltrim.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Removes leading whitespaces from a string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "   some text  ",  color = " red "
| EVAL message = LTRIM(message)
| EVAL color = LTRIM(color)
| EVAL message = CONCAT("'", message, "'")
| EVAL color = CONCAT("'", color, "'")
```

| message:keyword | color:keyword |
| --- | --- |
| 'some text  ' | 'red ' |


