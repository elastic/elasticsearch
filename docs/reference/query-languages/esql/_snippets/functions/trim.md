## `TRIM` [esql-trim]

**Syntax**

:::{image} ../../../../../images/trim.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Removes leading and trailing whitespaces from a string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "   some text  ",  color = " red "
| EVAL message = TRIM(message)
| EVAL color = TRIM(color)
```

| message:s | color:s |
| --- | --- |
| some text | red |
