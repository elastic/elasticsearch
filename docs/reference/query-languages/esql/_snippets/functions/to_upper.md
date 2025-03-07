## `TO_UPPER` [esql-to_upper]

**Syntax**

:::{image} ../../../../../images/to_upper.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a new string representing the input string converted to upper case.

**Supported types**

| str | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "Some Text"
| EVAL message_upper = TO_UPPER(message)
```

| message:keyword | message_upper:keyword |
| --- | --- |
| Some Text | SOME TEXT |


