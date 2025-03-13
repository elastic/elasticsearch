## `TO_LOWER` [esql-to_lower]

**Syntax**

:::{image} ../../../../../images/to_lower.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a new string representing the input string converted to lower case.

**Supported types**

| str | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "Some Text"
| EVAL message_lower = TO_LOWER(message)
```

| message:keyword | message_lower:keyword |
| --- | --- |
| Some Text | some text |


