## `TO_BOOLEAN` [esql-to_boolean]

**Syntax**

:::{image} ../../../../../images/to_boolean.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to a boolean value. A string value of **true** will be case-insensitive converted to the Boolean **true**. For anything else, including the empty string, the function will return **false**. The numerical value of **0** will be converted to **false**, anything else will be converted to **true**.

**Supported types**

| field | result |
| --- | --- |
| boolean | boolean |
| double | boolean |
| integer | boolean |
| keyword | boolean |
| long | boolean |
| text | boolean |
| unsigned_long | boolean |

**Example**

```esql
ROW str = ["true", "TRuE", "false", "", "yes", "1"]
| EVAL bool = TO_BOOLEAN(str)
```

| str:keyword | bool:boolean |
| --- | --- |
| ["true", "TRuE", "false", "", "yes", "1"] | [true, true, false, false, false, false] |


