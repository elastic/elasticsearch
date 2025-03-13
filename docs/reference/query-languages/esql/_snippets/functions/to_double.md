## `TO_DOUBLE` [esql-to_double]

**Syntax**

:::{image} ../../../../../images/to_double.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to a double value. If the input parameter is of a date type, its value will be interpreted as milliseconds since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time), converted to double. Boolean **true** will be converted to double **1.0**, **false** to **0.0**.

**Supported types**

| field | result |
| --- | --- |
| boolean | double |
| counter_double | double |
| counter_integer | double |
| counter_long | double |
| date | double |
| double | double |
| integer | double |
| keyword | double |
| long | double |
| text | double |
| unsigned_long | double |

**Example**

```esql
ROW str1 = "5.20128E11", str2 = "foo"
| EVAL dbl = TO_DOUBLE("520128000000"), dbl1 = TO_DOUBLE(str1), dbl2 = TO_DOUBLE(str2)
```

| str1:keyword | str2:keyword | dbl:double | dbl1:double | dbl2:double |
| --- | --- | --- | --- | --- |
| 5.20128E11 | foo | 5.20128E11 | 5.20128E11 | null |

Note that in this example, the last conversion of the string isn’t possible. When this happens, the result is a **null** value. In this case a *Warning* header is added to the response. The header will provide information on the source of the failure:

`"Line 1:115: evaluation of [TO_DOUBLE(str2)] failed, treating result as null. Only first 20 failures recorded."`

A following header will contain the failure reason and the offending value: `"java.lang.NumberFormatException: For input string: "foo""`


