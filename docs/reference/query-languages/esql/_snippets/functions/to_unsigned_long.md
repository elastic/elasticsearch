## `TO_UNSIGNED_LONG` [esql-to_unsigned_long]

**Syntax**

:::{image} ../../../../../images/to_unsigned_long.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to an unsigned long value. If the input parameter is of a date type, its value will be interpreted as milliseconds since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time), converted to unsigned long. Boolean **true** will be converted to unsigned long **1**, **false** to **0**.

**Supported types**

| field | result |
| --- | --- |
| boolean | unsigned_long |
| date | unsigned_long |
| double | unsigned_long |
| integer | unsigned_long |
| keyword | unsigned_long |
| long | unsigned_long |
| text | unsigned_long |
| unsigned_long | unsigned_long |

**Example**

```esql
ROW str1 = "2147483648", str2 = "2147483648.2", str3 = "foo"
| EVAL long1 = TO_UNSIGNED_LONG(str1), long2 = TO_ULONG(str2), long3 = TO_UL(str3)
```

| str1:keyword | str2:keyword | str3:keyword | long1:unsigned_long | long2:unsigned_long | long3:unsigned_long |
| --- | --- | --- | --- | --- | --- |
| 2147483648 | 2147483648.2 | foo | 2147483648 | 2147483648 | null |

Note that in this example, the last conversion of the string isn’t possible. When this happens, the result is a **null** value. In this case a *Warning* header is added to the response. The header will provide information on the source of the failure:

`"Line 1:133: evaluation of [TO_UL(str3)] failed, treating result as null. Only first 20 failures recorded."`

A following header will contain the failure reason and the offending value:

`"java.lang.NumberFormatException: Character f is neither a decimal digit number, decimal point, + "nor "e" notation exponential mark."`


