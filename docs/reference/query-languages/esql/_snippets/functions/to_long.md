## `TO_LONG` [esql-to_long]

**Syntax**

:::{image} ../../../../../images/to_long.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to a long value. If the input parameter is of a date type, its value will be interpreted as milliseconds since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time), converted to long. Boolean **true** will be converted to long **1**, **false** to **0**.

**Supported types**

| field | result |
| --- | --- |
| boolean | long |
| counter_integer | long |
| counter_long | long |
| date | long |
| date_nanos | long |
| double | long |
| integer | long |
| keyword | long |
| long | long |
| text | long |
| unsigned_long | long |

**Example**

```esql
ROW str1 = "2147483648", str2 = "2147483648.2", str3 = "foo"
| EVAL long1 = TO_LONG(str1), long2 = TO_LONG(str2), long3 = TO_LONG(str3)
```

| str1:keyword | str2:keyword | str3:keyword | long1:long | long2:long | long3:long |
| --- | --- | --- | --- | --- | --- |
| 2147483648 | 2147483648.2 | foo | 2147483648 | 2147483648 | null |

Note that in this example, the last conversion of the string isn’t possible. When this happens, the result is a **null** value. In this case a *Warning* header is added to the response. The header will provide information on the source of the failure:

`"Line 1:113: evaluation of [TO_LONG(str3)] failed, treating result as null. Only first 20 failures recorded."`

A following header will contain the failure reason and the offending value:

`"java.lang.NumberFormatException: For input string: "foo""`


