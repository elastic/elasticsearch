## `TO_INTEGER` [esql-to_integer]

**Syntax**

:::{image} ../../../../../images/to_integer.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to an integer value. If the input parameter is of a date type, its value will be interpreted as milliseconds since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time), converted to integer. Boolean **true** will be converted to integer **1**, **false** to **0**.

**Supported types**

| field | result |
| --- | --- |
| boolean | integer |
| counter_integer | integer |
| date | integer |
| double | integer |
| integer | integer |
| keyword | integer |
| long | integer |
| text | integer |
| unsigned_long | integer |

**Example**

```esql
ROW long = [5013792, 2147483647, 501379200000]
| EVAL int = TO_INTEGER(long)
```

| long:long | int:integer |
| --- | --- |
| [5013792, 2147483647, 501379200000] | [5013792, 2147483647] |

Note that in this example, the last value of the multi-valued field cannot be converted as an integer. When this happens, the result is a **null** value. In this case a *Warning* header is added to the response. The header will provide information on the source of the failure:

`"Line 1:61: evaluation of [TO_INTEGER(long)] failed, treating result as null. Only first 20 failures recorded."`

A following header will contain the failure reason and the offending value:

`"org.elasticsearch.xpack.esql.core.InvalidArgumentException: [501379200000] out of [integer] range"`


