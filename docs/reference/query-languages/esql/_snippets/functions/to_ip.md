## `TO_IP` [esql-to_ip]

**Syntax**

:::{image} ../../../../../images/to_ip.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input string to an IP value.

**Supported types**

| field | result |
| --- | --- |
| ip | ip |
| keyword | ip |
| text | ip |

**Example**

```esql
ROW str1 = "1.1.1.1", str2 = "foo"
| EVAL ip1 = TO_IP(str1), ip2 = TO_IP(str2)
| WHERE CIDR_MATCH(ip1, "1.0.0.0/8")
```

| str1:keyword | str2:keyword | ip1:ip | ip2:ip |
| --- | --- | --- | --- |
| 1.1.1.1 | foo | 1.1.1.1 | null |

Note that in this example, the last conversion of the string isn’t possible. When this happens, the result is a **null** value. In this case a *Warning* header is added to the response. The header will provide information on the source of the failure:

`"Line 1:68: evaluation of [TO_IP(str2)] failed, treating result as null. Only first 20 failures recorded."`

A following header will contain the failure reason and the offending value:

`"java.lang.IllegalArgumentException: 'foo' is not an IP string literal."`


