## `TO_STRING` [esql-to_string]

**Syntax**

:::{image} ../../../../../images/to_string.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value into a string.

**Supported types**

| field | result |
| --- | --- |
| boolean | keyword |
| cartesian_point | keyword |
| cartesian_shape | keyword |
| date | keyword |
| date_nanos | keyword |
| double | keyword |
| geo_point | keyword |
| geo_shape | keyword |
| integer | keyword |
| ip | keyword |
| keyword | keyword |
| long | keyword |
| text | keyword |
| unsigned_long | keyword |
| version | keyword |

**Examples**

```esql
ROW a=10
| EVAL j = TO_STRING(a)
```

| a:integer | j:keyword |
| --- | --- |
| 10 | "10" |

It also works fine on multivalued fields:

```esql
ROW a=[10, 9, 8]
| EVAL j = TO_STRING(a)
```

| a:integer | j:keyword |
| --- | --- |
| [10, 9, 8] | ["10", "9", "8"] |


