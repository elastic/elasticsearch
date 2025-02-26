## `LOG` [esql-log]

**Syntax**

:::{image} ../../../../../images/log.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`base`
:   Base of logarithm. If `null`, the function returns `null`. If not provided, this function returns the natural logarithm (base e) of a value.

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the logarithm of a value to a base. The input can be any numeric value, the return value is always a double.  Logs of zero, negative numbers, and base of one return `null` as well as a warning.

**Supported types**

| base | number | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| double | unsigned_long | double |
| double |  | double |
| integer | double | double |
| integer | integer | double |
| integer | long | double |
| integer | unsigned_long | double |
| integer |  | double |
| long | double | double |
| long | integer | double |
| long | long | double |
| long | unsigned_long | double |
| long |  | double |
| unsigned_long | double | double |
| unsigned_long | integer | double |
| unsigned_long | long | double |
| unsigned_long | unsigned_long | double |
| unsigned_long |  | double |

**Examples**

```esql
ROW base = 2.0, value = 8.0
| EVAL s = LOG(base, value)
```

| base: double | value: double | s:double |
| --- | --- | --- |
| 2.0 | 8.0 | 3.0 |

```esql
row value = 100
| EVAL s = LOG(value);
```

| value: integer | s:double |
| --- | --- |
| 100 | 4.605170185988092 |


