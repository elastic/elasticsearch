## `ABS` [esql-abs]

**Syntax**

:::{image} ../../../../../images/abs.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the absolute value.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Examples**

```esql
ROW number = -1.0
| EVAL abs_number = ABS(number)
```

| number:double | abs_number:double |
| --- | --- |
| -1.0 | 1.0 |

```esql
FROM employees
| KEEP first_name, last_name, height
| EVAL abs_height = ABS(0.0 - height)
```

| first_name:keyword | last_name:keyword | height:double | abs_height:double |
| --- | --- | --- | --- |
| Alejandro | McAlpine | 1.48 | 1.48 |
| Amabile | Gomatam | 2.09 | 2.09 |
| Anneke | Preusig | 1.56 | 1.56 |


