## `BIT_LENGTH` [esql-bit_length]

**Syntax**

:::{image} ../../../../../images/bit_length.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns the bit length of a string.

::::{note}
All strings are in UTF-8, so a single character can use multiple bytes.
::::


**Supported types**

| string | result |
| --- | --- |
| keyword | integer |
| text | integer |

**Example**

```esql
FROM airports
| WHERE country == "India"
| KEEP city
| EVAL fn_length = LENGTH(city), fn_bit_length = BIT_LENGTH(city)
```

| city:keyword | fn_length:integer | fn_bit_length:integer |
| --- | --- | --- |
| Agwār | 5 | 48 |
| Ahmedabad | 9 | 72 |
| Bangalore | 9 | 72 |


