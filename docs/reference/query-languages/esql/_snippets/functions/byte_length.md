## `BYTE_LENGTH` [esql-byte_length]

**Syntax**

:::{image} ../../../../../images/byte_length.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns the byte length of a string.

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
| EVAL fn_length = LENGTH(city), fn_byte_length = BYTE_LENGTH(city)
```

| city:keyword | fn_length:integer | fn_byte_length:integer |
| --- | --- | --- |
| Agwār | 5 | 6 |
| Ahmedabad | 9 | 9 |
| Bangalore | 9 | 9 |


