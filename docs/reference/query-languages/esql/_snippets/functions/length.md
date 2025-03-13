## `LENGTH` [esql-length]

**Syntax**

:::{image} ../../../../../images/length.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns the character length of a string.

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
| EVAL fn_length = LENGTH(city)
```

| city:keyword | fn_length:integer |
| --- | --- |
| Agwār | 5 |
| Ahmedabad | 9 |
| Bangalore | 9 |


