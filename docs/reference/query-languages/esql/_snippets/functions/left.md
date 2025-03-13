## `LEFT` [esql-left]

**Syntax**

:::{image} ../../../../../images/left.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   The string from which to return a substring.

`length`
:   The number of characters to return.

**Description**

Returns the substring that extracts *length* chars from *string* starting from the left.

**Supported types**

| string | length | result |
| --- | --- | --- |
| keyword | integer | keyword |
| text | integer | keyword |

**Example**

```esql
FROM employees
| KEEP last_name
| EVAL left = LEFT(last_name, 3)
| SORT last_name ASC
| LIMIT 5
```

| last_name:keyword | left:keyword |
| --- | --- |
| Awdeh | Awd |
| Azuma | Azu |
| Baek | Bae |
| Bamford | Bam |
| Bernatsky | Ber |


