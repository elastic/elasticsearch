## `RIGHT` [esql-right]

**Syntax**

:::{image} ../../../../../images/right.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   The string from which to returns a substring.

`length`
:   The number of characters to return.

**Description**

Return the substring that extracts *length* chars from *str* starting from the right.

**Supported types**

| string | length | result |
| --- | --- | --- |
| keyword | integer | keyword |
| text | integer | keyword |

**Example**

```esql
FROM employees
| KEEP last_name
| EVAL right = RIGHT(last_name, 3)
| SORT last_name ASC
| LIMIT 5
```

| last_name:keyword | right:keyword |
| --- | --- |
| Awdeh | deh |
| Azuma | uma |
| Baek | aek |
| Bamford | ord |
| Bernatsky | sky |


