## `STARTS_WITH` [esql-starts_with]

**Syntax**

:::{image} ../../../../../images/starts_with.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

`prefix`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a boolean that indicates whether a keyword string starts with another string.

**Supported types**

| str | prefix | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| keyword | text | boolean |
| text | keyword | boolean |
| text | text | boolean |

**Example**

```esql
FROM employees
| KEEP last_name
| EVAL ln_S = STARTS_WITH(last_name, "B")
```

| last_name:keyword | ln_S:boolean |
| --- | --- |
| Awdeh | false |
| Azuma | false |
| Baek | true |
| Bamford | true |
| Bernatsky | true |


