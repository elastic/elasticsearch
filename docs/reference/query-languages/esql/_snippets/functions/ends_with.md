## `ENDS_WITH` [esql-ends_with]

**Syntax**

:::{image} ../../../../../images/ends_with.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

`suffix`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a boolean that indicates whether a keyword string ends with another string.

**Supported types**

| str | suffix | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| keyword | text | boolean |
| text | keyword | boolean |
| text | text | boolean |

**Example**

```esql
FROM employees
| KEEP last_name
| EVAL ln_E = ENDS_WITH(last_name, "d")
```

| last_name:keyword | ln_E:boolean |
| --- | --- |
| Awdeh | false |
| Azuma | false |
| Baek | false |
| Bamford | true |
| Bernatsky | false |


