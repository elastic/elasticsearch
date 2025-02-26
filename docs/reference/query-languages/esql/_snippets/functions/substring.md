## `SUBSTRING` [esql-substring]

**Syntax**

:::{image} ../../../../../images/substring.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

`start`
:   Start position.

`length`
:   Length of the substring from the start position. Optional; if omitted, all positions after `start` are returned.

**Description**

Returns a substring of a string, specified by a start position and an optional length.

**Supported types**

| string | start | length | result |
| --- | --- | --- | --- |
| keyword | integer | integer | keyword |
| text | integer | integer | keyword |

**Examples**

This example returns the first three characters of every last name:

```esql
FROM employees
| KEEP last_name
| EVAL ln_sub = SUBSTRING(last_name, 1, 3)
```

| last_name:keyword | ln_sub:keyword |
| --- | --- |
| Awdeh | Awd |
| Azuma | Azu |
| Baek | Bae |
| Bamford | Bam |
| Bernatsky | Ber |

A negative start position is interpreted as being relative to the end of the string. This example returns the last three characters of of every last name:

```esql
FROM employees
| KEEP last_name
| EVAL ln_sub = SUBSTRING(last_name, -3, 3)
```

| last_name:keyword | ln_sub:keyword |
| --- | --- |
| Awdeh | deh |
| Azuma | uma |
| Baek | aek |
| Bamford | ord |
| Bernatsky | sky |

If length is omitted, substring returns the remainder of the string. This example returns all characters except for the first:

```esql
FROM employees
| KEEP last_name
| EVAL ln_sub = SUBSTRING(last_name, 2)
```

| last_name:keyword | ln_sub:keyword |
| --- | --- |
| Awdeh | wdeh |
| Azuma | zuma |
| Baek | aek |
| Bamford | amford |
| Bernatsky | ernatsky |


