## `CONCAT` [esql-concat]

**Syntax**

:::{image} ../../../../../images/concat.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string1`
:   Strings to concatenate.

`string2`
:   Strings to concatenate.

**Description**

Concatenates two or more strings.

**Supported types**

| string1 | string2 | result |
| --- | --- | --- |
| keyword | keyword | keyword |
| keyword | text | keyword |
| text | keyword | keyword |
| text | text | keyword |

**Example**

```esql
FROM employees
| KEEP first_name, last_name
| EVAL fullname = CONCAT(first_name, " ", last_name)
```

| first_name:keyword | last_name:keyword | fullname:keyword |
| --- | --- | --- |
| Alejandro | McAlpine | Alejandro McAlpine |
| Amabile | Gomatam | Amabile Gomatam |
| Anneke | Preusig | Anneke Preusig |


