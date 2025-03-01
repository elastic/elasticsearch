## `DATE_FORMAT` [esql-date_format]

**Syntax**

:::{image} ../../../../../images/date_format.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`dateFormat`
:   Date format (optional).  If no format is specified, the `yyyy-MM-dd'T'HH:mm:ss.SSSZ` format is used. If `null`, the function returns `null`.

`date`
:   Date expression. If `null`, the function returns `null`.

**Description**

Returns a string representation of a date, in the provided format.

**Supported types**

| dateFormat | date | result |
| --- | --- | --- |
| date |  | keyword |
| date_nanos |  | keyword |
| keyword | date | keyword |
| keyword | date_nanos | keyword |
| text | date | keyword |
| text | date_nanos | keyword |

**Example**

```esql
FROM employees
| KEEP first_name, last_name, hire_date
| EVAL hired = DATE_FORMAT("yyyy-MM-dd", hire_date)
```

| first_name:keyword | last_name:keyword | hire_date:date | hired:keyword |
| --- | --- | --- | --- |
| Alejandro | McAlpine | 1991-06-26T00:00:00.000Z | 1991-06-26 |
| Amabile | Gomatam | 1992-11-18T00:00:00.000Z | 1992-11-18 |
| Anneke | Preusig | 1989-06-02T00:00:00.000Z | 1989-06-02 |


