## `TOP` [esql-top]

**Syntax**

:::{image} ../../../../../images/top.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   The field to collect the top values for.

`limit`
:   The maximum number of values to collect.

`order`
:   The order to calculate the top values. Either `asc` or `desc`.

**Description**

Collects the top values for a field. Includes repeated values.

**Supported types**

| field | limit | order | result |
| --- | --- | --- | --- |
| boolean | integer | keyword | boolean |
| date | integer | keyword | date |
| double | integer | keyword | double |
| integer | integer | keyword | integer |
| ip | integer | keyword | ip |
| keyword | integer | keyword | keyword |
| long | integer | keyword | long |
| text | integer | keyword | keyword |

**Example**

```esql
FROM employees
| STATS top_salaries = TOP(salary, 3, "desc"), top_salary = MAX(salary)
```

| top_salaries:integer | top_salary:integer |
| --- | --- |
| [74999, 74970, 74572] | 74999 |


