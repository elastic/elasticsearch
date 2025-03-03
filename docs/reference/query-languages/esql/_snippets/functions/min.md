## `MIN` [esql-min]

**Syntax**

:::{image} ../../../../../images/min.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

The minimum value of a field.

**Supported types**

| field | result |
| --- | --- |
| boolean | boolean |
| date | date |
| date_nanos | date_nanos |
| double | double |
| integer | integer |
| ip | ip |
| keyword | keyword |
| long | long |
| text | keyword |
| version | version |

**Examples**

```esql
FROM employees
| STATS MIN(languages)
```

| MIN(languages):integer |
| --- |
| 1 |

The expression can use inline functions. For example, to calculate the minimum over an average of a multivalued column, use `MV_AVG` to first average the multiple values per row, and use the result with the `MIN` function

```esql
FROM employees
| STATS min_avg_salary_change = MIN(MV_AVG(salary_change))
```

| min_avg_salary_change:double |
| --- |
| -8.46 |


