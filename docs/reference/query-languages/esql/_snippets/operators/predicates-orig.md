## `IS NULL` and `IS NOT NULL` predicates [esql-predicates]

For NULL comparison, use the `IS NULL` and `IS NOT NULL` predicates.

**Examples**

```esql
FROM employees
| WHERE birth_date IS NULL
| KEEP first_name, last_name
| SORT first_name
| LIMIT 3
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Basil | Tramer |
| Florian | Syrotiuk |
| Lucien | Rosenbaum |

```esql
FROM employees
| WHERE is_rehired IS NOT NULL
| STATS COUNT(emp_no)
```

| COUNT(emp_no):long |
| --- |
| 84 |
