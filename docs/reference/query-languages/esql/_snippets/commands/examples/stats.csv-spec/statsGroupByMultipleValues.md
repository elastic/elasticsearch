```esql
FROM employees
| EVAL hired = DATE_FORMAT("yyyy", hire_date)
| STATS avg_salary = AVG(salary) BY hired, languages.long
| EVAL avg_salary = ROUND(avg_salary)
| SORT hired, languages.long
```

