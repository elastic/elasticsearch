#### Nested subqueries are not supported

A subquery cannot contain another subquery in the
[`FROM`](/reference/query-languages/esql/commands/from.md) command. Only one
level of nesting is allowed.

For example, this query is **not supported** because the inner `FROM` itself
contains subqueries:

```esql
FROM
    (FROM
        (FROM employees | WHERE emp_no > 10090),
        (FROM sample_data | WHERE client_ip == "172.21.3.15")
    ),
    (FROM
        (FROM employees | WHERE emp_no < 10010),
        (FROM sample_data | WHERE client_ip == "172.21.0.5")
    )
| KEEP emp_no, languages, client_ip
```

Instead, use only a single level of subqueries:

```esql
FROM
    (FROM employees | WHERE emp_no > 10090),
    (FROM sample_data | WHERE client_ip == "172.21.3.15"),
    (FROM employees | WHERE emp_no < 10010),
    (FROM sample_data | WHERE client_ip == "172.21.0.5")
| KEEP emp_no, languages, client_ip
```

#### FORK is not supported with subqueries

The [`FORK`](/reference/query-languages/esql/commands/fork.md) command cannot
be used inside a subquery or after a `FROM` that contains subqueries.

For example, using `FORK` **inside** a subquery is not supported:

```esql
FROM
    (FROM employees
     | FORK
         (WHERE emp_no > 10090)
         (WHERE emp_no < 10010))
| KEEP emp_no, languages
```

And using `FORK` **after** a `FROM` with subqueries is also not supported:

```esql
FROM
    employees,
    (FROM sample_data | WHERE client_ip == "172.21.3.15")
| FORK
    (WHERE emp_no > 10090)
    (WHERE client_ip IS NOT NULL)
```
