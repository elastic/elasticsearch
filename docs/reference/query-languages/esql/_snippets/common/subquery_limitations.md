#### Nested FROM subqueries

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }
A `FROM` subquery can contain further `FROM` subqueries. Nested branches are
subject to the query-wide branch and command limits described in
[How views execute](/reference/query-languages/esql/esql-views.md#how-views-execute).

For example, each outer subquery in this query contains further `FROM`
subqueries:

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
:::
:::{applies-item} stack: preview =9.4, ga =9.5
A `FROM` subquery cannot contain another `FROM` subquery. Only one level of
`FROM` nesting is allowed. A `FROM` subquery can still contain
[`IN` subqueries](/reference/query-languages/esql/esql-in-subquery.md) in its
`WHERE` commands, `EVAL` commands, and `STATS` or `INLINE STATS` per-aggregate
`WHERE` filters.

Use only a single level of subqueries:

```esql
FROM
    (FROM employees | WHERE emp_no > 10090),
    (FROM sample_data | WHERE client_ip == "172.21.3.15"),
    (FROM employees | WHERE emp_no < 10010),
    (FROM sample_data | WHERE client_ip == "172.21.0.5")
| KEEP emp_no, languages, client_ip
```
:::
::::

#### FORK with FROM subqueries

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }
[`FORK`](/reference/query-languages/esql/commands/fork.md) can be used inside a
`FROM` subquery or after a `FROM` that contains subqueries. Two user-written
`FORK` commands still can't occur along the same uninterrupted pipeline.

For example, `FORK` can be used inside a subquery:

```esql
FROM
    (FROM employees
     | FORK
         (WHERE emp_no > 10090)
         (WHERE emp_no < 10010))
| KEEP emp_no, languages
```

It can also be used after a `FROM` with subqueries:

```esql
FROM
    employees,
    (FROM sample_data | WHERE client_ip == "172.21.3.15")
| FORK
    (WHERE emp_no > 10090)
    (WHERE client_ip IS NOT NULL)
```
:::
:::{applies-item} stack: preview =9.4, ga =9.5
`FORK` can't be used inside a `FROM` subquery or after a `FROM` that contains
subqueries.
:::
::::
