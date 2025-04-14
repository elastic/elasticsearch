---
navigation_title: "Implicit casting"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-implicit-casting.html
---

# {{esql}} implicit casting [esql-implicit-casting]


Often users will input `date`, `date_period`, `time_duration`, `ip` or `version` as simple strings in their queries for use in predicates, functions, or expressions. {{esql}} provides [type conversion functions](/reference/query-languages/esql/functions-operators/type-conversion-functions.md) to explicitly convert these strings into the desired data types.

Without implicit casting users must explicitly code these `to_X` functions in their queries, when string literals don’t match the target data types they are assigned or compared to. Here is an example of using `to_datetime` to explicitly perform a data type conversion.

```esql
FROM employees
| EVAL dd_ns1=date_diff("day", to_datetime("2023-12-02T11:00:00.00Z"), birth_date)
| SORT emp_no
| KEEP dd_ns1
| LIMIT 1
```


## Implicit casting example [esql-implicit-casting-example]

Implicit casting automatically converts string literals to the target data type. This allows users to specify string values for types like `date`, `date_period`, `time_duration`, `ip` and `version` in their queries.

The first query can be coded without calling the `to_datetime` function, as follows:

```esql
FROM employees
| EVAL dd_ns1=date_diff("day", "2023-12-02T11:00:00.00Z", birth_date)
| SORT emp_no
| KEEP dd_ns1
| LIMIT 1
```


## Operations that support implicit casting [esql-implicit-casting-supported-operations]

The following table details which {{esql}} operations support implicit casting for different data types.

|  | ScalarFunctions | Operators | [GroupingFunctions](/reference/query-languages/esql/functions-operators/grouping-functions.md) | [AggregateFunctions](/reference/query-languages/esql/functions-operators/aggregation-functions.md) |
| --- | --- | --- | --- | --- |
| DATE | Y | Y | Y | N |
| DATE_PERIOD/TIME_DURATION | Y | N | Y | N |
| IP | Y | Y | Y | N |
| VERSION | Y | Y | Y | N |
| BOOLEAN | Y | Y | Y | N |

ScalarFunctions includes:

* [Conditional Functions and Expressions](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions.md)
* [Date and Time Functions](/reference/query-languages/esql/functions-operators/date-time-functions.md)
* [IP Functions](/reference/query-languages/esql/functions-operators/ip-functions.md)

Operators includes:

* [Binary Operators](/reference/query-languages/esql/functions-operators/operators.md#esql-binary-operators)
* [Unary Operator](/reference/query-languages/esql/functions-operators/operators.md#esql-unary-operators)
* [IN](/reference/query-languages/esql/functions-operators/operators.md#esql-in-operator)

