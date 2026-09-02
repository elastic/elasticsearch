---
applies_to:
  stack:
  serverless:
navigation_title: "NULL values"
description: How ES|QL represents and evaluates NULL values in expressions, filters, sorting, aggregation, unmapped fields, and multivalued fields.
---

# Working with NULL values in {{esql}} [esql-null-values]

`NULL` represents a value that is unknown, missing, or unavailable in a result row. It is common when a document has no value for a field, when a field is not mapped for part of a query, or when an expression cannot produce a value.

The most important thing to know is that `NULL` is not the same as `false`, an empty string, or `0`. Many expressions that involve `NULL` evaluate to `NULL`, and [`WHERE`](commands/where.md) keeps only rows where the condition is `true`. This can make rows disappear unless you handle `NULL` explicitly.

Use this page to avoid the most common `NULL` gotchas, then refer to the sections that follow for the details behind each rule.

## Common NULL gotchas [esql-null-gotchas]

Use these patterns to avoid unexpected behavior:

| ❌ Avoid | ✅ Use instead | Why |
| --- | --- | --- |
| `field == NULL` | `field IS NULL` | Comparisons with `NULL` return `NULL`, not `true`. **Learn more:** [Test for NULL values](#esql-test-for-null). |
| `field != NULL` | `field IS NOT NULL` | Comparisons with `NULL` return `NULL`, not `false`. **Learn more:** [Test for NULL values](#esql-test-for-null). |
| `WHERE field != "x"` when you also want missing values | `WHERE field != "x" OR field IS NULL` | `WHERE` drops rows where the comparison returns `NULL`. **Learn more:** [Comparisons and NULL](#esql-null-comparisons). |
| `WHERE NOT field == "x"` when you also want missing values | `WHERE field != "x" OR field IS NULL` | `NOT NULL` is still `NULL`. **Learn more:** [Boolean logic with NULL](#esql-null-boolean-logic). |
| `WHERE optional_field < 100` when null rows should remain | `WHERE optional_field < 100 OR optional_field IS NULL` | `WHERE` keeps only `true`, not `NULL`. **Learn more:** [WHERE and NULL](#esql-where-null). |
| `COUNT(condition OR NULL)` | `COUNT(*) WHERE condition` | Filtered aggregates state the condition directly. **Learn more:** [Aggregates and NULL](#esql-null-aggregates). |

:::{warning}
Rows can disappear when a `WHERE` condition evaluates to `NULL`. `WHERE` keeps only rows where the condition is `true`; it drops both `false` and `NULL`.
:::

## Test for NULL values [esql-test-for-null]

Use `IS NULL` and `IS NOT NULL` to test whether an expression evaluates to a `NULL` value.

The following query returns rows where `languages` evaluates to `NULL`:

```esql
FROM employees
| WHERE languages IS NULL
```

Do not use equality or inequality comparisons to test for `NULL`. A comparison with `NULL` evaluates to `NULL`, not to `true` or `false`.

:::{tip}
If the question is "does this value exist?", use `IS NULL` or `IS NOT NULL`.
:::

The following query compares equality and inequality checks with the `IS NULL` predicate:

```esql
ROW x = NULL
| EVAL eq_null = x == NULL, neq_null = x != NULL, is_null = x IS NULL
```

::::{dropdown} Example response
```text
       x | eq_null | neq_null | is_null
---------+---------+----------+--------
    null |    null |     null |    true
```
::::

Refer to [`IS NULL`](functions-operators/operators.md#esql-is_null) and [`IS NOT NULL`](functions-operators/operators.md#esql-is_not_null).

## Comparisons and NULL [esql-null-comparisons]

Comparisons involving `NULL` evaluate to `NULL`. This includes `==`, `!=`, `<`, `<=`, `>`, and `>=`.

This can be surprising with exclusions. The following query does not keep rows where `process.name` is `NULL`:

```esql
FROM logs
| WHERE process.name != "svchost.exe"
```

When `process.name` is `NULL`, the comparison evaluates to `NULL`, and `WHERE` drops the row. If you want to keep rows with a different process name and rows with no process name, include the null case explicitly:

```esql
FROM logs
| WHERE process.name != "svchost.exe" OR process.name IS NULL
```

The same rule applies when negating a comparison:

```esql
FROM logs
| WHERE NOT process.name == "svchost.exe"
```

Rows where `process.name` is `NULL` are still dropped, because `process.name == "svchost.exe"` evaluates to `NULL`, and `NOT NULL` is also `NULL`.

:::{warning}
Negating a comparison is not the same as including missing values. If the comparison returns `NULL`, `NOT` also returns `NULL`.
:::

## Boolean logic with NULL [esql-null-boolean-logic]

Boolean operators use three-valued logic. `NULL` means unknown, so it is preserved unless the other operand determines the result.

Read the `AND` and `OR` tables as operator result matrices. Choose the left operand from the first column and the right operand from the header row; the cell where they meet is the result.

| left `AND` right | `true` | `false` | `NULL` |
| --- | --- | --- | --- |
| **`true`** | `true` | `false` | `NULL` |
| **`false`** | `false` | `false` | `false` |
| **`NULL`** | `NULL` | `false` | `NULL` |

| left `OR` right | `true` | `false` | `NULL` |
| --- | --- | --- | --- |
| **`true`** | `true` | `true` | `true` |
| **`false`** | `true` | `false` | `NULL` |
| **`NULL`** | `true` | `NULL` | `NULL` |

| expression | result |
| --- | --- |
| `NOT true` | `false` |
| `NOT false` | `true` |
| `NOT NULL` | `NULL` |

## WHERE and NULL [esql-where-null]

`WHERE` keeps rows only when the condition evaluates to `true`. Rows where the condition evaluates to `false` or `NULL` are filtered out.

The following query returns no rows, because `x == NULL` evaluates to `NULL`:

```esql
ROW x = NULL
| WHERE x == NULL
```

::::{dropdown} Example response
```text
Empty result set
```
::::

Use `IS NULL` to keep rows where an expression evaluates to `NULL`:

```esql
ROW x = NULL
| WHERE x IS NULL
```

::::{dropdown} Example response
```text
       x
--------
    null
```
::::

The `IS NULL` predicate returns `true`, so `WHERE` keeps the row.

If you want a filter to keep rows where a value is either missing or matches another condition, include both cases:

```esql
FROM employees
| WHERE languages < 3 OR languages IS NULL
```

## Expressions and functions [esql-null-functions]

Many scalar functions return `NULL` when an input is `NULL`. This preserves unknown or unavailable values instead of inventing a result.

Use conditional functions and expressions when you want to replace or branch on `NULL` values:

```esql
ROW department = NULL
| EVAL department = COALESCE(department, "Unknown")
```

::::{dropdown} Example response
```text
department
----------
Unknown
```
::::

Useful references:

- [`COALESCE`](functions-operators/conditional-functions-and-expressions/coalesce.md) returns the first non-null value.
- [`CASE`](functions-operators/conditional-functions-and-expressions/case.md) chooses a result based on conditions.
- [Type conversion functions](functions-operators/type-conversion-functions.md) can produce `NULL` when a value cannot be converted.

For exact behavior, check the reference page for the function you are using.

## Aggregates and NULL [esql-null-aggregates]

Aggregate functions handle `NULL` depending on the function.

Common cases:

- `COUNT(*)` and `COUNT()` count rows.
- `COUNT(field)` counts non-null values in `field`.
- `COUNT(NULL)` returns `0`.
- `COUNT(false)` returns `1`, because `false` is a non-null value.
- Other aggregates generally ignore null input values and return `NULL` when there are no values to aggregate. Check each aggregate function's reference page for details.
- Grouping by a `NULL` expression creates a group with a `NULL` key.

The following query shows the difference between counting rows and counting non-null values:

```esql
ROW x = NULL
| STATS rows = COUNT(*), values = COUNT(x), nulls = COUNT(NULL)
```

::::{dropdown} Example response
```text
    rows |  values |   nulls
---------+---------+--------
       1 |       0 |       0
```
::::

Do not use `COUNT(condition OR NULL)` unless you specifically want to rely on three-valued logic. Prefer a filtered aggregate:

```esql
FROM employees
| STATS hired = COUNT(*) WHERE still_hired
```

Other aggregates commonly return `NULL` when every input value is `NULL`. The following query has a row to aggregate, but no non-null value for `SUM`:

```esql
ROW x = NULL
| STATS sum_x = SUM(TO_INTEGER(x))
```

::::{dropdown} Example response
```text
   sum_x
--------
    null
```
::::

Refer to [`COUNT`](functions-operators/aggregation-functions/count.md) and [aggregation functions](functions-operators/aggregation-functions.md).

## Sorting and NULL [esql-null-sorting]

By default, `NULL` values are treated as larger than any other value. This means:

- ascending sorts put `NULL` values last
- descending sorts put `NULL` values first

Use `NULLS FIRST` or `NULLS LAST` to choose a different placement:

```esql
FROM employees
| SORT first_name ASC NULLS FIRST
```

Refer to [`SORT`](commands/sort.md).

## Missing values, unmapped fields, and NULL [esql-missing-unmapped-null]

Missing values, unmapped fields, and `NULL` are related but not the same thing.

:::{note}
Missing is data-level, unmapped is schema-level, and `NULL` is the {{esql}} value that a query can evaluate or return.
:::

- A missing value is a document or result row that has no value for a field. In {{esql}}, this often appears as `NULL`.
- An unmapped field is a field that exists in indexed documents but is not defined in the index mapping. This is a schema-level condition.
- With `SET unmapped_fields = "nullify"` {applies_to}`stack: preview =9.3, ga 9.4+`, fully unmapped fields return `NULL`.
- With `SET unmapped_fields = "load"` {applies_to}`stack: preview =9.4, ga 9.5+`, {{esql}} can load real values from `_source`; values absent from `_source` still return `NULL`.
- Runtime fields are computed fields in the mapping. {{esql}} treats mapped runtime fields like other mapped fields.

Refer to [unmapped fields](esql-unmapped-fields.md) and [`SET unmapped_fields`](directives/set.md#esql-unmapped_fields).

## Multivalued fields and NULL [esql-multivalued-null]

A multivalued field with one or more values is different from `NULL`. An empty multivalued field can appear as `NULL` in {{esql}}.

Some scalar comparisons and functions return `NULL` when a multivalued value cannot be reduced to a single value. Also, `MV_APPEND` currently returns `NULL` when either input is `NULL`.

The following query appends a `NULL` value to a multivalued value:

```esql
ROW values = MV_APPEND(1, 2)
| EVAL append_null = MV_APPEND(values, NULL)
```

::::{dropdown} Example response
```text
  values | append_null
---------+------------
  [1, 2] |        null
```
::::

Refer to [multivalued fields](esql-multivalued-fields.md) and [`MV_APPEND`](functions-operators/mv-functions/mv_append.md).

## Related reference [esql-null-related]

- [`WHERE`](commands/where.md)
- [`SORT`](commands/sort.md)
- [`IS NULL`](functions-operators/operators.md#esql-is_null) and [`IS NOT NULL`](functions-operators/operators.md#esql-is_not_null)
- [Conditional functions and expressions](functions-operators/conditional-functions-and-expressions.md)
- [Aggregation functions](functions-operators/aggregation-functions.md)
- [Multivalued fields](esql-multivalued-fields.md)
- [Unmapped fields](esql-unmapped-fields.md)
