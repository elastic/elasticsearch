## `STATS` [esql-stats-by]

The `STATS` processing command groups rows according to a common value and calculates one or more aggregated values over the grouped rows.

**Syntax**

```esql
STATS [column1 =] expression1 [WHERE boolean_expression1][,
      ...,
      [columnN =] expressionN [WHERE boolean_expressionN]]
      [BY grouping_expression1[, ..., grouping_expressionN]]
```

**Parameters**

`columnX`
:   The name by which the aggregated value is returned. If omitted, the name is equal to the corresponding expression (`expressionX`). If multiple columns have the same name, all but the rightmost column with this name will be ignored.

`expressionX`
:   An expression that computes an aggregated value.

`grouping_expressionX`
:   An expression that outputs the values to group by. If its name coincides with one of the computed columns, that column will be ignored.

`boolean_expressionX`
:   The condition that must be met for a row to be included in the evaluation of `expressionX`.

::::{note}
Individual `null` values are skipped when computing aggregations.
::::


**Description**

The `STATS` processing command groups rows according to a common value
and calculates one or more aggregated values over the grouped rows. For the
calculation of each aggregated value, the rows in a group can be filtered with
`WHERE`. If `BY` is omitted, the output table contains exactly one row with
the aggregations applied over the entire dataset.

The following [aggregation functions](/reference/query-languages/esql/functions-operators/aggregation-functions.md) are supported:

* [`AVG`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-avg)
* [`COUNT`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-count)
* [`COUNT_DISTINCT`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-count_distinct)
* [`MAX`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-max)
* [`MEDIAN`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-median)
* [`MEDIAN_ABSOLUTE_DEVIATION`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-median_absolute_deviation)
* [`MIN`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-min)
* [`PERCENTILE`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-percentile)
* [preview] [`ST_CENTROID_AGG`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-st_centroid_agg)
* [preview] [`ST_EXTENT_AGG`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-st_extent_agg)
* [`STD_DEV`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-std_dev)
* [`SUM`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-sum)
* [`TOP`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-top)
* [`VALUES`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-values)
* [`WEIGHTED_AVG`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-weighted_avg)

The following [grouping functions](/reference/query-languages/esql/functions-operators/grouping-functions.md) are supported:

* [`BUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-bucket)
* [preview] [`CATEGORIZE`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-categorize)

::::{note}
`STATS` without any groups is much much faster than adding a group.
::::


::::{note}
Grouping on a single expression is currently much more optimized than grouping
on many expressions. In some tests we have seen grouping on a single `keyword`
column to be five times faster than grouping on two `keyword` columns. Do
not try to work around this by combining the two columns together with
something like [`CONCAT`](/reference/query-languages/esql/functions-operators/string-functions.md#esql-concat) and then grouping - that is not going to
be faster.
::::


**Examples**

Calculating a statistic and grouping by the values of another column:

```esql
FROM employees
| STATS count = COUNT(emp_no) BY languages
| SORT languages
```

| count:long | languages:integer |
| --- | --- |
| 15 | 1 |
| 19 | 2 |
| 17 | 3 |
| 18 | 4 |
| 21 | 5 |
| 10 | null |

Omitting `BY` returns one row with the aggregations applied over the entire dataset:

```esql
FROM employees
| STATS avg_lang = AVG(languages)
```

| avg_lang:double |
| --- |
| 3.1222222222222222 |

It’s possible to calculate multiple values:

```esql
FROM employees
| STATS avg_lang = AVG(languages), max_lang = MAX(languages)
```

| avg_lang:double | max_lang:integer |
| --- | --- |
| 3.1222222222222222 | 5 |

To filter the rows that go into an aggregation, use the `WHERE` clause:

```esql
FROM employees
| STATS avg50s = AVG(salary)::LONG WHERE birth_date < "1960-01-01",
        avg60s = AVG(salary)::LONG WHERE birth_date >= "1960-01-01"
        BY gender
| SORT gender
```

| avg50s:long | avg60s:long | gender:keyword |
| --- | --- | --- |
| 55462 | 46637 | F |
| 48279 | 44879 | M |

The aggregations can be mixed, with and without a filter and grouping is optional as well:

```esql
FROM employees
| EVAL Ks = salary / 1000 // thousands
| STATS under_40K = COUNT(*) WHERE Ks < 40,
        inbetween = COUNT(*) WHERE 40 <= Ks AND Ks < 60,
        over_60K  = COUNT(*) WHERE 60 <= Ks,
        total     = COUNT(*)
```

| under_40K:long | inbetween:long | over_60K:long | total:long |
| --- | --- | --- | --- |
| 36 | 39 | 25 | 100 |

$$$esql-stats-mv-group$$$
If the grouping key is multivalued then the input row is in all groups:

```esql
ROW i=1, a=["a", "b"] | STATS MIN(i) BY a | SORT a ASC
```

| MIN(i):integer | a:keyword |
| --- | --- |
| 1 | a |
| 1 | b |

It’s also possible to group by multiple values:

```esql
FROM employees
| EVAL hired = DATE_FORMAT("yyyy", hire_date)
| STATS avg_salary = AVG(salary) BY hired, languages.long
| EVAL avg_salary = ROUND(avg_salary)
| SORT hired, languages.long
```

If all the grouping keys are multivalued then the input row is in all groups:

```esql
ROW i=1, a=["a", "b"], b=[2, 3] | STATS MIN(i) BY a, b | SORT a ASC, b ASC
```

| MIN(i):integer | a:keyword | b:integer |
| --- | --- | --- |
| 1 | a | 2 |
| 1 | a | 3 |
| 1 | b | 2 |
| 1 | b | 3 |

Both the aggregating functions and the grouping expressions accept other functions. This is useful for using `STATS` on multivalue columns. For example, to calculate the average salary change, you can use `MV_AVG` to first average the multiple values per employee, and use the result with the `AVG` function:

```esql
FROM employees
| STATS avg_salary_change = ROUND(AVG(MV_AVG(salary_change)), 10)
```

| avg_salary_change:double |
| --- |
| 1.3904535865 |

An example of grouping by an expression is grouping employees on the first letter of their last name:

```esql
FROM employees
| STATS my_count = COUNT() BY LEFT(last_name, 1)
| SORT `LEFT(last_name, 1)`
```

| my_count:long | LEFT(last_name, 1):keyword |
| --- | --- |
| 2 | A |
| 11 | B |
| 5 | C |
| 5 | D |
| 2 | E |
| 4 | F |
| 4 | G |
| 6 | H |
| 2 | J |
| 3 | K |
| 5 | L |
| 12 | M |
| 4 | N |
| 1 | O |
| 7 | P |
| 5 | R |
| 13 | S |
| 4 | T |
| 2 | W |
| 3 | Z |

Specifying the output column name is optional. If not specified, the new column name is equal to the expression. The following query returns a column named `AVG(salary)`:

```esql
FROM employees
| STATS AVG(salary)
```

| AVG(salary):double |
| --- |
| 48248.55 |

Because this name contains special characters, [it needs to be quoted](/reference/query-languages/esql/esql-syntax.md#esql-identifiers) with backticks (```) when using it in subsequent commands:

```esql
FROM employees
| STATS AVG(salary)
| EVAL avg_salary_rounded = ROUND(`AVG(salary)`)
```

| AVG(salary):double | avg_salary_rounded:double |
| --- | --- |
| 48248.55 | 48249.0 |


