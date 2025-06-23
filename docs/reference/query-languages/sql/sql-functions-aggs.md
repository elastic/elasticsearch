---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-aggs.html
---

# Aggregate functions [sql-functions-aggs]

Functions for computing a *single* result from a set of input values. Elasticsearch SQL supports aggregate functions only alongside [grouping](/reference/query-languages/sql/sql-syntax-select.md#sql-syntax-group-by) (implicit or explicit).


## General purpose [sql-functions-aggs-general]

## `AVG` [sql-functions-aggs-avg]

```sql
AVG(numeric_field) <1>
```

**Input**:

1. numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `double` numeric value

**Description**: Returns the [Average](https://en.wikipedia.org/wiki/Arithmetic_mean) (arithmetic mean) of input values.

```sql
SELECT AVG(salary) AS avg FROM emp;

      avg
---------------
48248.55
```

```sql
SELECT AVG(salary / 12.0) AS avg FROM emp;

      avg
---------------
4020.7125
```


## `COUNT` [sql-functions-aggs-count]

```sql
COUNT(expression) <1>
```

**Input**:

1. a field name, wildcard (`*`) or any numeric value. For `COUNT(*)` or `COUNT(<literal>)`, all values are considered, including `null` or missing ones. For `COUNT(<field_name>)`, `null` values are not considered.


**Output**: numeric value

**Description**: Returns the total number (count) of input values.

```sql
SELECT COUNT(*) AS count FROM emp;

     count
---------------
100
```


## `COUNT(ALL)` [sql-functions-aggs-count-all]

```sql
COUNT(ALL field_name) <1>
```

**Input**:

1. a field name. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: numeric value

**Description**: Returns the total number (count) of all *non-null* input values. `COUNT(<field_name>)` and `COUNT(ALL <field_name>)` are equivalent.

```sql
SELECT COUNT(ALL last_name) AS count_all, COUNT(DISTINCT last_name) count_distinct FROM emp;

   count_all   |  count_distinct
---------------+------------------
100            |96
```

```sql
SELECT COUNT(ALL CASE WHEN languages IS NULL THEN -1 ELSE languages END) AS count_all, COUNT(DISTINCT CASE WHEN languages IS NULL THEN -1 ELSE languages END) count_distinct FROM emp;

   count_all   |  count_distinct
---------------+---------------
100            |6
```


## `COUNT(DISTINCT)` [sql-functions-aggs-count-distinct]

```sql
COUNT(DISTINCT field_name) <1>
```

**Input**:

1. a field name


**Output**: numeric value. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.

**Description**: Returns the total number of *distinct non-null* values in input values.

```sql
SELECT COUNT(DISTINCT hire_date) unique_hires, COUNT(hire_date) AS hires FROM emp;

  unique_hires  |     hires
----------------+---------------
99              |100
```

```sql
SELECT COUNT(DISTINCT DATE_TRUNC('YEAR', hire_date)) unique_hires, COUNT(DATE_TRUNC('YEAR', hire_date)) AS hires FROM emp;

 unique_hires  |     hires
---------------+---------------
14             |100
```


## `FIRST/FIRST_VALUE` [sql-functions-aggs-first]

```sql
FIRST(
    field_name               <1>
    [, ordering_field_name]) <2>
```

**Input**:

1. target field for the aggregation
2. optional field used for ordering


**Output**: same type as the input

**Description**: Returns the first non-`null` value (if such exists) of the `field_name` input column sorted by the `ordering_field_name` column. If `ordering_field_name` is not provided, only the `field_name` column is used for the sorting. E.g.:

| a | b |
| --- | --- |
| 100 | 1 |
| 200 | 1 |
| 1 | 2 |
| 2 | 2 |
| 10 | null |
| 20 | null |
| null | null |

```sql
SELECT FIRST(a) FROM t
```

will result in:

| **FIRST(a)** |
| --- |
| 1 |

and

```sql
SELECT FIRST(a, b) FROM t
```

will result in:

| **FIRST(a, b)** |
| --- |
| 100 |

```sql
SELECT FIRST(first_name) FROM emp;

   FIRST(first_name)
--------------------
Alejandro
```

```sql
SELECT gender, FIRST(first_name) FROM emp GROUP BY gender ORDER BY gender;

   gender   |   FIRST(first_name)
------------+--------------------
null        |   Berni
F           |   Alejandro
M           |   Amabile
```

```sql
SELECT FIRST(first_name, birth_date) FROM emp;

   FIRST(first_name, birth_date)
--------------------------------
Remzi
```

```sql
SELECT gender, FIRST(first_name, birth_date) FROM emp GROUP BY gender ORDER BY gender;

    gender    |   FIRST(first_name, birth_date)
--------------+--------------------------------
null          |   Lillian
F             |   Sumant
M             |   Remzi
```

`FIRST_VALUE` is a name alias and can be used instead of `FIRST`, e.g.:

```sql
SELECT gender, FIRST_VALUE(first_name, birth_date) FROM emp GROUP BY gender ORDER BY gender;

    gender    |   FIRST_VALUE(first_name, birth_date)
--------------+--------------------------------------
null          |   Lillian
F             |   Sumant
M             |   Remzi
```

```sql
SELECT gender, FIRST_VALUE(SUBSTRING(first_name, 2, 6), birth_date) AS "first" FROM emp GROUP BY gender ORDER BY gender;

    gender     |     first
---------------+---------------
null           |illian
F              |umant
M              |emzi
```

::::{note}
`FIRST` cannot be used in a HAVING clause.
::::


::::{note}
`FIRST` cannot be used with columns of type [`text`](/reference/elasticsearch/mapping-reference/text.md) unless the field is also [saved as a keyword](/reference/elasticsearch/mapping-reference/text.md#before-enabling-fielddata).
::::



## `LAST/LAST_VALUE` [sql-functions-aggs-last]

```sql
LAST(
    field_name               <1>
    [, ordering_field_name]) <2>
```

**Input**:

1. target field for the aggregation
2. optional field used for ordering


**Output**: same type as the input

**Description**: It’s the inverse of [`FIRST/FIRST_VALUE`](#sql-functions-aggs-first). Returns the last non-`null` value (if such exists) of the `field_name` input column sorted descending by the `ordering_field_name` column. If `ordering_field_name` is not provided, only the `field_name` column is used for the sorting. E.g.:

| a | b |
| --- | --- |
| 10 | 1 |
| 20 | 1 |
| 1 | 2 |
| 2 | 2 |
| 100 | null |
| 200 | null |
| null | null |

```sql
SELECT LAST(a) FROM t
```

will result in:

| **LAST(a)** |
| --- |
| 200 |

and

```sql
SELECT LAST(a, b) FROM t
```

will result in:

| **LAST(a, b)** |
| --- |
| 2 |

```sql
SELECT LAST(first_name) FROM emp;

   LAST(first_name)
-------------------
Zvonko
```

```sql
SELECT gender, LAST(first_name) FROM emp GROUP BY gender ORDER BY gender;

   gender   |   LAST(first_name)
------------+-------------------
null        |   Patricio
F           |   Xinglin
M           |   Zvonko
```

```sql
SELECT LAST(first_name, birth_date) FROM emp;

   LAST(first_name, birth_date)
-------------------------------
Hilari
```

```sql
SELECT gender, LAST(first_name, birth_date) FROM emp GROUP BY gender ORDER BY gender;

   gender  |   LAST(first_name, birth_date)
-----------+-------------------------------
null       |   Eberhardt
F          |   Valdiodio
M          |   Hilari
```

`LAST_VALUE` is a name alias and can be used instead of `LAST`, e.g.:

```sql
SELECT gender, LAST_VALUE(first_name, birth_date) FROM emp GROUP BY gender ORDER BY gender;

   gender  |   LAST_VALUE(first_name, birth_date)
-----------+-------------------------------------
null       |   Eberhardt
F          |   Valdiodio
M          |   Hilari
```

```sql
SELECT gender, LAST_VALUE(SUBSTRING(first_name, 3, 8), birth_date) AS "last" FROM emp GROUP BY gender ORDER BY gender;

    gender     |     last
---------------+---------------
null           |erhardt
F              |ldiodio
M              |lari
```

::::{note}
`LAST` cannot be used in `HAVING` clause.
::::


::::{note}
`LAST` cannot be used with columns of type [`text`](/reference/elasticsearch/mapping-reference/text.md) unless the field is also [`saved as a keyword`](/reference/elasticsearch/mapping-reference/text.md#before-enabling-fielddata).
::::



## `MAX` [sql-functions-aggs-max]

```sql
MAX(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: same type as the input

**Description**: Returns the maximum value across input values in the field `field_name`.

```sql
SELECT MAX(salary) AS max FROM emp;

      max
---------------
74999
```

```sql
SELECT MAX(ABS(salary / -12.0)) AS max FROM emp;

       max
-----------------
6249.916666666667
```

::::{note}
`MAX` on a field of type [`text`](/reference/elasticsearch/mapping-reference/text.md) or [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) is translated into [`LAST/LAST_VALUE`](#sql-functions-aggs-last) and therefore, it cannot be used in `HAVING` clause.
::::



## `MIN` [sql-functions-aggs-min]

```sql
MIN(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: same type as the input

**Description**: Returns the minimum value across input values in the field `field_name`.

```sql
SELECT MIN(salary) AS min FROM emp;

      min
---------------
25324
```

::::{note}
`MIN` on a field of type [`text`](/reference/elasticsearch/mapping-reference/text.md) or [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) is translated into [`FIRST/FIRST_VALUE`](#sql-functions-aggs-first) and therefore, it cannot be used in `HAVING` clause.
::::



## `SUM` [sql-functions-aggs-sum]

```sql
SUM(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `bigint` for integer input, `double` for floating points

**Description**: Returns the sum of input values in the field `field_name`.

```sql
SELECT SUM(salary) AS sum FROM emp;

      sum
---------------
4824855
```

```sql
SELECT ROUND(SUM(salary / 12.0), 1) AS sum FROM emp;

      sum
---------------
402071.3
```


## Statistics [sql-functions-aggs-statistics]


## `KURTOSIS` [sql-functions-aggs-kurtosis]

```sql
KURTOSIS(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `double` numeric value

**Description**:

[Quantify](https://en.wikipedia.org/wiki/Kurtosis) the shape of the distribution of input values in the field `field_name`.

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, KURTOSIS(salary) AS k FROM emp;

      min      |      max      |        k
---------------+---------------+------------------
25324          |74999          |2.0444718929142986
```

::::{note}
`KURTOSIS` cannot be used on top of scalar functions or operators but only directly on a field. So, for example, the following is not allowed and an error is returned:

```sql
 SELECT KURTOSIS(salary / 12.0), gender FROM emp GROUP BY gender
```

::::



## `MAD` [sql-functions-aggs-mad]

```sql
MAD(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `double` numeric value

**Description**:

[Measure](https://en.wikipedia.org/wiki/Median_absolute_deviation) the variability of the input values in the field `field_name`.

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, AVG(salary) AS avg, MAD(salary) AS mad FROM emp;

      min      |      max      |      avg      |      mad
---------------+---------------+---------------+---------------
25324          |74999          |48248.55       |10096.5
```

```sql
SELECT MIN(salary / 12.0) AS min, MAX(salary / 12.0) AS max, AVG(salary/ 12.0) AS avg, MAD(salary / 12.0) AS mad FROM emp;

       min        |       max       |      avg      |       mad
------------------+-----------------+---------------+-----------------
2110.3333333333335|6249.916666666667|4020.7125      |841.3750000000002
```


## `PERCENTILE` [sql-functions-aggs-percentile]

```sql
PERCENTILE(
    field_name,         <1>
    percentile[,        <2>
    method[,            <3>
    method_parameter]]) <4>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.
2. a numeric expression (must be a constant and not based on a field). If `null`, the function returns `null`.
3. optional string literal for the [percentile algorithm](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-approximation). Possible values: `tdigest` or `hdr`. Defaults to `tdigest`.
4. optional numeric literal that configures the [percentile algorithm](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-approximation). Configures `compression` for `tdigest` or `number_of_significant_value_digits` for `hdr`. The default is the same as that of the backing algorithm.


**Output**: `double` numeric value

**Description**:

Returns the nth [percentile](https://en.wikipedia.org/wiki/Percentile) (represented by `numeric_exp` parameter) of input values in the field `field_name`.

```sql
SELECT languages, PERCENTILE(salary, 95) AS "95th" FROM emp
       GROUP BY languages;

   languages   |      95th
---------------+-----------------
null           |74482.4
1              |71122.8
2              |70271.4
3              |71926.0
4              |69352.15
5              |56371.0
```

```sql
SELECT languages, PERCENTILE(salary / 12.0, 95) AS "95th" FROM emp
       GROUP BY languages;

   languages   |       95th
---------------+------------------
null           |6206.866666666667
1              |5926.9
2              |5855.949999999999
3              |5993.833333333333
4              |5779.345833333333
5              |4697.583333333333
```

```sql
SELECT
    languages,
    PERCENTILE(salary, 97.3, 'tdigest', 100.0) AS "97.3_TDigest",
    PERCENTILE(salary, 97.3, 'hdr', 3) AS "97.3_HDR"
FROM emp
GROUP BY languages;

   languages   | 97.3_TDigest    |   97.3_HDR
---------------+-----------------+---------------
null           |74720.036        |74992.0
1              |72316.132        |73712.0
2              |71792.436        |69936.0
3              |73326.23999999999|74992.0
4              |71753.281        |74608.0
5              |61176.16000000001|56368.0
```


## `PERCENTILE_RANK` [sql-functions-aggs-percentile-rank]

```sql
PERCENTILE_RANK(
    field_name,         <1>
    value[,             <2>
    method[,            <3>
    method_parameter]]) <4>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.
2. a numeric expression (must be a constant and not based on a field). If `null`, the function returns `null`.
3. optional string literal for the [percentile algorithm](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-approximation). Possible values: `tdigest` or `hdr`. Defaults to `tdigest`.
4. optional numeric literal that configures the [percentile algorithm](/reference/aggregations/search-aggregations-metrics-percentile-aggregation.md#search-aggregations-metrics-percentile-aggregation-approximation). Configures `compression` for `tdigest` or `number_of_significant_value_digits` for `hdr`. The default is the same as that of the backing algorithm.


**Output**: `double` numeric value

**Description**:

Returns the nth [percentile rank](https://en.wikipedia.org/wiki/Percentile_rank) (represented by `numeric_exp` parameter) of input values in the field `field_name`.

```sql
SELECT languages, PERCENTILE_RANK(salary, 65000) AS rank FROM emp GROUP BY languages;

   languages   |      rank
---------------+-----------------
null           |73.65766569962062
1              |73.7291625157734
2              |88.88005607010643
3              |79.43662623295829
4              |85.70446389643493
5              |96.79075152940749
```

```sql
SELECT languages, PERCENTILE_RANK(salary/12, 5000) AS rank FROM emp GROUP BY languages;

   languages   |       rank
---------------+------------------
null           |66.91240875912409
1              |66.70766707667076
2              |84.13266895048271
3              |61.052992625621684
4              |76.55646443990001
5              |94.00696864111498
```

```sql
SELECT
    languages,
    ROUND(PERCENTILE_RANK(salary, 65000, 'tdigest', 100.0), 2) AS "rank_TDigest",
    ROUND(PERCENTILE_RANK(salary, 65000, 'hdr', 3), 2) AS "rank_HDR"
FROM emp
GROUP BY languages;

   languages   | rank_TDigest  |   rank_HDR
---------------+---------------+---------------
null           |73.66          |80.0
1              |73.73          |73.33
2              |88.88          |89.47
3              |79.44          |76.47
4              |85.7           |83.33
5              |96.79          |95.24
```


## `SKEWNESS` [sql-functions-aggs-skewness]

```sql
SKEWNESS(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `double` numeric value

**Description**:

[Quantify](https://en.wikipedia.org/wiki/Skewness) the asymmetric distribution of input values in the field `field_name`.

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, SKEWNESS(salary) AS s FROM emp;

      min      |      max      |        s
---------------+---------------+------------------
25324          |74999          |0.2707722118423227
```

::::{note}
`SKEWNESS` cannot be used on top of scalar functions but only directly on a field. So, for example, the following is not allowed and an error is returned:

```sql
 SELECT SKEWNESS(ROUND(salary / 12.0, 2), gender FROM emp GROUP BY gender
```

::::



## `STDDEV_POP` [sql-functions-aggs-stddev-pop]

```sql
STDDEV_POP(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `double` numeric value

**Description**:

Returns the [population standard deviation](https://en.wikipedia.org/wiki/Standard_deviations) of input values in the field `field_name`.

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, STDDEV_POP(salary) AS stddev FROM emp;

      min      |      max      |      stddev
---------------+---------------+------------------
25324          |74999          |13765.125502787832
```

```sql
SELECT MIN(salary / 12.0) AS min, MAX(salary / 12.0) AS max, STDDEV_POP(salary / 12.0) AS stddev FROM emp;

       min        |       max       |     stddev
------------------+-----------------+-----------------
2110.3333333333335|6249.916666666667|1147.093791898986
```


## `STDDEV_SAMP` [sql-functions-aggs-stddev-samp]

```sql
STDDEV_SAMP(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `double` numeric value

**Description**:

Returns the [sample standard deviation](https://en.wikipedia.org/wiki/Standard_deviations) of input values in the field `field_name`.

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, STDDEV_SAMP(salary) AS stddev FROM emp;

      min      |      max      |      stddev
---------------+---------------+------------------
25324          |74999          |13834.471662090747
```

```sql
SELECT MIN(salary / 12.0) AS min, MAX(salary / 12.0) AS max, STDDEV_SAMP(salary / 12.0) AS stddev FROM emp;

       min        |       max       |     stddev
------------------+-----------------+-----------------
2110.3333333333335|6249.916666666667|1152.872638507562
```


## `SUM_OF_SQUARES` [sql-functions-aggs-sum-squares]

```sql
SUM_OF_SQUARES(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `double` numeric value

**Description**:

Returns the sum of squares of input values in the field `field_name`.

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, SUM_OF_SQUARES(salary) AS sumsq
       FROM emp;

      min      |      max      |     sumsq
---------------+---------------+----------------
25324          |74999          |2.51740125721E11
```

```sql
SELECT MIN(salary / 24.0) AS min, MAX(salary / 24.0) AS max, SUM_OF_SQUARES(salary / 24.0) AS sumsq FROM emp;

       min        |       max        |       sumsq
------------------+------------------+-------------------
1055.1666666666667|3124.9583333333335|4.370488293767361E8
```


## `VAR_POP` [sql-functions-aggs-var-pop]

```sql
VAR_POP(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `double` numeric value

**Description**:

Returns the [population variance](https://en.wikipedia.org/wiki/Variance) of input values in the field `field_name`.

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, VAR_POP(salary) AS varpop FROM emp;

      min      |      max      |     varpop
---------------+---------------+----------------
25324          |74999          |1.894786801075E8
```

```sql
SELECT MIN(salary / 24.0) AS min, MAX(salary / 24.0) AS max, VAR_POP(salary / 24.0) AS varpop FROM emp;

       min        |       max        |      varpop
------------------+------------------+------------------
1055.1666666666667|3124.9583333333335|328956.04185329855
```


## `VAR_SAMP` [sql-functions-aggs-var-samp]

```sql
VAR_SAMP(field_name) <1>
```

**Input**:

1. a numeric field. If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.


**Output**: `double` numeric value

**Description**:

Returns the [sample variance](https://en.wikipedia.org/wiki/Variance) of input values in the field `field_name`.

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, VAR_SAMP(salary) AS varsamp FROM emp;

      min      |      max      |     varsamp
---------------+---------------+----------------
25324          |74999          |1.913926061691E8
```

```sql
SELECT MIN(salary / 24.0) AS min, MAX(salary / 24.0) AS max, VAR_SAMP(salary / 24.0) AS varsamp FROM emp;

       min        |       max        |     varsamp
------------------+------------------+----------------
1055.1666666666667|3124.9583333333335|332278.830154847
```


