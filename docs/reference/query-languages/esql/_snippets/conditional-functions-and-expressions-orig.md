## {{esql}} conditional functions and expressions [esql-conditional-functions-and-expressions]


Conditional functions return one of their arguments by evaluating in an if-else manner. {{esql}} supports these conditional functions:

:::{include} lists/conditional-functions-and-expressions.md
:::


## `CASE` [esql-case]

**Syntax**

:::{image} ../../../../images/case.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`condition`
:   A condition.

`trueValue`
:   The value that’s returned when the corresponding condition is the first to evaluate to `true`. The default value is returned when no condition matches.

`elseValue`
:   The value that’s returned when no condition evaluates to `true`.

**Description**

Accepts pairs of conditions and values. The function returns the value that belongs to the first condition that evaluates to `true`.  If the number of arguments is odd, the last argument is the default value which is returned when no condition matches. If the number of arguments is even, and no condition matches, the function returns `null`.

**Supported types**

| condition | trueValue | elseValue | result |
| --- | --- | --- | --- |
| boolean | boolean | boolean | boolean |
| boolean | boolean |  | boolean |
| boolean | cartesian_point | cartesian_point | cartesian_point |
| boolean | cartesian_point |  | cartesian_point |
| boolean | cartesian_shape | cartesian_shape | cartesian_shape |
| boolean | cartesian_shape |  | cartesian_shape |
| boolean | date | date | date |
| boolean | date |  | date |
| boolean | date_nanos | date_nanos | date_nanos |
| boolean | date_nanos |  | date_nanos |
| boolean | double | double | double |
| boolean | double |  | double |
| boolean | geo_point | geo_point | geo_point |
| boolean | geo_point |  | geo_point |
| boolean | geo_shape | geo_shape | geo_shape |
| boolean | geo_shape |  | geo_shape |
| boolean | integer | integer | integer |
| boolean | integer |  | integer |
| boolean | ip | ip | ip |
| boolean | ip |  | ip |
| boolean | keyword | keyword | keyword |
| boolean | keyword | text | keyword |
| boolean | keyword |  | keyword |
| boolean | long | long | long |
| boolean | long |  | long |
| boolean | text | keyword | keyword |
| boolean | text | text | keyword |
| boolean | text |  | keyword |
| boolean | unsigned_long | unsigned_long | unsigned_long |
| boolean | unsigned_long |  | unsigned_long |
| boolean | version | version | version |
| boolean | version |  | version |

**Examples**

Determine whether employees are monolingual, bilingual, or polyglot:

```esql
FROM employees
| EVAL type = CASE(
    languages <= 1, "monolingual",
    languages <= 2, "bilingual",
     "polyglot")
| KEEP emp_no, languages, type
```

| emp_no:integer | languages:integer | type:keyword |
| --- | --- | --- |
| 10001 | 2 | bilingual |
| 10002 | 5 | polyglot |
| 10003 | 4 | polyglot |
| 10004 | 5 | polyglot |
| 10005 | 1 | monolingual |

Calculate the total connection success rate based on log messages:

```esql
FROM sample_data
| EVAL successful = CASE(
    STARTS_WITH(message, "Connected to"), 1,
    message == "Connection error", 0
  )
| STATS success_rate = AVG(successful)
```

| success_rate:double |
| --- |
| 0.5 |

Calculate an hourly error rate as a percentage of the total number of log messages:

```esql
FROM sample_data
| EVAL error = CASE(message LIKE "*error*", 1, 0)
| EVAL hour = DATE_TRUNC(1 hour, @timestamp)
| STATS error_rate = AVG(error) by hour
| SORT hour
```

| error_rate:double | hour:date |
| --- | --- |
| 0.0 | 2023-10-23T12:00:00.000Z |
| 0.6 | 2023-10-23T13:00:00.000Z |


## `COALESCE` [esql-coalesce]

**Syntax**

:::{image} ../../../../images/coalesce.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`first`
:   Expression to evaluate.

`rest`
:   Other expression to evaluate.

**Description**

Returns the first of its arguments that is not null. If all arguments are null, it returns `null`.

**Supported types**

| first | rest | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| boolean |  | boolean |
| cartesian_point | cartesian_point | cartesian_point |
| cartesian_shape | cartesian_shape | cartesian_shape |
| date | date | date |
| date_nanos | date_nanos | date_nanos |
| geo_point | geo_point | geo_point |
| geo_shape | geo_shape | geo_shape |
| integer | integer | integer |
| integer |  | integer |
| ip | ip | ip |
| keyword | keyword | keyword |
| keyword |  | keyword |
| long | long | long |
| long |  | long |
| text | text | keyword |
| text |  | keyword |
| version | version | version |

**Example**

```esql
ROW a=null, b="b"
| EVAL COALESCE(a, b)
```

| a:null | b:keyword | COALESCE(a, b):keyword |
| --- | --- | --- |
| null | b | b |


## `GREATEST` [esql-greatest]

**Syntax**

:::{image} ../../../../images/greatest.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`first`
:   First of the columns to evaluate.

`rest`
:   The rest of the columns to evaluate.

**Description**

Returns the maximum value from multiple columns. This is similar to [`MV_MAX`](../esql-functions-operators.md#esql-mv_max) except it is intended to run on multiple columns at once.

::::{note}
When run on `keyword` or `text` fields, this returns the last string in alphabetical order. When run on `boolean` columns this will return `true` if any values are `true`.
::::


**Supported types**

| first | rest | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| boolean |  | boolean |
| date | date | date |
| date_nanos | date_nanos | date_nanos |
| double | double | double |
| integer | integer | integer |
| integer |  | integer |
| ip | ip | ip |
| keyword | keyword | keyword |
| keyword |  | keyword |
| long | long | long |
| long |  | long |
| text | text | keyword |
| text |  | keyword |
| version | version | version |

**Example**

```esql
ROW a = 10, b = 20
| EVAL g = GREATEST(a, b)
```

| a:integer | b:integer | g:integer |
| --- | --- | --- |
| 10 | 20 | 20 |


## `LEAST` [esql-least]

**Syntax**

:::{image} ../../../../images/least.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`first`
:   First of the columns to evaluate.

`rest`
:   The rest of the columns to evaluate.

**Description**

Returns the minimum value from multiple columns. This is similar to [`MV_MIN`](../esql-functions-operators.md#esql-mv_min) except it is intended to run on multiple columns at once.

**Supported types**

| first | rest | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| boolean |  | boolean |
| date | date | date |
| date_nanos | date_nanos | date_nanos |
| double | double | double |
| integer | integer | integer |
| integer |  | integer |
| ip | ip | ip |
| keyword | keyword | keyword |
| keyword |  | keyword |
| long | long | long |
| long |  | long |
| text | text | keyword |
| text |  | keyword |
| version | version | version |

**Example**

```esql
ROW a = 10, b = 20
| EVAL l = LEAST(a, b)
```

| a:integer | b:integer | l:integer |
| --- | --- | --- |
| 10 | 20 | 10 |
