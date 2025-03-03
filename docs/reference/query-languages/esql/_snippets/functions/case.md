## `CASE` [esql-case]

**Syntax**

:::{image} ../../../../../images/case.svg
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


