## `KEEP` [esql-keep]

The `KEEP` processing command enables you to specify what columns are returned and the order in which they are returned.

**Syntax**

```esql
KEEP columns
```

**Parameters**

`columns`
:   A comma-separated list of columns to keep. Supports wildcards. See below for the behavior in case an existing column matches multiple given wildcards or column names.

**Description**

The `KEEP` processing command enables you to specify what columns are returned and the order in which they are returned.

Precedence rules are applied when a field name matches multiple expressions. Fields are added in the order they appear. If one field matches multiple expressions, the following precedence rules apply (from highest to lowest priority):

1. Complete field name (no wildcards)
2. Partial wildcard expressions (for example: `fieldNam*`)
3. Wildcard only (`*`)

If a field matches two expressions with the same precedence, the rightmost expression wins.

Refer to the examples for illustrations of these precedence rules.

**Examples**

The columns are returned in the specified order:

```esql
FROM employees
| KEEP emp_no, first_name, last_name, height
```

| emp_no:integer | first_name:keyword | last_name:keyword | height:double |
| --- | --- | --- | --- |
| 10001 | Georgi | Facello | 2.03 |
| 10002 | Bezalel | Simmel | 2.08 |
| 10003 | Parto | Bamford | 1.83 |
| 10004 | Chirstian | Koblick | 1.78 |
| 10005 | Kyoichi | Maliniak | 2.05 |

Rather than specify each column by name, you can use wildcards to return all columns with a name that matches a pattern:

```esql
FROM employees
| KEEP h*
```

| height:double | height.float:double | height.half_float:double | height.scaled_float:double | hire_date:date |
| --- | --- | --- | --- | --- |

The asterisk wildcard (`*`) by itself translates to all columns that do not match the other arguments.

This query will first return all columns with a name that starts with `h`, followed by all other columns:

```esql
FROM employees
| KEEP h*, *
```

| height:double | height.float:double | height.half_float:double | height.scaled_float:double | hire_date:date | avg_worked_seconds:long | birth_date:date | emp_no:integer | first_name:keyword | gender:keyword | is_rehired:boolean | job_positions:keyword | languages:integer | languages.byte:integer | languages.long:long | languages.short:integer | last_name:keyword | salary:integer | salary_change:double | salary_change.int:integer | salary_change.keyword:keyword | salary_change.long:long | still_hired:boolean |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

The following examples show how precedence rules work when a field name matches multiple expressions.

Complete field name has precedence over wildcard expressions:

```esql
FROM employees
| KEEP first_name, last_name, first_name*
```

| first_name:keyword | last_name:keyword |
| --- | --- |

Wildcard expressions have the same priority, but last one wins (despite being less specific):

```esql
FROM employees
| KEEP first_name*, last_name, first_na*
```

| last_name:keyword | first_name:keyword |
| --- | --- |

A simple wildcard expression `*` has the lowest precedence. Output order is determined by the other arguments:

```esql
FROM employees
| KEEP *, first_name
```

| avg_worked_seconds:long | birth_date:date | emp_no:integer | gender:keyword | height:double | height.float:double | height.half_float:double | height.scaled_float:double | hire_date:date | is_rehired:boolean | job_positions:keyword | languages:integer | languages.byte:integer | languages.long:long | languages.short:integer | last_name:keyword | salary:integer | salary_change:double | salary_change.int:integer | salary_change.keyword:keyword | salary_change.long:long | still_hired:boolean | first_name:keyword |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |


