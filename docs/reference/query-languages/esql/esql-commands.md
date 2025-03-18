---
navigation_title: "Commands"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-commands.html
---

# {{esql}} commands [esql-commands]

## Source commands [esql-source-commands]

An {{esql}} source command produces a table, typically with data from {{es}}. An {{esql}} query must start with a source command.

:::{image} ../../../images/source-command.svg
:alt: A source command producing a table from {{es}}
:::

{{esql}} supports these source commands:

* [`FROM`](#esql-from)
* [`ROW`](#esql-row)
* [`SHOW`](#esql-show)


## Processing commands [esql-processing-commands]

{{esql}} processing commands change an input table by adding, removing, or changing rows and columns.

:::{image} ../../../images/processing-command.svg
:alt: A processing command changing an input table
:::

{{esql}} supports these processing commands:

* [`DISSECT`](#esql-dissect)
* [`DROP`](#esql-drop)
* [`ENRICH`](#esql-enrich)
* [`EVAL`](#esql-eval)
* [`GROK`](#esql-grok)
* [`KEEP`](#esql-keep)
* [`LIMIT`](#esql-limit)
* [preview] [`LOOKUP JOIN`](#esql-lookup-join)
* [preview] [`MV_EXPAND`](#esql-mv_expand)
* [`RENAME`](#esql-rename)
* [`SORT`](#esql-sort)
* [`STATS`](#esql-stats-by)
* [`WHERE`](#esql-where)


## `FROM` [esql-from]

The `FROM` source command returns a table with data from a data stream, index, or alias.

**Syntax**

```esql
FROM index_pattern [METADATA fields]
```

**Parameters**

`index_pattern`
:   A list of indices, data streams or aliases. Supports wildcards and date math.

`fields`
:   A comma-separated list of [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md) to retrieve.

**Description**

The `FROM` source command returns a table with data from a data stream, index, or alias. Each row in the resulting table represents a document. Each column corresponds to a field, and can be accessed by the name of that field.

::::{note}
By default, an {{esql}} query without an explicit [`LIMIT`](#esql-limit) uses an implicit limit of 1000. This applies to `FROM` too. A `FROM` command without `LIMIT`:

```esql
FROM employees
```

is executed as:

```esql
FROM employees
| LIMIT 1000
```

::::


**Examples**

```esql
FROM employees
```

You can use [date math](/reference/elasticsearch/rest-apis/api-conventions.md#api-date-math-index-names) to refer to indices, aliases and data streams. This can be useful for time series data, for example to access today’s index:

```esql
FROM <logs-{now/d}>
```

Use comma-separated lists or wildcards to [query multiple data streams, indices, or aliases](docs-content://explore-analyze/query-filter/languages/esql-multi-index.md):

```esql
FROM employees-00001,other-employees-*
```

Use the format `<remote_cluster_name>:<target>` to [query data streams and indices on remote clusters](docs-content://explore-analyze/query-filter/languages/esql-cross-clusters.md):

```esql
FROM cluster_one:employees-00001,cluster_two:other-employees-*
```

Use the optional `METADATA` directive to enable [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md):

```esql
FROM employees METADATA _id
```

Use enclosing double quotes (`"`) or three enclosing double quotes (`"""`) to escape index names that contain special characters:

```esql
FROM "this=that", """this[that"""
```


## `ROW` [esql-row]

The `ROW` source command produces a row with one or more columns with values that you specify. This can be useful for testing.

**Syntax**

```esql
ROW column1 = value1[, ..., columnN = valueN]
```

**Parameters**

`columnX`
:   The column name. In case of duplicate column names, only the rightmost duplicate creates a column.

`valueX`
:   The value for the column. Can be a literal, an expression, or a [function](/reference/query-languages/esql/esql-functions-operators.md#esql-functions).

**Examples**

```esql
ROW a = 1, b = "two", c = null
```

| a:integer | b:keyword | c:null |
| --- | --- | --- |
| 1 | "two" | null |

Use square brackets to create multi-value columns:

```esql
ROW a = [2, 1]
```

`ROW` supports the use of [functions](/reference/query-languages/esql/esql-functions-operators.md#esql-functions):

```esql
ROW a = ROUND(1.23, 0)
```


## `SHOW` [esql-show]

The `SHOW` source command returns information about the deployment and its capabilities.

**Syntax**

```esql
SHOW item
```

**Parameters**

`item`
:   Can only be `INFO`.

**Examples**

Use `SHOW INFO` to return the deployment’s version, build date and hash.

```esql
SHOW INFO
```

| version | date | hash |
| --- | --- | --- |
| 8.13.0 | 2024-02-23T10:04:18.123117961Z | 04ba8c8db2507501c88f215e475de7b0798cb3b3 |


## `DISSECT` [esql-dissect]

`DISSECT` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md).

**Syntax**

```esql
DISSECT input "pattern" [APPEND_SEPARATOR="<separator>"]
```

**Parameters**

`input`
:   The column that contains the string you want to structure.  If the column has multiple values, `DISSECT` will process each value.

`pattern`
:   A [dissect pattern](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md#esql-dissect-patterns). If a field name conflicts with an existing column, the existing column is dropped. If a field name is used more than once, only the rightmost duplicate creates a column.

`<separator>`
:   A string used as the separator between appended values, when using the [append modifier](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md#esql-append-modifier).

**Description**

`DISSECT` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md). `DISSECT` matches the string against a delimiter-based pattern, and extracts the specified keys as columns.

Refer to [Process data with `DISSECT`](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md#esql-process-data-with-dissect) for the syntax of dissect patterns.

**Examples**

The following example parses a string that contains a timestamp, some text, and an IP address:

```esql
ROW a = "2023-01-23T12:15:00.000Z - some text - 127.0.0.1"
| DISSECT a """%{date} - %{msg} - %{ip}"""
| KEEP date, msg, ip
```

| date:keyword | msg:keyword | ip:keyword |
| --- | --- | --- |
| 2023-01-23T12:15:00.000Z | some text | 127.0.0.1 |

By default, `DISSECT` outputs keyword string columns. To convert to another type, use [Type conversion functions](/reference/query-languages/esql/esql-functions-operators.md#esql-type-conversion-functions):

```esql
ROW a = "2023-01-23T12:15:00.000Z - some text - 127.0.0.1"
| DISSECT a """%{date} - %{msg} - %{ip}"""
| KEEP date, msg, ip
| EVAL date = TO_DATETIME(date)
```

| msg:keyword | ip:keyword | date:date |
| --- | --- | --- |
| some text | 127.0.0.1 | 2023-01-23T12:15:00.000Z |


## `DROP` [esql-drop]

The `DROP` processing command removes one or more columns.

**Syntax**

```esql
DROP columns
```

**Parameters**

`columns`
:   A comma-separated list of columns to remove. Supports wildcards.

**Examples**

```esql
FROM employees
| DROP height
```

Rather than specify each column by name, you can use wildcards to drop all columns with a name that matches a pattern:

```esql
FROM employees
| DROP height*
```


## `ENRICH` [esql-enrich]

`ENRICH` enables you to add data from existing indices as new columns using an enrich policy.

**Syntax**

```esql
ENRICH policy [ON match_field] [WITH [new_name1 = ]field1, [new_name2 = ]field2, ...]
```

**Parameters**

`policy`
:   The name of the enrich policy. You need to [create](/reference/query-languages/esql/esql-enrich-data.md#esql-set-up-enrich-policy) and [execute](/reference/query-languages/esql/esql-enrich-data.md#esql-execute-enrich-policy) the enrich policy first.

`mode`
:   The mode of the enrich command in cross cluster {{esql}}. See [enrich across clusters](docs-content://explore-analyze/query-filter/languages/esql-cross-clusters.md#ccq-enrich).

`match_field`
:   The match field. `ENRICH` uses its value to look for records in the enrich index. If not specified, the match will be performed on the column with the same name as the `match_field` defined in the [enrich policy](/reference/query-languages/esql/esql-enrich-data.md#esql-enrich-policy).

`fieldX`
:   The enrich fields from the enrich index that are added to the result as new columns. If a column with the same name as the enrich field already exists, the existing column will be replaced by the new column. If not specified, each of the enrich fields defined in the policy is added. A column with the same name as the enrich field will be dropped unless the enrich field is renamed.

`new_nameX`
:   Enables you to change the name of the column that’s added for each of the enrich fields. Defaults to the enrich field name. If a column has the same name as the new name, it will be discarded. If a name (new or original) occurs more than once, only the rightmost duplicate creates a new column.

**Description**

`ENRICH` enables you to add data from existing indices as new columns using an enrich policy. Refer to [Data enrichment](/reference/query-languages/esql/esql-enrich-data.md) for information about setting up a policy.

:::{image} ../../../images/esql-enrich.png
:alt: esql enrich
:::

::::{tip}
Before you can use `ENRICH`, you need to [create and execute an enrich policy](/reference/query-languages/esql/esql-enrich-data.md#esql-set-up-enrich-policy).
::::


**Examples**

The following example uses the `languages_policy` enrich policy to add a new column for each enrich field defined in the policy. The match is performed using the `match_field` defined in the [enrich policy](/reference/query-languages/esql/esql-enrich-data.md#esql-enrich-policy) and requires that the input table has a column with the same name (`language_code` in this example). `ENRICH` will look for records in the [enrich index](/reference/query-languages/esql/esql-enrich-data.md#esql-enrich-index) based on the match field value.

```esql
ROW language_code = "1"
| ENRICH languages_policy
```

| language_code:keyword | language_name:keyword |
| --- | --- |
| 1 | English |

To use a column with a different name than the `match_field` defined in the policy as the match field, use `ON <column-name>`:

```esql
ROW a = "1"
| ENRICH languages_policy ON a
```

| a:keyword | language_name:keyword |
| --- | --- |
| 1 | English |

By default, each of the enrich fields defined in the policy is added as a column. To explicitly select the enrich fields that are added, use `WITH <field1>, <field2>, ...`:

```esql
ROW a = "1"
| ENRICH languages_policy ON a WITH language_name
```

| a:keyword | language_name:keyword |
| --- | --- |
| 1 | English |

You can rename the columns that are added using `WITH new_name=<field1>`:

```esql
ROW a = "1"
| ENRICH languages_policy ON a WITH name = language_name
```

| a:keyword | name:keyword |
| --- | --- |
| 1 | English |

In case of name collisions, the newly created columns will override existing columns.


## `EVAL` [esql-eval]

The `EVAL` processing command enables you to append new columns with calculated values.

**Syntax**

```esql
EVAL [column1 =] value1[, ..., [columnN =] valueN]
```

**Parameters**

`columnX`
:   The column name. If a column with the same name already exists, the existing column is dropped. If a column name is used more than once, only the rightmost duplicate creates a column.

`valueX`
:   The value for the column. Can be a literal, an expression, or a [function](/reference/query-languages/esql/esql-functions-operators.md#esql-functions). Can use columns defined left of this one.

**Description**

The `EVAL` processing command enables you to append new columns with calculated values. `EVAL` supports various functions for calculating values. Refer to [Functions](/reference/query-languages/esql/esql-functions-operators.md#esql-functions) for more information.

**Examples**

```esql
FROM employees
| SORT emp_no
| KEEP first_name, last_name, height
| EVAL height_feet = height * 3.281, height_cm = height * 100
```

| first_name:keyword | last_name:keyword | height:double | height_feet:double | height_cm:double |
| --- | --- | --- | --- | --- |
| Georgi | Facello | 2.03 | 6.66043 | 202.99999999999997 |
| Bezalel | Simmel | 2.08 | 6.82448 | 208.0 |
| Parto | Bamford | 1.83 | 6.004230000000001 | 183.0 |

If the specified column already exists, the existing column will be dropped, and the new column will be appended to the table:

```esql
FROM employees
| SORT emp_no
| KEEP first_name, last_name, height
| EVAL height = height * 3.281
```

| first_name:keyword | last_name:keyword | height:double |
| --- | --- | --- |
| Georgi | Facello | 6.66043 |
| Bezalel | Simmel | 6.82448 |
| Parto | Bamford | 6.004230000000001 |

Specifying the output column name is optional. If not specified, the new column name is equal to the expression. The following query adds a column named `height*3.281`:

```esql
FROM employees
| SORT emp_no
| KEEP first_name, last_name, height
| EVAL height * 3.281
```

| first_name:keyword | last_name:keyword | height:double | height * 3.281:double |
| --- | --- | --- | --- |
| Georgi | Facello | 2.03 | 6.66043 |
| Bezalel | Simmel | 2.08 | 6.82448 |
| Parto | Bamford | 1.83 | 6.004230000000001 |

Because this name contains special characters, [it needs to be quoted](/reference/query-languages/esql/esql-syntax.md#esql-identifiers) with backticks (```) when using it in subsequent commands:

```esql
FROM employees
| EVAL height * 3.281
| STATS avg_height_feet = AVG(`height * 3.281`)
```

| avg_height_feet:double |
| --- |
| 5.801464200000001 |


## `GROK` [esql-grok]

`GROK` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md).

**Syntax**

```esql
GROK input "pattern"
```

**Parameters**

`input`
:   The column that contains the string you want to structure. If the column has multiple values, `GROK` will process each value.

`pattern`
:   A grok pattern. If a field name conflicts with an existing column, the existing column is discarded. If a field name is used more than once, a multi-valued column will be created with one value per each occurrence of the field name.

**Description**

`GROK` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md). `GROK` matches the string against patterns, based on regular expressions, and extracts the specified patterns as columns.

Refer to [Process data with `GROK`](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md#esql-process-data-with-grok) for the syntax of grok patterns.

**Examples**

The following example parses a string that contains a timestamp, an IP address, an email address, and a number:

```esql
ROW a = "2023-01-23T12:15:00.000Z 127.0.0.1 some.email@foo.com 42"
| GROK a """%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num}"""
| KEEP date, ip, email, num
```

| date:keyword | ip:keyword | email:keyword | num:keyword |
| --- | --- | --- | --- |
| 2023-01-23T12:15:00.000Z | 127.0.0.1 | `some.email@foo.com` | 42 |

By default, `GROK` outputs keyword string columns. `int` and `float` types can be converted by appending `:type` to the semantics in the pattern. For example `{NUMBER:num:int}`:

```esql
ROW a = "2023-01-23T12:15:00.000Z 127.0.0.1 some.email@foo.com 42"
| GROK a """%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}"""
| KEEP date, ip, email, num
```

| date:keyword | ip:keyword | email:keyword | num:integer |
| --- | --- | --- | --- |
| 2023-01-23T12:15:00.000Z | 127.0.0.1 | `some.email@foo.com` | 42 |

For other type conversions, use [Type conversion functions](/reference/query-languages/esql/esql-functions-operators.md#esql-type-conversion-functions):

```esql
ROW a = "2023-01-23T12:15:00.000Z 127.0.0.1 some.email@foo.com 42"
| GROK a """%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}"""
| KEEP date, ip, email, num
| EVAL date = TO_DATETIME(date)
```

| ip:keyword | email:keyword | num:integer | date:date |
| --- | --- | --- | --- |
| 127.0.0.1 | `some.email@foo.com` | 42 | 2023-01-23T12:15:00.000Z |

If a field name is used more than once, `GROK` creates a multi-valued column:

```esql
FROM addresses
| KEEP city.name, zip_code
| GROK zip_code """%{WORD:zip_parts} %{WORD:zip_parts}"""
```

| city.name:keyword | zip_code:keyword | zip_parts:keyword |
| --- | --- | --- |
| Amsterdam | 1016 ED | ["1016", "ED"] |
| San Francisco | CA 94108 | ["CA", "94108"] |
| Tokyo | 100-7014 | null |


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


## `LIMIT` [esql-limit]

The `LIMIT` processing command enables you to limit the number of rows that are returned.

**Syntax**

```esql
LIMIT max_number_of_rows
```

**Parameters**

`max_number_of_rows`
:   The maximum number of rows to return.

**Description**

The `LIMIT` processing command enables you to limit the number of rows that are returned. Queries do not return more than 10,000 rows, regardless of the `LIMIT` command’s value.

This limit only applies to the number of rows that are retrieved by the query. Queries and aggregations run on the full data set.

To overcome this limitation:

* Reduce the result set size by modifying the query to only return relevant data. Use [`WHERE`](#esql-where) to select a smaller subset of the data.
* Shift any post-query processing to the query itself. You can use the {{esql}} [`STATS`](#esql-stats-by) command to aggregate data in the query.

The default and maximum limits can be changed using these dynamic cluster settings:

* `esql.query.result_truncation_default_size`
* `esql.query.result_truncation_max_size`

**Example**

```esql
FROM employees
| SORT emp_no ASC
| LIMIT 5
```

## `LOOKUP JOIN` [esql-lookup-join]

::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::

`LOOKUP JOIN` enables you to add data from another index, AKA a 'lookup' index, to your {{esql}} query results, simplifying data enrichment and analysis workflows.

**Syntax**

```
FROM <source_index>
| LOOKUP JOIN <lookup_index> ON <field_name>
```

```esql
FROM firewall_logs
| LOOKUP JOIN threat_list ON source.IP
| WHERE threat_level IS NOT NULL
```

**Parameters**

`<lookup_index>`
: The name of the lookup index. This must be a specific index name - wildcards, aliases, and remote cluster references are not supported.

`<field_name>`
: The field to join on. This field must exist in both your current query results and in the lookup index. If the field contains multi-valued entries, those entries will not match anything (the added fields will contain `null` for those rows).

**Description**

The `LOOKUP JOIN` command adds new columns to your {esql} query results table by finding documents in a lookup index that share the same join field value as your result rows.

For each row in your results table that matches a document in the lookup index based on the join field, all fields from the matching document are added as new columns to that row.

If multiple documents in the lookup index match a single row in your results, the output will contain one row for each matching combination.

**Examples**

::::{tip}
In case of name collisions, the newly created columns will override existing columns.
::::

**IP Threat correlation**: This query would allow you to see if any source IPs match known malicious addresses.

```esql
FROM firewall_logs
| LOOKUP JOIN threat_list ON source.IP
```

**Host metadata correlation**: This query pulls in environment or ownership details for each host to correlate with your metrics data.

```esql
FROM system_metrics
| LOOKUP JOIN host_inventory ON host.name
| LOOKUP JOIN employees ON host.name
```

**Service ownership mapping**: This query would show logs with the owning team or escalation information for faster triage and incident response.

```esql
FROM app_logs
| LOOKUP JOIN service_owners ON service_id
```

`LOOKUP JOIN` is generally faster when there are fewer rows to join with. {{esql}} will try and perform any `WHERE` clause before the `LOOKUP JOIN` where possible.

The two following examples will have the same results. The two examples have the `WHERE` clause before and after the `LOOKUP JOIN`. It does not matter how you write your query, our optimizer will move the filter before the lookup when possible.

```esql
FROM Left
| WHERE Language IS NOT NULL
| LOOKUP JOIN Right ON Key
```

```esql
FROM Left
| LOOKUP JOIN Right ON Key
| WHERE Language IS NOT NULL 
```

## `MV_EXPAND` [esql-mv_expand]

::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The `MV_EXPAND` processing command expands multivalued columns into one row per value, duplicating other columns.

**Syntax**

```esql
MV_EXPAND column
```

**Parameters**

`column`
:   The multivalued column to expand.

**Example**

```esql
ROW a=[1,2,3], b="b", j=["a","b"]
| MV_EXPAND a
```

| a:integer | b:keyword | j:keyword |
| --- | --- | --- |
| 1 | b | ["a", "b"] |
| 2 | b | ["a", "b"] |
| 3 | b | ["a", "b"] |


## `RENAME` [esql-rename]

The `RENAME` processing command renames one or more columns.

**Syntax**

```esql
RENAME old_name1 AS new_name1[, ..., old_nameN AS new_nameN]
```

**Parameters**

`old_nameX`
:   The name of a column you want to rename.

`new_nameX`
:   The new name of the column. If it conflicts with an existing column name, the existing column is dropped. If multiple columns are renamed to the same name, all but the rightmost column with the same new name are dropped.

**Description**

The `RENAME` processing command renames one or more columns. If a column with the new name already exists, it will be replaced by the new column.

**Examples**

```esql
FROM employees
| KEEP first_name, last_name, still_hired
| RENAME  still_hired AS employed
```

Multiple columns can be renamed with a single `RENAME` command:

```esql
FROM employees
| KEEP first_name, last_name
| RENAME first_name AS fn, last_name AS ln
```


## `SORT` [esql-sort]

The `SORT` processing command sorts a table on one or more columns.

**Syntax**

```esql
SORT column1 [ASC/DESC][NULLS FIRST/NULLS LAST][, ..., columnN [ASC/DESC][NULLS FIRST/NULLS LAST]]
```

**Parameters**

`columnX`
:   The column to sort on.

**Description**

The `SORT` processing command sorts a table on one or more columns.

The default sort order is ascending. Use `ASC` or `DESC` to specify an explicit sort order.

Two rows with the same sort key are considered equal. You can provide additional sort expressions to act as tie breakers.

Sorting on multivalued columns uses the lowest value when sorting ascending and the highest value when sorting descending.

By default, `null` values are treated as being larger than any other value. With an ascending sort order, `null` values are sorted last, and with a descending sort order, `null` values are sorted first. You can change that by providing `NULLS FIRST` or `NULLS LAST`.

**Examples**

```esql
FROM employees
| KEEP first_name, last_name, height
| SORT height
```

Explicitly sorting in ascending order with `ASC`:

```esql
FROM employees
| KEEP first_name, last_name, height
| SORT height DESC
```

Providing additional sort expressions to act as tie breakers:

```esql
FROM employees
| KEEP first_name, last_name, height
| SORT height DESC, first_name ASC
```

Sorting `null` values first using `NULLS FIRST`:

```esql
FROM employees
| KEEP first_name, last_name, height
| SORT first_name ASC NULLS FIRST
```


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

The `STATS` processing command groups rows according to a common value and calculates one or more aggregated values over the grouped rows. For the calculation of each aggregated value, the rows in a group can be filtered with `WHERE`. If `BY` is omitted, the output table contains exactly one row with the aggregations applied over the entire dataset.

The following [aggregation functions](/reference/query-languages/esql/esql-functions-operators.md#esql-agg-functions) are supported:

* [`AVG`](/reference/query-languages/esql/esql-functions-operators.md#esql-avg)
* [`COUNT`](/reference/query-languages/esql/esql-functions-operators.md#esql-count)
* [`COUNT_DISTINCT`](/reference/query-languages/esql/esql-functions-operators.md#esql-count_distinct)
* [`MAX`](/reference/query-languages/esql/esql-functions-operators.md#esql-max)
* [`MEDIAN`](/reference/query-languages/esql/esql-functions-operators.md#esql-median)
* [`MEDIAN_ABSOLUTE_DEVIATION`](/reference/query-languages/esql/esql-functions-operators.md#esql-median_absolute_deviation)
* [`MIN`](/reference/query-languages/esql/esql-functions-operators.md#esql-min)
* [`PERCENTILE`](/reference/query-languages/esql/esql-functions-operators.md#esql-percentile)
* [preview] [`ST_CENTROID_AGG`](/reference/query-languages/esql/esql-functions-operators.md#esql-st_centroid_agg)
* [preview] [`ST_EXTENT_AGG`](/reference/query-languages/esql/esql-functions-operators.md#esql-st_extent_agg)
* [`STD_DEV`](/reference/query-languages/esql/esql-functions-operators.md#esql-std_dev)
* [`SUM`](/reference/query-languages/esql/esql-functions-operators.md#esql-sum)
* [`TOP`](/reference/query-languages/esql/esql-functions-operators.md#esql-top)
* [`VALUES`](/reference/query-languages/esql/esql-functions-operators.md#esql-values)
* [`WEIGHTED_AVG`](/reference/query-languages/esql/esql-functions-operators.md#esql-weighted_avg)

The following [grouping functions](/reference/query-languages/esql/esql-functions-operators.md#esql-group-functions) are supported:

* [`BUCKET`](/reference/query-languages/esql/esql-functions-operators.md#esql-bucket)
* [preview] [`CATEGORIZE`](/reference/query-languages/esql/esql-functions-operators.md#esql-categorize)

::::{note}
`STATS` without any groups is much much faster than adding a group.
::::


::::{note}
Grouping on a single expression is currently much more optimized than grouping on many expressions. In some tests we have seen grouping on a single `keyword` column to be five times faster than grouping on two `keyword` columns. Do not try to work around this by combining the two columns together with something like [`CONCAT`](/reference/query-languages/esql/esql-functions-operators.md#esql-concat) and then grouping - that is not going to be faster.
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


## `WHERE` [esql-where]

The `WHERE` processing command produces a table that contains all the rows from the input table for which the provided condition evaluates to `true`.

::::{tip}
In case of value exclusions, fields with `null` values will be excluded from search results. In this context a `null` means either there is an explicit `null` value in the document or there is no value at all. For example: `WHERE field != "value"` will be interpreted as `WHERE field != "value" AND field IS NOT NULL`.

::::


**Syntax**

```esql
WHERE expression
```

**Parameters**

`expression`
:   A boolean expression.

**Examples**

```esql
FROM employees
| KEEP first_name, last_name, still_hired
| WHERE still_hired == true
```

Which, if `still_hired` is a boolean field, can be simplified to:

```esql
FROM employees
| KEEP first_name, last_name, still_hired
| WHERE still_hired
```

Use date math to retrieve data from a specific time range. For example, to retrieve the last hour of logs:

```esql
FROM sample_data
| WHERE @timestamp > NOW() - 1 hour
```

`WHERE` supports various [functions](/reference/query-languages/esql/esql-functions-operators.md#esql-functions). For example the [`LENGTH`](/reference/query-languages/esql/esql-functions-operators.md#esql-length) function:

```esql
FROM employees
| KEEP first_name, last_name, height
| WHERE LENGTH(first_name) < 4
```

For a complete list of all functions, refer to [Functions overview](/reference/query-languages/esql/esql-functions-operators.md#esql-functions).

For NULL comparison, use the `IS NULL` and `IS NOT NULL` predicates:

```esql
FROM employees
| WHERE birth_date IS NULL
| KEEP first_name, last_name
| SORT first_name
| LIMIT 3
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Basil | Tramer |
| Florian | Syrotiuk |
| Lucien | Rosenbaum |

```esql
FROM employees
| WHERE is_rehired IS NOT NULL
| STATS COUNT(emp_no)
```

| COUNT(emp_no):long |
| --- |
| 84 |

For matching text, you can use [full text search functions](/reference/query-languages/esql/esql-functions-operators.md#esql-search-functions) like `MATCH`.

Use [`MATCH`](/reference/query-languages/esql/esql-functions-operators.md#esql-match) to perform a [match query](/reference/query-languages/query-dsl/query-dsl-match-query.md) on a specified field.

Match can be used on text fields, as well as other field types like boolean, dates, and numeric types.

```esql
FROM books
| WHERE MATCH(author, "Faulkner")
| KEEP book_no, author
| SORT book_no
| LIMIT 5
```

| book_no:keyword | author:text |
| --- | --- |
| 2378 | [Carol Faulkner, Holly Byers Ochoa, Lucretia Mott] |
| 2713 | William Faulkner |
| 2847 | Colleen Faulkner |
| 2883 | William Faulkner |
| 3293 | Danny Faulkner |

::::{tip}
You can also use the shorthand [match operator](/reference/query-languages/esql/esql-functions-operators.md#esql-search-operators) `:` instead of `MATCH`.

::::


Use `LIKE` to filter data based on string patterns using wildcards. `LIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

The following wildcard characters are supported:

* `*` matches zero or more characters.
* `?` matches one character.

**Supported types**

| str | pattern | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| text | keyword | boolean |

```esql
FROM employees
| WHERE first_name LIKE """?b*"""
| KEEP first_name, last_name
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Ebbe | Callaway |
| Eberhardt | Terkki |

Matching the exact characters `*` and `.` will require escaping. The escape character is backslash `\`. Since also backslash is a special character in string literals, it will require further escaping.

```esql
ROW message = "foo * bar"
| WHERE message LIKE "foo \\* bar"
```

To reduce the overhead of escaping, we suggest using triple quotes strings `"""`

```esql
ROW message = "foo * bar"
| WHERE message LIKE """foo \* bar"""
```

Use `RLIKE` to filter data based on string patterns using using [regular expressions](/reference/query-languages/query-dsl/regexp-syntax.md). `RLIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

**Supported types**

| str | pattern | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| text | keyword | boolean |

```esql
FROM employees
| WHERE first_name RLIKE """.leja.*"""
| KEEP first_name, last_name
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Alejandro | McAlpine |

Matching special characters (eg. `.`, `*`, `(`…​) will require escaping. The escape character is backslash `\`. Since also backslash is a special character in string literals, it will require further escaping.

```esql
ROW message = "foo ( bar"
| WHERE message RLIKE "foo \\( bar"
```

To reduce the overhead of escaping, we suggest using triple quotes strings `"""`

```esql
ROW message = "foo ( bar"
| WHERE message RLIKE """foo \( bar"""
```

The `IN` operator allows testing whether a field or expression equals an element in a list of literals, fields or expressions:

```esql
ROW a = 1, b = 4, c = 3
| WHERE c-a IN (3, b / 2, a)
```

| a:integer | b:integer | c:integer |
| --- | --- | --- |
| 1 | 4 | 3 |

For a complete list of all operators, refer to [Operators](/reference/query-languages/esql/esql-functions-operators.md#esql-operators).
