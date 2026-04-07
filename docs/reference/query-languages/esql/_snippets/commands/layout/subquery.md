```yaml {applies_to}
serverless: preview
stack: preview 9.4.0
```

A subquery is a complete ES|QL query wrapped in parentheses that can be used
in place of an index pattern in the [`FROM`](/reference/query-languages/esql/commands/from.md) command.
Each subquery is executed independently. The final output combines all these
results into a single list, including any duplicate rows.

## Syntax

```esql
FROM index_pattern [, (FROM index_pattern [METADATA fields] [| processing_commands])]* [METADATA fields] <1>
FROM (FROM index_pattern [METADATA fields] [| processing_commands]) [, (FROM index_pattern [METADATA fields] [| processing_commands])]* [METADATA fields] <2>
```

A subquery starts with a `FROM` source command followed by zero or more piped
processing commands, all enclosed in parentheses. Multiple subqueries and regular
index patterns can be combined in a single `FROM` clause, separated by commas.

1. When an index pattern is present, zero or more subqueries can follow.
2. Without an index pattern, one or more subqueries are required.

## Description

Subqueries enable you to combine results from multiple independently processed
data sources within a single query. Each subquery runs its own pipeline of
processing commands (such as `WHERE`, `EVAL`, `STATS`, or `SORT`) and the
results are combined together with results from other index patterns or subqueries
in the `FROM` clause.

Fields that exist in one source but not another are filled with `null` values.

The subquery pipeline can include commands such as the following:

Source commands:

- [`FROM`](/reference/query-languages/esql/commands/from.md)

Processing commands:

- [`CHANGE_POINT`](/reference/query-languages/esql/commands/change-point.md)
- [`COMPLETION`](/reference/query-languages/esql/commands/completion.md)
- [`DISSECT`](/reference/query-languages/esql/commands/dissect.md)
- [`DROP`](/reference/query-languages/esql/commands/drop.md)
- [`ENRICH`](/reference/query-languages/esql/commands/enrich.md)
- [`EVAL`](/reference/query-languages/esql/commands/eval.md)
- [`GROK`](/reference/query-languages/esql/commands/grok.md)
- [`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md)
- [`KEEP`](/reference/query-languages/esql/commands/keep.md)
- [`LIMIT`](/reference/query-languages/esql/commands/limit.md)
- [`LOOKUP JOIN`](/reference/query-languages/esql/commands/lookup-join.md)
- [`MV_EXPAND`](/reference/query-languages/esql/commands/mv_expand.md)
- [`RENAME`](/reference/query-languages/esql/commands/rename.md)
- [`RERANK`](/reference/query-languages/esql/commands/rerank.md)
- [`SAMPLE`](/reference/query-languages/esql/commands/sample.md)
- [`SORT`](/reference/query-languages/esql/commands/sort.md)
- [`STATS`](/reference/query-languages/esql/commands/stats-by.md)
- [`WHERE`](/reference/query-languages/esql/commands/where.md)

The [`METADATA` directive](/reference/query-languages/esql/esql-metadata-fields.md)
is also supported on either the subquery or the outer `FROM`.

## Examples

The following examples show how to use subqueries within the `FROM` command.

### Combine data from multiple indices

Use a subquery alongside a regular index pattern to combine results from
different indices:

```esql
FROM
    employees,
    (FROM sample_data)
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| SORT emp_no, client_ip
| KEEP emp_no, languages, client_ip
```

| emp_no:integer | languages:integer | client_ip:ip |
| --- | --- | --- |
| 10091 | 3 | null |
| 10092 | 1 | null |
| 10093 | 3 | null |
| null | null | 172.21.0.5 |
| null | null | 172.21.2.113 |
| null | null | 172.21.2.162 |
| null | null | 172.21.3.15 |
| null | null | 172.21.3.15 |
| null | null | 172.21.3.15 |
| null | null | 172.21.3.15 |

Rows from `employees` have `null` for `client_ip`, while rows from `sample_data`
have `null` for `emp_no` and `languages`, because each index has different fields.

### Use only subqueries (no main index pattern)

You can use one or more subqueries without specifying a regular index pattern:

```esql
FROM (FROM employees)
| WHERE emp_no >= 10091 AND emp_no < 10094
| SORT emp_no
| KEEP emp_no, languages
```

| emp_no:integer | languages:integer |
| --- | --- |
| 10091 | 3 |
| 10092 | 1 |
| 10093 | 3 |

The `FROM` clause contains only a subquery with no regular index pattern. The
subquery wraps the `employees` index, and the outer query filters, sorts, and
projects the results.

### Filter data inside a subquery

Apply a `WHERE` clause inside the subquery to pre-filter data before combining:

```esql
FROM
    employees,
    (FROM sample_data metadata _index
     | WHERE client_ip == "172.21.3.15")
    metadata _index
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| EVAL _index = MV_LAST(SPLIT(_index, ":"))
| SORT emp_no
| KEEP _index,  emp_no, languages, client_ip
```

| _index:keyword | emp_no:integer | languages:integer | client_ip:ip |
| --- | --- | --- | --- |
| employees | 10091 | 3 | null |
| employees | 10092 | 1 | null |
| employees | 10093 | 3 | null |
| sample_data | null | null | 172.21.3.15 |
| sample_data | null | null | 172.21.3.15 |
| sample_data | null | null | 172.21.3.15 |
| sample_data | null | null | 172.21.3.15 |

The `WHERE` inside the subquery filters `sample_data` to only rows where
`client_ip` is `172.21.3.15` before combining with `employees`. The `_index`
metadata field shows which index each row originated from.

### Aggregate data inside a subquery

Use `STATS` inside a subquery to aggregate data before combining with other sources:

```esql
FROM
    employees,
    (FROM sample_data metadata _index
     | STATS cnt = count(*) by _index, client_ip)
    metadata _index
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| EVAL _index = MV_LAST(SPLIT(_index, ":"))
| SORT _index, emp_no, client_ip
| KEEP _index, emp_no, languages, cnt, client_ip
```

| _index:keyword | emp_no:integer | languages:integer | cnt:long | client_ip:ip |
| --- | --- | --- | --- | --- |
| employees | 10091 | 3 | null | null |
| employees | 10092 | 1 | null | null |
| employees | 10093 | 3 | null | null |
| sample_data | null | null | 1 | 172.21.0.5 |
| sample_data | null | null | 1 | 172.21.2.113 |
| sample_data | null | null | 1 | 172.21.2.162 |
| sample_data | null | null | 4 | 172.21.3.15 |

The `STATS` inside the subquery aggregates `sample_data` by counting rows per
`client_ip` before combining with `employees`. The `cnt` column is `null` for
`employees` rows since that field only exists in the subquery output.

### Combine multiple subqueries

Multiple subqueries can be combined in a single `FROM` clause:

```esql
FROM
    (FROM sample_data metadata _index
     | STATS cnt = count(*) by _index, client_ip),
    (FROM sample_data_str metadata _index
     | STATS cnt = count(*) by _index, client_ip)
    metadata _index
| EVAL client_ip = client_ip::ip, _index = MV_LAST(SPLIT(_index, ":"))
| WHERE client_ip == "172.21.3.15" AND cnt >0
| SORT _index, client_ip
| KEEP _index, cnt, client_ip
```

| _index:keyword | cnt:long | client_ip:ip |
| --- | --- | --- |
| sample_data | 4 | 172.21.3.15 |
| sample_data_str | 4 | 172.21.3.15 |

Two subqueries aggregate `sample_data` and `sample_data_str` separately, each
counting rows by `client_ip`. The results are combined and then filtered to only
show rows where `client_ip` is `172.21.3.15`. The `_index` field confirms each
row's source.

### Use LOOKUP JOIN inside a subquery

Enrich subquery results with a lookup join before combining:

```esql
FROM
    employees,
    (FROM sample_data
     | EVAL client_ip = client_ip::keyword
     | LOOKUP JOIN clientips_lookup ON client_ip)
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| SORT emp_no, client_ip
| KEEP emp_no, languages, client_ip, env
```

| emp_no:integer | languages:integer | client_ip:keyword | env:keyword |
| --- | --- | --- | --- |
| 10091 | 3 | null | null |
| 10092 | 1 | null | null |
| 10093 | 3 | null | null |
| null | null | 172.21.0.5 | Development |
| null | null | 172.21.2.113 | QA |
| null | null | 172.21.2.162 | QA |
| null | null | 172.21.3.15 | Production |
| null | null | 172.21.3.15 | Production |
| null | null | 172.21.3.15 | Production |
| null | null | 172.21.3.15 | Production |

The `LOOKUP JOIN` inside the subquery joins each `sample_data` row with the
`env` field from `clientips_lookup` based on `client_ip`. The `env` column is
`null` for `employees` rows since the lookup only applies within the subquery.

### Sort and limit inside a subquery

Use `SORT` and `LIMIT` inside a subquery to return only top results:

```esql
FROM
    employees,
    (FROM sample_data
     | STATS cnt = count(*) by client_ip
     | SORT cnt DESC
     | LIMIT 1 )
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| SORT emp_no, client_ip
| KEEP emp_no, languages, cnt, client_ip
```

| emp_no:integer | languages:integer | cnt:long | client_ip:ip |
| --- | --- | --- | --- |
| 10091 | 3 | null | null |
| 10092 | 1 | null | null |
| 10093 | 3 | null | null |
| null | null | 4 | 172.21.3.15 |

The subquery aggregates `sample_data` by `client_ip`, sorts by count in
descending order, and limits to the top result. Only the `client_ip` with the
highest count (`172.21.3.15` with 4 occurrences) is included when combined with
`employees`.
