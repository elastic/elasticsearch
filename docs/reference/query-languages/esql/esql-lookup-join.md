---
navigation_title: "Correlate data with LOOKUP JOIN"
mapped_pages:
 - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-enrich-data.html
---

# LOOKUP JOIN [esql-lookup-join-reference]

The {{esql}} [`LOOKUP JOIN`](/reference/query-languages/esql/esql-commands.md#esql-lookup-join) processing command combines data from your {esql} query results table with matching records from a specified lookup index. It adds fields from the lookup index as new columns to your results table based on matching values in the join field.

Teams often have data scattered across multiple indices – like logs, IPs, user IDs, hosts, employees etc. Without a direct way to enrich or correlate each event with reference data, root-cause analysis, security checks, and operational insights become time-consuming.

For example, you can use `LOOKUP JOIN` to:

* Retrieve environment or ownership details for each host to correlate your metrics data.
* Quickly see if any source IPs match known malicious addresses.
* Tag logs with the owning team or escalation info for faster triage and incident response.

[`LOOKUP join`](/reference/query-languages/esql/esql-commands.md#esql-lookup-join) is similar to [`ENRICH`](/reference/query-languages/esql/esql-commands.md#esql-enrich) in the fact that they both help you join data together. You should use `LOOKUP JOIN` when:

* Your enrichment data changes frequently
* You want to avoid index-time processing
* You're working with regular indices
* You need to preserve distinct matches
* You need to match on any field in a lookup index
* You use document or field level security
* You want to restrict users to a specific lookup indices that they can you

## How the `LOOKUP JOIN` command works [esql-how-lookup-join-works]

The `LOOKUP JOIN` command adds new columns to a table, with data from {{es}} indices.

:::{image} ../../../images/esql-lookup-join.png
:alt: esql lookup join
:::

`<lookup_index>`
: The name of the lookup index. This must be a specific index name - wildcards, aliases, and remote cluster references are not supported.

`<field_name>`
: The field to join on. This field must exist in both your current query results and in the lookup index. If the field contains multi-valued entries, those entries will not match anything (the added fields will contain `null` for those rows).

## Example

`LOOKUP JOIN` has left-join behavior. If no rows match in the looked index, `LOOKUP JOIN` retains the incoming row and adds `null`s. If many rows in the lookedup index match, `LOOKUP JOIN` adds one row per match.

In this example, we have two sample tables:

**employees**

| birth_date|emp_no|first_name|gender|hire_date|language|
|---|---|---|---|---|---|
|1955-10-04T00:00:00Z|10091|Amabile    |M|1992-11-18T00:00:00Z|3|
|1964-10-18T00:00:00Z|10092|Valdiodio  |F|1989-09-22T00:00:00Z|1|
|1964-06-11T00:00:00Z|10093|Sailaja    |M|1996-11-05T00:00:00Z|3|
|1957-05-25T00:00:00Z|10094|Arumugam   |F|1987-04-18T00:00:00Z|5|
|1965-01-03T00:00:00Z|10095|Hilari     |M|1986-07-15T00:00:00Z|4|

**languages_non_unique_key**

|language_code|language_name|country|
|---|---|---|
|1|English|Canada|
|1|English|
|1||United Kingdom|
|1|English|United States of America|
|2|German|[Germany\|Austria]|
|2|German|Switzerland|
|2|German|
|4|Spanish|
|5||France|
|[6\|7]|Mv-Lang|Mv-Land|
|[7\|8]|Mv-Lang2|Mv-Land2|
||Null-Lang|Null-Land|
||Null-Lang2|Null-Land2|

Running the following query would provide the results shown below.

```esql
FROM employees
| EVAL language_code = emp_no % 10
| LOOKUP JOIN languages_lookup_non_unique_key ON language_code
| WHERE emp_no > 10090 AND emp_no < 10096
| SORT emp_no, country
| KEEP emp_no, language_code, language_name, country;
```

|emp_no|language_code|language_name|country|
|---|---|---|---|
|    10091      | 1                     | English               | Canada|
|    10091      | 1                     | null                  | United Kingdom|
|    10091      | 1                     | English               | United States of America|
|    10091      | 1                     | English               | null|
|    10092      | 2                     | German                | [Germany, Austria]|
|    10092      | 2                     | German                | Switzerland|
|    10092      | 2                     | German                | null|
|    10093      | 3                     | null                  | null|
|    10094      | 4                     | Spanish               | null|
|    10095      | 5                     | null                  | France|

::::{important}
`LOOKUP JOIN` does not guarantee the output to be in any particular order. If a certain order is required, users should use a [`SORT`](/reference/query-languages/esql/esql-commands.md#esql-sort) somewhere after the `LOOKUP JOIN`.

::::

## Prerequisites [esql-lookup-join-prereqs]

To use `LOOKUP JOIN`, the following requirements must be met:

* **Compatible data types**: The join key and join field in the lookup index must have compatible data types. This means:
  * The data types must either be identical or be internally represented as the same type in Elasticsearch's type system
  * Numeric types follow these compatibility rules:
    * `short` and `byte` are compatible with `integer` (all represented as `int`)
    * `float`, `half_float`, and `scaled_float` are compatible with `double` (all represented as `double`)
  * For text fields: You can use text fields on the left-hand side of the join only if they have a `.keyword` subfield

For a complete list of supported data types and their internal representations, see the [Supported Field Types documentation](/reference/query-languages/esql/limitations.md#_supported_types).

## Limitations

The following are the current limitations with `LOOKUP JOIN`

* `LOOKUP JOIN` will be successful if the join field in the lookup index is a `KEYWORD` type. If the main index's join field is `TEXT` type, it must have an exact `.keyword` subfield that can be matched with the lookup index's `KEYWORD` field.
* Indices in [lookup](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting) mode are always single-sharded.
* Cross cluster search is unsupported. Both source and lookup indices must be local.
* `LOOKUP JOIN` can only use a single match field and a single index. Wildcards, aliases, datemath, and datastreams are not supported.
* The name of the match field in `LOOKUP JOIN lu_idx ON match_field` must match an existing field in the query. This may require renames or evals to achieve.
* The query will circuit break if there are too many matching documents in the lookup index, or if the documents are too large. More precisely, `LOOKUP JOIN` works in batches of, normally, about 10,000 rows; a large amount of heap space is needed if the matching documents from the lookup index for a batch are multiple megabytes or larger. This is roughly the same as for `ENRICH`.
