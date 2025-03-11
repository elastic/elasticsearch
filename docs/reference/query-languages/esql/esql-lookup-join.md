---
navigation_title: "Correlate data with LOOKUP JOIN"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-enrich-data.html
---

# LOOKUP JOIN [esql-lookup-join]

The {{esql}} [`LOOKUP join`](/reference/query-languages/esql/esql-commands.md#esql-lookup-join) processing command combines, at query-time, data from one or more source indexes with correlated information found in an input table. Teams often have data scattered across multiple indices – like logs, IPs, user IDs, hosts, employees etc. Without a direct way to enrich or correlate each event with reference data, root-cause analysis, security checks, and operational insights become time-consuming.

For example, you can use `LOOKUP JOIN` to:

* Pull in environment or ownership details for each host to correlate your metrics data.
* Quickly see if any source IPs match known malicious addresses.
* Tag logs with the owning team or escalation info for faster triage and incident response.

[`LOOKUP join`](/reference/query-languages/esql/esql-commands.md#esql-lookup-join) is similar to [`ENRICH`](/reference/query-languages/esql/esql-commands.md#esql-enrich) in the fact that they both help you join data together. You should use `LOOKUP JOIN` when:

* Enrichment data changes frequently
* You want to avoid index time processing
* Working with regular indices
* Need to preserve distinct matches
* Need to match on any field in a lookup index
* You use document or field level security

## How the `LOOKUP JOIN` command works [esql-how-lookup-join-works]

The `LOOKUP JOIN` command adds new columns to a table, with data from {{es}} indices.

:::{image} ../../../images/esql-lookup-join.png
:alt: esql lookup join
:::

$$$esql-source-index$$$

Source index
:   An index which stores data that the `LOOKUP` command can add to input tables. You can create and manage these indices just like a regular {{es}} index. You also can use the same source index in multiple lookup joins.

## Example

In the case where there are multiple matches on the index `LOOKUP JOIN` the output rows is the combination of each match from the left with each match on the right. 

Imagine you have the two tables:

**Left**

|Key|Entry|
| --- | --- |
|1|A|
|2|B|

**Right**

|Key|Language|TLD|
|---|---|---|
|1|English|CA|
|1|English|null|
|1|null|UK|
|1|English|US|
|2|German|AT,DE|
|2|German|CH|
|2|German|null|

Running the following query would provide the results shown below.

```esql
FROM employees
| WHERE Language IS NOT NULL // works and filter TLD UK
| LOOKUP JOIN Right ON Key
```

|Key|Entry|Language|TLD|
|---|---|---|---|
|1|A|English|CA|
|1|A|English|null|
|1|A|null|UK|
|1|A|English|US|
|2|A|German|AT,DE|
|2|B|German|CH|
|2|B|German|null|

::::{important}
`LOOKUP JOIN` does not guarantee the output to be in any particular order. If a certain order is required, users should use a [`SORT`](/reference/query-languages/esql/esql-commands.md#esql-sort) somewhere after the `LOOKUP JOIN`.

::::

## Prerequisites [esql-enrich-prereqs]

To use `LOOKUP JOIN`, you must have:

* Data types of join key and join field in the lookup index need to generally be the same. As long as the field data type is structurally capable of handling the data that you are passing through. For example `short,byte` is considered equal to `integer`. Also, text fields can be used on the left hand side if and only if there is an exact subfield whose name is suffixed with `.keyword`.

## Limitations

The following are the current limitations with `LOOKUP JOIN`

* `LOOKUP JOIN` will be sucessfull if both left and right type of the join are both `KEYWORD` types or if the left type is of `TEXT` and the right type is `KEYWORD`.
* Indices in [lookup](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting) mode are always single-sharded.
* Cross cluster search is unsupported. Both source and lookup indicies must be local.
* `LOOKUP JOIN` can only use a single match field, and can only use a single index. Wildcards, aliases, datemath, and datastreams are not supported.
* The name of the match field in `LOOKUP JOIN lu_idx ON match_field` must match an existing field in the query. This may require renames or evals to achieve.
* The query will circuit break if many documents from the lookup index have the same key. A large heap is needed to manage results of multiple megabytes per key.
  * This limit is per page of data which is about about 10,000 rows.
  * Matching many rows per incoming row will count against this limit.
  * This limit is approximately the same as for [`ENRICH`](/reference/query-languages/esql/esql-commands.md#esql-enrich).