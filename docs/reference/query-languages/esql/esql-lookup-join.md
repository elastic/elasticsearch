---
navigation_title: "Correlate data with LOOKUP JOIN"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-enrich-data.html
---

# LOOKUP JOIN [esql-lookup-join]

The {{esql}} [`LOOKUP join`](/reference/query-languages/esql/esql-commands.md#esql-lookup-join) processing command combines, at query-time, data from one or more source indexes with field-value combinations found in an input table. Teams often have data scattered across multiple indices – like logs, IPs, user IDs, hosts, employees etc. Without a direct way to enrich or correlate each event with reference data, root-cause analysis, security checks, and operational insights become time-consuming.

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

## How the `LOOKUP JOIN` command works [esql-how-lookup-join-works]

The `LOOKUP JOIN` command adds new columns to a table, with data from {{es}} indices. It requires a few special components:

:::{image} ../../../images/esql-lookup-join.png
:alt: esql lookup join
:::

::::{tip}
`LOOKUP JOIN` does not guarantee the output to be in any particular order. If a certain order is required, users should use a [`SORT`](/reference/query-languages/esql/esql-commands.md#esql-sort) somewhere after the `LOOKUP JOIN`.

::::

$$$esql-source-index$$$

Source index
:   An index which stores enrich data that the `LOOKUP` command can add to input tables. You can create and manage these indices just like a regular {{es}} index. You can use multiple source indices in an enrich policy. You also can use the same source index in multiple enrich policies.


## Prerequisites [esql-enrich-prereqs]

To use `LOOKUP JOIN`, you must have:

* Data types of join key and join field in the lookup index need to generally be the same - up to widening of data types, where e.g. `short,byte` are considered equal to `integer`. Also, text fields can be used on the left hand side if and only if there is an exact subfield whose name is suffixed with `.keyword`.

## Limitations

The following is a list of current limitations with `LOOKUP JOIN`

* `LOOKUP JOIN` will be sucessfull if both left and right type of the join are both `KEYWORD` types or if the left type is of `TEXT` and the right type is `KEYWORD`.
* Indices in [lookup](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting) mode are always single-sharded.
* Cross cluster search is unsupported. Both source and lookup indicies must be local.
* `LOOKUP JOIN` can only use a single match field, and can only use a single index. Wildcards, aliases, and datastreams are not supported.
* The name of the match field in `LOOKUP JOIN lu_idx ON match_field` must match an existing field in the query. This may require renames or evals to achieve it.
* The query will circuit break if you fetch too much data in a single page. A large heap is needed to manage results of multiple megabytes.
  * This limit is per page of data which is about about 10,000 rows.
  * Matching many rows per incoming row will count against this limit.
  * This limit is approximately the same as for [`ENRICH`](/reference/query-languages/esql/esql-commands.md#esql-enrich).

