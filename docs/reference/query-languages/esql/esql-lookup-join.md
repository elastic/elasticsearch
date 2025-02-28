---
navigation_title: "LOOKUP JOIN"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-enrich-data.html
---

# LOOKUP JOIN [esql-lookup-join]

The {{esql}} [`LOOKUP join`](/reference/query-languages/esql/esql-commands.md#esql-lookup-join) processing command combines, at query-time, data from one or more source indexes with field-value combinations found in an input table.

For example, you can use `LOOKUP JOIN` to:

* Pull in environment or ownership details for each host to enrich your metrics data
* Quickly see if any source IPs match known malicious addresses.
* Tag logs with the owning team or escalation info for faster triage and incident response.


### How the `LOOKUP JOIN` command works [esql-how-lookup-join-works]

The `LOOKUP JOIN` command adds new columns to a table, with data from {{es}} indices. It requires a few special components:

:::{image} ../../../images/esql-lookup-join.png
:alt: esql lookup join
:::


$$$esql-source-index$$$

Source index
:   An index which stores enrich data that the `LOOKUP` command can add to input tables. You can create and manage these indices just like a regular {{es}} index. You can use multiple source indices in an enrich policy. You also can use the same source index in multiple enrich policies.


### Prerequisites [esql-enrich-prereqs]

To use `LOOKUP JOIN`, you must have:

* Data types of join key and join field in the lookup index need to generally be the same - up to widening of data types, where e.g. `short,byte` are considered equal to `integer`. Also, text fields can be used on the left hand side if and only if there is an exact subfield whose name is suffixed with `.keyword`.

