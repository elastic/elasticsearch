---
navigation_title: Conventions
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-concepts.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/_mapping_concepts_across_sql_and_elasticsearch.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Conventions and terminology [sql-concepts]

For clarity, it is important to establish the meaning behind certain words as, the same wording might convey different meanings to different readers depending on one’s familiarity with SQL versus {{es}}.

::::{note}
This documentation while trying to be complete, does assume the reader has *basic* understanding of {{es}} and/or SQL. If that is not the case, continue reading the documentation however take notes and pursue the topics that are unclear either through the main {{es}} documentation or through the plethora of SQL material available in the open (there are simply too many excellent resources here to enumerate).
::::


As a general rule, {{es}} SQL as the name indicates provides a SQL interface to {{es}}. As such, it follows the SQL terminology and conventions first, whenever possible. However, the backing engine itself is {{es}} for which Elasticsearch SQL was purposely created hence why features or concepts that are not available, or cannot be mapped correctly, in SQL appear in Elasticsearch SQL. Last but not least, {{es}} SQL tries to obey the [principle of least surprise](https://en.wikipedia.org/wiki/Principle_of_least_astonishment), though as all things in the world, everything is relative.

## Mapping concepts across SQL and Elasticsearch [_mapping_concepts_across_sql_and_es]

While SQL and {{es}} have different terms for the way the data is organized (and different semantics), essentially their purpose is the same.

So let’s start from the bottom; these roughly are:

| SQL | {{es}} | Description |
| --- | --- | --- |
| `column` | `field` | In both cases, at the lowest level, data is stored in *named* entries, of a variety of [data types](sql-data-types.md), containing *one* value. SQL calls such an entry a *column* while {{es}} a *field*. Notice that in {{es}} a field can contain *multiple* values of the same type (essentially a list) while in SQL, a *column* can contain *exactly* one value of said type. Elasticsearch SQL will do its best to preserve the SQL semantic and, depending on the query, reject those that return fields with more than one value. |
| `row` | `document` | `Column`s and `field`s do *not* exist by themselves; they are part of a `row` or a `document`. The two have slightly different semantics: a `row` tends to be *strict* (and have more enforcements) while a `document` tends to be a bit more flexible or loose (while still having a structure). |
| `table` | `index` | The target against which queries, whether in SQL or {{es}} get executed against. |
| `schema` | *implicit* | In RDBMS, `schema` is mainly a namespace of tables and typically used as a security boundary. {{es}} does not provide an equivalent concept for it. However when security is enabled, {{es}} automatically applies the security enforcement so that a role sees only the data it is allowed to (in SQL jargon, its *schema*). |
| `catalog` or `database` | `cluster` instance | In SQL, `catalog` or `database` are used interchangeably and represent a set of schemas that is, a number of tables.In {{es}} the set of indices available are grouped in a `cluster`. The semantics also differ a bit; a `database` is essentially yet another namespace (which can have some implications on the way data is stored) while an {{es}} `cluster` is a runtime instance, or rather a set of at least one {{es}} instance (typically running distributed).In practice this means that while in SQL one can potentially have multiple catalogs inside an instance, in {{es}} one is restricted to only *one*. |
| `cluster` | `cluster` (federated) | Traditionally in SQL, *cluster* refers to a single RDBMS instance which contains a number of `catalog`s or `database`s (see above). The same word can be reused inside {{es}} as well however its semantic clarified a bit.<br>While RDBMS tend to have only one running instance, on a single machine (*not* distributed), {{es}} goes the opposite way and by default, is distributed and multi-instance.<br>Further more, an {{es}} `cluster` can be connected to other `cluster`s in a *federated* fashion thus `cluster` means:<br>single cluster::Multiple {{es}} instances typically distributed across machines, running within the same namespace.multiple clusters::Multiple clusters, each with its own namespace, connected to each other in a federated setup (see [{{ccs-cap}}](docs-content://explore-analyze/cross-cluster-search.md)). |

As one can see while the mapping between the concepts are not exactly one to one and the semantics somewhat different, there are more things in common than differences. In fact, thanks to SQL declarative nature, many concepts can move across {{es}} transparently and the terminology of the two likely to be used interchangeably throughout the rest of the material.




