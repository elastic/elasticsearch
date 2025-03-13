## `KQL` [esql-kql]

::::{warning}
Do not use on production environments. This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


**Syntax**

:::{image} ../../../../../images/kql.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`query`
:   Query string in KQL query string format.

**Description**

Performs a KQL query. Returns true if the provided KQL query string matches the row.

**Supported types**

| query | result |
| --- | --- |
| keyword | boolean |
| text | boolean |

**Example**

```esql
FROM books
| WHERE KQL("author: Faulkner")
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


