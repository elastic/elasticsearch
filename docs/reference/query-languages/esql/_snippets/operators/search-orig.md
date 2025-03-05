## Search operators [esql-search-operators]

The only search operator is match (`:`).

**Syntax**

:::{image} ../../../../images/match.svg
:alt: Embedded
:class: text-center
:::


::::{warning}
Do not use on production environments. This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The match operator performs a [match query](/reference/query-languages/query-dsl-match-query.md) on the specified field. Returns true if the provided query matches the row.

The match operator is equivalent to the [match function](../esql-functions-operators.md#esql-match).

For using the function syntax, or adding [match query parameters](/reference/query-languages/query-dsl-match-query.md#match-field-params), you can use the [match function](../esql-functions-operators.md#esql-match).

**Supported types**

| field | query | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| boolean | keyword | boolean |
| date | date | boolean |
| date | keyword | boolean |
| date_nanos | date_nanos | boolean |
| date_nanos | keyword | boolean |
| double | double | boolean |
| double | integer | boolean |
| double | keyword | boolean |
| double | long | boolean |
| integer | double | boolean |
| integer | integer | boolean |
| integer | keyword | boolean |
| integer | long | boolean |
| ip | ip | boolean |
| ip | keyword | boolean |
| keyword | keyword | boolean |
| long | double | boolean |
| long | integer | boolean |
| long | keyword | boolean |
| long | long | boolean |
| text | keyword | boolean |
| unsigned_long | double | boolean |
| unsigned_long | integer | boolean |
| unsigned_long | keyword | boolean |
| unsigned_long | long | boolean |
| unsigned_long | unsigned_long | boolean |
| version | keyword | boolean |
| version | version | boolean |

**Example**

```esql
FROM books
| WHERE author:"Faulkner"
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
