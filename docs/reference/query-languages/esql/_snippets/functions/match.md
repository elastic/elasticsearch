## `MATCH` [esql-match]

::::{warning}
Do not use on production environments. This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


**Syntax**

:::{image} ../../../../../images/match.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Field that the query will target.

`query`
:   Value to find in the provided field.

`options`
:   (Optional) Match additional options as [function named parameters](/reference/query-languages/esql/esql-syntax.md#esql-function-named-params). See [match query](/reference/query-languages/query-dsl-match-query.md) for more information.

**Description**

Use `MATCH` to perform a [match query](/reference/query-languages/query-dsl-match-query.md) on the specified field. Using `MATCH` is equivalent to using the `match` query in the Elasticsearch Query DSL.  Match can be used on fields from the text family like [text](/reference/elasticsearch/mapping-reference/text.md) and [semantic_text](/reference/elasticsearch/mapping-reference/semantic-text.md), as well as other field types like keyword, boolean, dates, and numeric types.  Match can use [function named parameters](/reference/query-languages/esql/esql-syntax.md#esql-function-named-params) to specify additional options for the match query. All [match query parameters](/reference/query-languages/query-dsl-match-query.md#match-field-params) are supported.  For a simplified syntax, you can use the [match operator](../../esql-functions-operators.md#esql-search-operators) `:` operator instead of `MATCH`.  `MATCH` returns true if the provided query matches the row.

**Supported types**

| field | query | options | result |
| --- | --- | --- | --- |
| boolean | boolean | named parameters | boolean |
| boolean | keyword | named parameters | boolean |
| date | date | named parameters | boolean |
| date | keyword | named parameters | boolean |
| date_nanos | date_nanos | named parameters | boolean |
| date_nanos | keyword | named parameters | boolean |
| double | double | named parameters | boolean |
| double | integer | named parameters | boolean |
| double | keyword | named parameters | boolean |
| double | long | named parameters | boolean |
| integer | double | named parameters | boolean |
| integer | integer | named parameters | boolean |
| integer | keyword | named parameters | boolean |
| integer | long | named parameters | boolean |
| ip | ip | named parameters | boolean |
| ip | keyword | named parameters | boolean |
| keyword | keyword | named parameters | boolean |
| long | double | named parameters | boolean |
| long | integer | named parameters | boolean |
| long | keyword | named parameters | boolean |
| long | long | named parameters | boolean |
| text | keyword | named parameters | boolean |
| unsigned_long | double | named parameters | boolean |
| unsigned_long | integer | named parameters | boolean |
| unsigned_long | keyword | named parameters | boolean |
| unsigned_long | long | named parameters | boolean |
| unsigned_long | unsigned_long | named parameters | boolean |
| version | keyword | named parameters | boolean |
| version | version | named parameters | boolean |

**Supported function named parameters**

| name | types | description |
| --- | --- | --- |
| fuzziness | [keyword] | Maximum edit distance allowed for matching. |
| auto_generate_synonyms_phrase_query | [boolean] | If true, match phrase queries are automatically created for multi-term synonyms. |
| analyzer | [keyword] | Analyzer used to convert the text in the query value into token. |
| minimum_should_match | [integer] | Minimum number of clauses that must match for a document to be returned. |
| zero_terms_query | [keyword] | Number of beginning characters left unchanged for fuzzy matching. |
| boost | [float] | Floating point number used to decrease or increase the relevance scores of the query. |
| fuzzy_transpositions | [boolean] | If true, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba). |
| fuzzy_rewrite | [keyword] | Method used to rewrite the query. See the rewrite parameter for valid values and more information. |
| prefix_length | [integer] | Number of beginning characters left unchanged for fuzzy matching. |
| lenient | [boolean] | If false, format-based errors, such as providing a text query value for a numeric field, are returned. |
| operator | [keyword] | Boolean logic used to interpret text in the query value. |
| max_expansions | [integer] | Maximum number of terms to which the query will expand. |

**Examples**

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

```esql
FROM books
| WHERE MATCH(title, "Hobbit Back Again", {"operator": "AND"})
| KEEP title;
```

| title:text |
| --- |
| The Hobbit or There and Back Again |


