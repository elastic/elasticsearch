---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-search.html
---

# Full-text search functions [sql-functions-search]

Search functions should be used when performing full-text search, namely when the `MATCH` or `QUERY` predicates are being used. Outside a, so-called, search context, these functions will return default values such as `0` or `NULL`.

Elasticsearch SQL optimizes all queries executed against {{es}} depending on the scoring needs. Using [`track_scores`](/reference/elasticsearch/rest-apis/sort-search-results.md#_track_scores) on the search request or [`_doc` sorting](/reference/elasticsearch/rest-apis/sort-search-results.md) that disables scores calculation, Elasticsearch SQL instructs {{es}} not to compute scores when these are not needed. For example, every time a `SCORE()` function is encountered in the SQL query, the scores are computed.

## `MATCH` [sql-functions-search-match]

```sql
MATCH(
    field_exp,   <1>
    constant_exp <2>
    [, options]) <3>
```

**Input**:

1. field(s) to match
2. matching text
3. additional parameters; optional


**Description**: A full-text search option, in the form of a predicate, available in Elasticsearch SQL that gives the user control over powerful [match](/reference/query-languages/query-dsl/query-dsl-match-query.md) and [multi_match](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md) {{es}} queries.

The first parameter is the field or fields to match against. In case it receives one value only, Elasticsearch SQL will use a `match` query to perform the search:

```sql
SELECT author, name FROM library WHERE MATCH(author, 'frank');

    author     |       name
---------------+-------------------
Frank Herbert  |Dune
Frank Herbert  |Dune Messiah
Frank Herbert  |Children of Dune
Frank Herbert  |God Emperor of Dune
```

However, it can also receive a list of fields and their corresponding optional `boost` value. In this case, Elasticsearch SQL will use a `multi_match` query to match the documents:

```sql
SELECT author, name, SCORE() FROM library WHERE MATCH('author^2,name^5', 'frank dune');

    author     |       name        |    SCORE()
---------------+-------------------+---------------
Frank Herbert  |Dune               |11.443176
Frank Herbert  |Dune Messiah       |9.446629
Frank Herbert  |Children of Dune   |8.043278
Frank Herbert  |God Emperor of Dune|7.0029488
```

::::{note}
The `multi_match` query in {{es}} has the option of [per-field boosting](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md) that gives preferential weight (in terms of scoring) to fields being searched in, using the `^` character. In the example above, the `name` field has a greater weight in the final score than the `author` field when searching for `frank dune` text in both of them.
::::


Both options above can be used in combination with the optional third parameter of the `MATCH()` predicate, where one can specify additional configuration parameters (separated by semicolon `;`) for either `match` or `multi_match` queries. For example:

```sql
SELECT author, name, SCORE() FROM library WHERE MATCH(name, 'to the star', 'operator=OR;fuzziness=AUTO:1,5;minimum_should_match=1')
ORDER BY SCORE() DESC LIMIT 2;

     author      |                name                |    SCORE()
-----------------+------------------------------------+---------------
Douglas Adams    |The Hitchhiker's Guide to the Galaxy|3.1756816
Peter F. Hamilton|Pandora's Star                      |3.0997515
```

::::{note}
The allowed optional parameters for a single-field `MATCH()` variant (for the `match` {{es}} query) are: `analyzer`, `auto_generate_synonyms_phrase_query`, `lenient`, `fuzziness`, `fuzzy_transpositions`, `fuzzy_rewrite`, `minimum_should_match`, `operator`, `max_expansions`, `prefix_length`.
::::


::::{note}
The allowed optional parameters for a multi-field `MATCH()` variant (for the `multi_match` {{es}} query) are: `analyzer`, `auto_generate_synonyms_phrase_query`, `lenient`, `fuzziness`, `fuzzy_transpositions`, `fuzzy_rewrite`, `minimum_should_match`, `operator`, `max_expansions`, `prefix_length`, `slop`, `tie_breaker`, `type`.
::::



## `QUERY` [sql-functions-search-query]

```sql
QUERY(
    constant_exp <1>
    [, options]) <2>
```

**Input**:

1. query text
2. additional parameters; optional


**Description**: Just like `MATCH`, `QUERY` is a full-text search predicate that gives the user control over the [query_string](/reference/query-languages/query-dsl/query-dsl-query-string-query.md) query in {{es}}.

The first parameter is basically the input that will be passed as is to the `query_string` query, which means that anything that `query_string` accepts in its `query` field can be used here as well:

```sql
SELECT author, name, SCORE() FROM library WHERE QUERY('name:dune');

    author     |       name        |    SCORE()
---------------+-------------------+---------------
Frank Herbert  |Dune               |2.2886353
Frank Herbert  |Dune Messiah       |1.8893257
Frank Herbert  |Children of Dune   |1.6086556
Frank Herbert  |God Emperor of Dune|1.4005898
```

A more advanced example, showing more of the features that `query_string` supports, of course possible with Elasticsearch SQL:

```sql
SELECT author, name, page_count, SCORE() FROM library WHERE QUERY('_exists_:"author" AND page_count:>200 AND (name:/star.*/ OR name:duna~)');

      author      |       name        |  page_count   |    SCORE()
------------------+-------------------+---------------+---------------
Frank Herbert     |Dune               |604            |3.7164764
Frank Herbert     |Dune Messiah       |331            |3.4169943
Frank Herbert     |Children of Dune   |408            |3.2064917
Frank Herbert     |God Emperor of Dune|454            |3.0504425
Peter F. Hamilton |Pandora's Star     |768            |3.0
Robert A. Heinlein|Starship Troopers  |335            |3.0
```

The query above uses the `_exists_` query to select documents that have values in the `author` field, a range query for `page_count` and regex and fuzziness queries for the `name` field.

If one needs to customize various configuration options that `query_string` exposes, this can be done using the second *optional* parameter. Multiple settings can be specified separated by a semicolon `;`:

```sql
SELECT author, name, SCORE() FROM library WHERE QUERY('dune god', 'default_operator=and;default_field=name');

    author     |       name        |    SCORE()
---------------+-------------------+---------------
Frank Herbert  |God Emperor of Dune|3.6984892
```

::::{note}
The allowed optional parameters for `QUERY()` are: `allow_leading_wildcard`, `analyze_wildcard`, `analyzer`, `auto_generate_synonyms_phrase_query`, `default_field`, `default_operator`, `enable_position_increments`, `escape`, `fuzziness`, `fuzzy_max_expansions`, `fuzzy_prefix_length`, `fuzzy_rewrite`, `fuzzy_transpositions`, `lenient`, `max_determinized_states`, `minimum_should_match`, `phrase_slop`, `rewrite`, `quote_analyzer`, `quote_field_suffix`, `tie_breaker`, `time_zone`, `type`.
::::



## `SCORE` [sql-functions-search-score]

```sql
SCORE()
```

**Input**: *none*

**Output**: `double` numeric value

**Description**: Returns the [relevance](https://www.elastic.co/guide/en/elasticsearch/guide/2.x/relevance-intro.html) of a given input to the executed query. The higher score, the more relevant the data.

::::{note}
When doing multiple text queries in the `WHERE` clause then, their scores will be combined using the same rules as {{es}}'s [bool query](/reference/query-languages/query-dsl/query-dsl-bool-query.md).
::::


Typically `SCORE` is used for ordering the results of a query based on their relevance:

```sql
SELECT SCORE(), * FROM library WHERE MATCH(name, 'dune') ORDER BY SCORE() DESC;

    SCORE()    |    author     |       name        |  page_count   |    release_date
---------------+---------------+-------------------+---------------+--------------------
2.2886353      |Frank Herbert  |Dune               |604            |1965-06-01T00:00:00Z
1.8893257      |Frank Herbert  |Dune Messiah       |331            |1969-10-15T00:00:00Z
1.6086556      |Frank Herbert  |Children of Dune   |408            |1976-04-21T00:00:00Z
1.4005898      |Frank Herbert  |God Emperor of Dune|454            |1981-05-28T00:00:00Z
```

However, it is perfectly fine to return the score without sorting by it:

```sql
SELECT SCORE() AS score, name, release_date FROM library WHERE QUERY('dune') ORDER BY YEAR(release_date) DESC;

     score     |       name        |    release_date
---------------+-------------------+--------------------
1.4005898      |God Emperor of Dune|1981-05-28T00:00:00Z
1.6086556      |Children of Dune   |1976-04-21T00:00:00Z
1.8893257      |Dune Messiah       |1969-10-15T00:00:00Z
2.2886353      |Dune               |1965-06-01T00:00:00Z
```


