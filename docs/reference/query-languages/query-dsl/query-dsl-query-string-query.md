---
navigation_title: "Query string"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
---

# Query string query [query-dsl-query-string-query]


::::{tip}
This page contains information about the `query_string` query type. For information about running a search query in {{es}}, see [*The search API*](docs-content://solutions/search/querying-for-search.md).
::::


Returns documents based on a provided query string, using a parser with a strict syntax.

This query uses a [syntax](#query-string-syntax) to parse and split the provided query string based on operators, such as `AND` or `NOT`. The query then [analyzes](docs-content://manage-data/data-store/text-analysis.md) each split text independently before returning matching documents.

You can use the `query_string` query to create a complex search that includes wildcard characters, searches across multiple fields, and more. While versatile, the query is strict and returns an error if the query string includes any invalid syntax.

::::{warning}
Because it returns an error for any invalid syntax, we don’t recommend using the `query_string` query for search boxes.

If you don’t need to support a query syntax, consider using the [`match`](/reference/query-languages/query-dsl/query-dsl-match-query.md) query. If you need the features of a query syntax, use the [`simple_query_string`](/reference/query-languages/query-dsl/query-dsl-simple-query-string-query.md) query, which is less strict.

::::


## Example request [query-string-query-ex-request]

When running the following search, the `query_string` query splits `(new york city) OR (big apple)` into two parts: `new york city` and `big apple`. The `content` field’s analyzer then independently converts each part into tokens before returning matching documents. Because the query syntax does not use whitespace as an operator, `new york city` is passed as-is to the analyzer.

```console
GET /_search
{
  "query": {
    "query_string": {
      "query": "(new york city) OR (big apple)",
      "default_field": "content"
    }
  }
}
```


## Top-level parameters for `query_string` [query-string-top-level-params]

`query`
:   (Required, string) Query string you wish to parse and use for search. See [Query string syntax](#query-string-syntax).

`default_field`
:   (Optional, string) Default field to search if no field is provided in the query string. Supports wildcards (`*`).

Defaults to the [`index.query.default_field`](/reference/elasticsearch/index-settings/index-modules.md#index-query-default-field) index setting, which has a default value of `*`. The `*` value extracts all fields that are eligible for term queries and filters the metadata fields. All extracted fields are then combined to build a query if no `prefix` is specified.

Searching across all eligible fields does not include [nested documents](/reference/elasticsearch/mapping-reference/nested.md). Use a [`nested` query](/reference/query-languages/query-dsl/query-dsl-nested-query.md) to search those documents.

::::{admonition}
:name: WARNING

For mappings with a large number of fields, searching across all eligible fields could be expensive.

There is a limit on the number of fields times terms that can be queried at once. It is defined by the `indices.query.bool.max_clause_count` [search setting](/reference/elasticsearch/configuration-reference/search-settings.md).

::::



`allow_leading_wildcard`
:   (Optional, Boolean) If `true`, the wildcard characters `*` and `?` are allowed as the first character of the query string. Defaults to `true`.

`analyze_wildcard`
:   (Optional, Boolean) If `true`, the query attempts to analyze wildcard terms in the query string. Defaults to `false`. Note that, in case of  `true`, only queries that end with a `*`
are fully analyzed. Queries that start with `*` or have it in the middle
are only [normalized](/reference/text-analysis/normalizers.md).

`analyzer`
:   (Optional, string) [Analyzer](docs-content://manage-data/data-store/text-analysis.md) used to convert text in the query string into tokens. Defaults to the [index-time analyzer](docs-content://manage-data/data-store/text-analysis/specify-an-analyzer.md#specify-index-time-analyzer) mapped for the `default_field`. If no analyzer is mapped, the index’s default analyzer is used.

`auto_generate_synonyms_phrase_query`
:   (Optional, Boolean) If `true`, [match phrase](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) queries are automatically created for multi-term synonyms. Defaults to `true`. See [Synonyms and the `query_string` query](#query-string-synonyms) for an example.

`boost`
:   (Optional, float) Floating point number used to decrease or increase the [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of the query. Defaults to `1.0`.

Boost values are relative to the default value of `1.0`. A boost value between `0` and `1.0` decreases the relevance score. A value greater than `1.0` increases the relevance score.


`default_operator`
:   (Optional, string) Default boolean logic used to interpret text in the query string if no operators are specified. Valid values are:

`OR` (Default)
:   For example, a query string of `capital of Hungary` is interpreted as `capital OR of OR Hungary`.

`AND`
:   For example, a query string of `capital of Hungary` is interpreted as `capital AND of AND Hungary`.


`enable_position_increments`
:   (Optional, Boolean) If `true`, enable position increments in queries constructed from a `query_string` search. Defaults to `true`.

`fields`
:   (Optional, array of strings) Array of fields to search. Supports wildcards (`*`).

    You can use this parameter query to search across multiple fields. See [Search multiple fields](#query-string-multi-field).


`fuzziness`
:   (Optional, string) Maximum edit distance allowed for fuzzy matching. For fuzzy syntax, see [Fuzziness](#query-string-fuzziness).

`fuzzy_max_expansions`
:   (Optional, integer) Maximum number of terms to which the query expands for fuzzy matching. Defaults to `50`.

`fuzzy_prefix_length`
:   (Optional, integer) Number of beginning characters left unchanged for fuzzy matching. Defaults to `0`.

`fuzzy_transpositions`
:   (Optional, Boolean) If `true`, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba). Defaults to `true`.

`lenient`
:   (Optional, Boolean) If `true`, format-based errors, such as providing a text value for a [numeric](/reference/elasticsearch/mapping-reference/number.md) field, are ignored. Defaults to `false`.

`max_determinized_states`
:   (Optional, integer) Maximum number of [automaton states](https://en.wikipedia.org/wiki/Deterministic_finite_automaton) required for the query. Default is `10000`.

{{es}} uses [Apache Lucene](https://lucene.apache.org/core/) internally to parse regular expressions. Lucene converts each regular expression to a finite automaton containing a number of determinized states.

You can use this parameter to prevent that conversion from unintentionally consuming too many resources. You may need to increase this limit to run complex regular expressions.


`minimum_should_match`
:   (Optional, string) Minimum number of clauses that must match for a document to be returned. See the [`minimum_should_match` parameter](/reference/query-languages/query-dsl/query-dsl-minimum-should-match.md) for valid values and more information. See [How `minimum_should_match` works](#query-string-min-should-match) for an example.

`quote_analyzer`
:   (Optional, string) [Analyzer](docs-content://manage-data/data-store/text-analysis.md) used to convert quoted text in the query string into tokens. Defaults to the [`search_quote_analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md#search-quote-analyzer) mapped for the `default_field`.

For quoted text, this parameter overrides the analyzer specified in the `analyzer` parameter.


`phrase_slop`
:   (Optional, integer) Maximum number of positions allowed between matching tokens for phrases. Defaults to `0`. If `0`, exact phrase matches are required. Transposed terms have a slop of `2`.

`quote_field_suffix`
:   (Optional, string) Suffix appended to quoted text in the query string.

You can use this suffix to use a different analysis method for exact matches. See [Mixing exact search with stemming](docs-content://solutions/search/full-text/search-relevance/mixing-exact-search-with-stemming.md).


`rewrite`
:   (Optional, string) Method used to rewrite the query. For valid values and more information, see the [`rewrite` parameter](/reference/query-languages/query-dsl/query-dsl-multi-term-rewrite.md).

`time_zone`
:   (Optional, string) [Coordinated Universal Time (UTC) offset](https://en.wikipedia.org/wiki/List_of_UTC_time_offsets) or [IANA time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) used to convert `date` values in the query string to UTC.

Valid values are ISO 8601 UTC offsets, such as `+01:00` or -`08:00`, and IANA time zone IDs, such as `America/Los_Angeles`.

::::{note}
The `time_zone` parameter does **not** affect the [date math](/reference/elasticsearch/rest-apis/common-options.md#date-math) value of `now`. `now` is always the current system time in UTC. However, the `time_zone` parameter does convert dates calculated using `now` and [date math rounding](/reference/elasticsearch/rest-apis/common-options.md#date-math). For example, the `time_zone` parameter will convert a value of `now/d`.

::::




## Notes [query-string-query-notes]

### Query string syntax [query-string-syntax]

The query string mini-language is used by the Query string and by the `q` query string parameter in the [`search` API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search).

The query string is parsed into a series of *terms* and *operators*. A term can be a single word — `quick` or `brown` — or a phrase, surrounded by double quotes — `"quick brown"` — which searches for all the words in the phrase, in the same order.

Operators allow you to customize the search — the available options are explained below.

#### Field names [_field_names]

You can specify fields to search in the query syntax:

* where the `status` field contains `active`

    ```
    status:active
    ```

* where the `title` field contains `quick` or `brown`

    ```
    title:(quick OR brown)
    ```

* where the `author` field contains the exact phrase `"john smith"`

    ```
    author:"John Smith"
    ```

* where the `first name` field contains `Alice` (note how we need to escape the space with a backslash)

    ```
    first\ name:Alice
    ```

* where any of the fields `book.title`, `book.content` or `book.date` contains `quick` or `brown` (note how we need to escape the `*` with a backslash):

    ```
    book.\*:(quick OR brown)
    ```

* where the field `title` has any non-null value:

    ```
    _exists_:title
    ```



#### Wildcards [query-string-wildcard]

Wildcard searches can be run on individual terms, using `?` to replace a single character, and `*` to replace zero or more characters:

```
qu?ck bro*
```
Be aware that wildcard queries can use an enormous amount of memory and perform very badly — just think how many terms need to be queried to match the query string `"a* b* c*"`.

::::{warning}
Pure wildcards `\*` are rewritten to [`exists`](/reference/query-languages/query-dsl/query-dsl-exists-query.md) queries for efficiency. As a consequence, the wildcard `"field:*"` would match documents with an empty value like the following:

```
{
  "field": ""
}
```

... and would **not** match if the field is missing or set with an explicit null value like the following:

```
{
  "field": null
}
```

::::


::::{warning}
Allowing a wildcard at the beginning of a word (eg `"*ing"`) is particularly heavy, because all terms in the index need to be examined, just in case they match. Leading wildcards can be disabled by setting `allow_leading_wildcard` to `false`.

::::


Only parts of the analysis chain that operate at the character level are applied. So for instance, if the analyzer performs both lowercasing and stemming, only the lowercasing will be applied: it would be wrong to perform stemming on a word that is missing some of its letters.

By setting `analyze_wildcard` to true, queries that end with a `*` will be analyzed and a boolean query will be built out of the different tokens, by ensuring exact matches on the first N-1 tokens, and prefix match on the last token.


#### Regular expressions [_regular_expressions]

Regular expression patterns can be embedded in the query string by wrapping them in forward-slashes (`"/"`):

```
name:/joh?n(ath[oa]n)/
```
The supported regular expression syntax is explained in [*Regular expression syntax*](/reference/query-languages/query-dsl/regexp-syntax.md).

::::{warning}
The `allow_leading_wildcard` parameter does not have any control over regular expressions. A query string such as the following would force Elasticsearch to visit every term in the index:

```
/.*n/
```
Use with caution!

::::



#### Fuzziness [query-string-fuzziness]

You can run [`fuzzy` queries](/reference/query-languages/query-dsl/query-dsl-fuzzy-query.md) using the `~` operator:

```
quikc~ brwn~ foks~
```
For these queries, the query string is [normalized](/reference/text-analysis/normalizers.md). If present, only certain filters from the analyzer are applied. For a list of applicable filters, see [*Normalizers*](/reference/text-analysis/normalizers.md).

The query uses the [Damerau-Levenshtein distance](https://en.wikipedia.org/wiki/Damerau-Levenshtein_distance) to find all terms with a maximum of two changes, where a change is the insertion, deletion or substitution of a single character, or transposition of two adjacent characters.

The default *edit distance* is `2`, but an edit distance of `1` should be sufficient to catch 80% of all human misspellings. It can be specified as:

```
quikc~1
```
::::{admonition} Avoid mixing fuzziness with wildcards
:class: warning

:name: avoid-widlcards-fuzzy-searches

Mixing [fuzzy](/reference/elasticsearch/rest-apis/common-options.md#fuzziness) and [wildcard](#query-string-wildcard) operators is *not* supported. When mixed, one of the operators is not applied. For example, you can search for `app~1` (fuzzy) or `app*` (wildcard), but searches for `app*~1` do not apply the fuzzy operator (`~1`).

::::



#### Proximity searches [_proximity_searches]

While a phrase query (eg `"john smith"`) expects all of the terms in exactly the same order, a proximity query allows the specified words to be further apart or in a different order. In the same way that fuzzy queries can specify a maximum edit distance for characters in a word, a proximity search allows us to specify a maximum edit distance of words in a phrase:

```
"fox quick"~5
```
The closer the text in a field is to the original order specified in the query string, the more relevant that document is considered to be. When compared to the above example query, the phrase `"quick fox"` would be considered more relevant than `"quick brown fox"`.


#### Ranges [_ranges]

Ranges can be specified for date, numeric or string fields. Inclusive ranges are specified with square brackets `[min TO max]` and exclusive ranges with curly brackets `{min TO max}`.

* All days in 2012:

    ```
    date:[2012-01-01 TO 2012-12-31]
    ```

* Numbers 1..5

    ```
    count:[1 TO 5]
    ```

* Tags between `alpha` and `omega`, excluding `alpha` and `omega`:

    ```
    tag:{alpha TO omega}
    ```

* Numbers from 10 upwards

    ```
    count:[10 TO *]
    ```

* Dates before 2012

    ```
    date:{* TO 2012-01-01}
    ```


Curly and square brackets can be combined:

* Numbers from 1 up to but not including 5

    ```
    count:[1 TO 5}
    ```


Ranges with one side unbounded can use the following syntax:

```
age:>10
age:>=10
age:<10
age:<=10
```
::::{note}
To combine an upper and lower bound with the simplified syntax, you would need to join two clauses with an `AND` operator:

```
age:(>=10 AND <20)
age:(+>=10 +<20)
```
::::


The parsing of ranges in query strings can be complex and error prone. It is much more reliable to use an explicit [`range` query](/reference/query-languages/query-dsl/query-dsl-range-query.md).


#### Boosting [boosting]

Use the *boost* operator `^` to make one term more relevant than another. For instance, if we want to find all documents about foxes, but we are especially interested in quick foxes:

```
quick^2 fox
```
The default `boost` value is 1, but can be any positive floating point number. Boosts between 0 and 1 reduce relevance.

Boosts can also be applied to phrases or to groups:

```
"john smith"^2   (foo bar)^4
```

#### Boolean operators [_boolean_operators]

By default, all terms are optional, as long as one term matches. A search for `foo bar baz` will find any document that contains one or more of `foo` or `bar` or `baz`. We have already discussed the `default_operator` above which allows you to force all terms to be required, but there are also *boolean operators* which can be used in the query string itself to provide more control.

The preferred operators are `+` (this term **must** be present) and `-` (this term **must not** be present). All other terms are optional. For example, this query:

```
quick brown +fox -news
```
states that:

* `fox` must be present
* `news` must not be present
* `quick` and `brown` are optional — their presence increases the relevance

The familiar boolean operators `AND`, `OR` and `NOT` (also written `&&`, `||` and `!`) are also supported but beware that they do not honor the usual precedence rules, so parentheses should be used whenever multiple operators are used together. For instance the previous query could be rewritten as:

`((quick AND fox) OR (brown AND fox) OR fox) AND NOT news`
:   This form now replicates the logic from the original query correctly, but the relevance scoring bears little resemblance to the original.

In contrast, the same query rewritten using the [`match` query](/reference/query-languages/query-dsl/query-dsl-match-query.md) would look like this:

```
{
    "bool": {
        "must":     { "match": "fox"         },
        "should":   { "match": "quick brown" },
        "must_not": { "match": "news"        }
    }
}
```

#### Grouping [_grouping]

Multiple terms or clauses can be grouped together with parentheses, to form sub-queries:

```
(quick OR brown) AND fox
```
Groups can be used to target a particular field, or to boost the result of a sub-query:

```
status:(active OR pending) title:(full text search)^2
```

#### Reserved characters [_reserved_characters]

If you need to use any of the characters which function as operators in your query itself (and not as operators), then you should escape them with a leading backslash. For instance, to search for `(1+1)=2`, you would need to write your query as `\(1\+1\)\=2`. When using JSON for the request body, two preceding backslashes (`\\`) are required; the backslash is a reserved escaping character in JSON strings.

```console
GET /my-index-000001/_search
{
  "query" : {
    "query_string" : {
      "query" : "kimchy\\!",
      "fields"  : ["user.id"]
    }
  }
}
```

The reserved characters are:  `+ - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /`

Failing to escape these special characters correctly could lead to a syntax error which prevents your query from running.

::::{note}
`<` and `>` can’t be escaped at all. The only way to prevent them from attempting to create a range query is to remove them from the query string entirely.
::::



#### Whitespaces and empty queries [_whitespaces_and_empty_queries]

Whitespace is not considered an operator.

If the query string is empty or only contains whitespaces the query will yield an empty result set.


#### Avoid using the `query_string` query for nested documents [query-string-nested]

`query_string` searches do not return [nested](/reference/elasticsearch/mapping-reference/nested.md) documents. To search nested documents, use the [`nested` query](/reference/query-languages/query-dsl/query-dsl-nested-query.md).


#### Search multiple fields [query-string-multi-field]

You can use the `fields` parameter to perform a `query_string` search across multiple fields.

The idea of running the `query_string` query against multiple fields is to expand each query term to an OR clause like this:

```
field1:query_term OR field2:query_term | ...
```

For example, the following query

```console
GET /_search
{
  "query": {
    "query_string": {
      "fields": [ "content", "name" ],
      "query": "this AND that"
    }
  }
}
```

matches the same words as

```console
GET /_search
{
  "query": {
    "query_string": {
      "query": "(content:this OR name:this) AND (content:that OR name:that)"
    }
  }
}
```

Since several queries are generated from the individual search terms, combining them is automatically done using a `dis_max` query with a `tie_breaker`. For example (the `name` is boosted by 5 using `^5` notation):

```console
GET /_search
{
  "query": {
    "query_string" : {
      "fields" : ["content", "name^5"],
      "query" : "this AND that OR thus",
      "tie_breaker" : 0
    }
  }
}
```

Simple wildcard can also be used to search "within" specific inner elements of the document. For example, if we have a `city` object with several fields (or inner object with fields) in it, we can automatically search on all "city" fields:

```console
GET /_search
{
  "query": {
    "query_string" : {
      "fields" : ["city.*"],
      "query" : "this AND that OR thus"
    }
  }
}
```

Another option is to provide the wildcard fields search in the query string itself (properly escaping the `*` sign), for example: `city.\*:something`:

```console
GET /_search
{
  "query": {
    "query_string" : {
      "query" : "city.\\*:(this AND that OR thus)"
    }
  }
}
```

::::{note}
Since `\` (backslash) is a special character in json strings, it needs to be escaped, hence the two backslashes in the above `query_string`.
::::


The fields parameter can also include pattern based field names, allowing to automatically expand to the relevant fields (dynamically introduced fields included). For example:

```console
GET /_search
{
  "query": {
    "query_string" : {
      "fields" : ["content", "name.*^5"],
      "query" : "this AND that OR thus"
    }
  }
}
```


#### Additional parameters for multiple field searches [query-string-multi-field-parms]

When running the `query_string` query against multiple fields, the following additional parameters are supported.

`type`
:   (Optional, string) Determines how the query matches and scores documents. Valid values are:

`best_fields` (Default)
:   Finds documents which match any field and uses the highest [`_score`](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) from any matching field. See [`best_fields`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md#type-best-fields).

`bool_prefix`
:   Creates a `match_bool_prefix` query on each field and combines the `_score` from each field. See [`bool_prefix`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md#type-bool-prefix).

`cross_fields`
:   Treats fields with the same `analyzer` as though they were one big field. Looks for each word in **any** field. See [`cross_fields`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md#type-cross-fields).

`most_fields`
:   Finds documents which match any field and combines the `_score` from each field. See [`most_fields`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md#type-most-fields).

`phrase`
:   Runs a `match_phrase` query on each field and uses the `_score` from the best field. See [`phrase` and `phrase_prefix`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md#type-phrase).

`phrase_prefix`
:   Runs a `match_phrase_prefix` query on each field and uses the `_score` from the best field. See [`phrase` and `phrase_prefix`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md#type-phrase).

NOTE: Additional top-level `multi_match` parameters may be available based on the [`type`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md#multi-match-types) value.




### Synonyms and the `query_string` query [query-string-synonyms]

The `query_string` query supports multi-terms synonym expansion with the [synonym_graph](/reference/text-analysis/analysis-synonym-graph-tokenfilter.md) token filter. When this filter is used, the parser creates a phrase query for each multi-terms synonyms. For example, the following synonym: `ny, new york` would produce:

`(ny OR ("new york"))`

It is also possible to match multi terms synonyms with conjunctions instead:

```console
GET /_search
{
   "query": {
       "query_string" : {
           "default_field": "title",
           "query" : "ny city",
           "auto_generate_synonyms_phrase_query" : false
       }
   }
}
```

The example above creates a boolean query:

`(ny OR (new AND york)) city`

that matches documents with the term `ny` or the conjunction `new AND york`. By default the parameter `auto_generate_synonyms_phrase_query` is set to `true`.


### How `minimum_should_match` works [query-string-min-should-match]

The `query_string` splits the query around each operator to create a boolean query for the entire input. You can use `minimum_should_match` to control how many "should" clauses in the resulting query should match.

```console
GET /_search
{
  "query": {
    "query_string": {
      "fields": [
        "title"
      ],
      "query": "this that thus",
      "minimum_should_match": 2
    }
  }
}
```

The example above creates a boolean query:

`(title:this title:that title:thus)~2`

that matches documents with at least two of the terms `this`, `that` or `thus` in the single field `title`.


### How `minimum_should_match` works for multiple fields [query-string-min-should-match-multi]

```console
GET /_search
{
  "query": {
    "query_string": {
      "fields": [
        "title",
        "content"
      ],
      "query": "this that thus",
      "minimum_should_match": 2
    }
  }
}
```

The example above creates a boolean query:

`((content:this content:that content:thus) | (title:this title:that title:thus))`

that matches documents with the disjunction max over the fields `title` and `content`. Here the `minimum_should_match` parameter can’t be applied.

```console
GET /_search
{
  "query": {
    "query_string": {
      "fields": [
        "title",
        "content"
      ],
      "query": "this OR that OR thus",
      "minimum_should_match": 2
    }
  }
}
```

Adding explicit operators forces each term to be considered as a separate clause.

The example above creates a boolean query:

`((content:this | title:this) (content:that | title:that) (content:thus | title:thus))~2`

that matches documents with at least two of the three "should" clauses, each of them made of the disjunction max over the fields for each term.


### How `minimum_should_match` works for cross-field searches [query-string-min-should-match-cross]

A `cross_fields` value in the `type` field indicates fields with the same analyzer are grouped together when the input is analyzed.

```console
GET /_search
{
  "query": {
    "query_string": {
      "fields": [
        "title",
        "content"
      ],
      "query": "this OR that OR thus",
      "type": "cross_fields",
      "minimum_should_match": 2
    }
  }
}
```

The example above creates a boolean query:

`(blended(terms:[field2:this, field1:this]) blended(terms:[field2:that, field1:that]) blended(terms:[field2:thus, field1:thus]))~2`

that matches documents with at least two of the three per-term blended queries.


### Allow expensive queries [_allow_expensive_queries]

Query string query can be internally be transformed to a [`prefix query`](/reference/query-languages/query-dsl/query-dsl-prefix-query.md) which means that if the prefix queries are disabled as explained [here](/reference/query-languages/query-dsl/query-dsl-prefix-query.md#prefix-query-allow-expensive-queries) the query will not be executed and an exception will be thrown.
