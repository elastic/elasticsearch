```yaml {applies_to}
stack: preview 9.6+
serverless: preview
```

The `HIGHLIGHT` [processing command](/reference/query-languages/esql/commands/processing-commands.md)
extracts and highlights matching text snippets from one or more fields based on a
full-text query. Matching terms are wrapped in highlight tags, bringing the
highlighting features of the Elasticsearch
[`_search` API](/reference/elasticsearch/rest-apis/highlighting.md) to {{esql}}.

## Syntax

```esql
HIGHLIGHT [prefix = "<prefix>"] [query] [ON field [, field, ...] | ON *] [WITH { "option": value [, ...] }]
```

## Parameters

`prefix`
:   (Optional) A quoted string literal used to name the output columns. Each
    highlighted field is written to `<prefix><field>`. Defaults to `highlight_` (for
    example, `HIGHLIGHT "fox" ON content` produces `highlight_content`). If a generated
    column name matches an existing column, the existing column is replaced. To
    overwrite the source column in place, specify an empty prefix (`prefix = ""`).
    Unlike the query and the `WITH` option values, `prefix` cannot be a query parameter.

`query`
:   (Optional) The query used to find matching terms to highlight. This can be a
    string literal (which uses
    [`query_string`](/reference/query-languages/query-dsl/query-dsl-query-string-query.md)
    syntax) or a full-text search function such as
    [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md),
    [`MATCH_PHRASE`](/reference/query-languages/esql/functions-operators/search-functions/match_phrase.md),
    [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md),
    [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md),
    or the [match operator `:`](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator).
    You can combine full-text functions using `AND`, `OR`, and `NOT`.

    If you don't specify a query, `HIGHLIGHT` automatically reuses full-text
    search conditions from earlier [`WHERE`](/reference/query-languages/esql/commands/where.md)
    commands in the query. Refer to [Reuse a query from WHERE](#esql-highlight-implicit-query).

    When you provide both a query and an `ON` clause, any field named in your
    query must also be listed in `ON`. For example,
    `HIGHLIGHT MATCH(title, "fox") ON body` is rejected because `title` is not in
    `ON`. When you let {{esql}} determine the query or fields automatically, it
    handles this check for you.

    Unqualified string literals and `QSTR` expressions are evaluated against
    whichever fields are being highlighted. Queries without positive search
    conditions (such as `NOT MATCH(...)`) have no terms to highlight and return
    `null`, unless you configure `no_match_size`.

`field`
:   (Optional) One or more comma-separated columns to highlight, or `*` to
    highlight every `text` and `keyword` column in the table. Fields must be `text`
    or `keyword` types (`semantic_text` fields are supported and treated as `text`).
    You can only use `*` by itself; wildcard patterns like `title*` and combining
    `*` with specific field names (such as `ON *, title`) are not supported.

    If you omit `ON`, `HIGHLIGHT` determines which columns to highlight based on
    your query:
    * For queries targeting a specific column (such as `MATCH` or `MATCH_PHRASE`),
      only that column is highlighted.
    * For queries that don't target a single column (such as string literals,
      `QSTR`, or `KQL`), `HIGHLIGHT` checks all `text` and `keyword` columns in
      the table.

    Refer to [Choose fields with ON](#esql-highlight-on-fields). If a field has no
    matching terms, its output is `null` unless you set `no_match_size`.

## WITH options

All option values passed in the `WITH` clause must be constants. Both literals and
[query parameters](/reference/query-languages/esql/esql-rest.md#esql-rest-params) that
resolve to a literal are accepted; column references are not.

`pre_tags`
:   (Optional) Opening tag inserted before each highlighted term. Accepts a string
    or a single-element array of strings. Defaults to `<em>`. Multiple rotating
    tags are not supported.

`post_tags`
:   (Optional) Closing tag inserted after each highlighted term. Accepts a string
    or a single-element array of strings. Defaults to `</em>`.

`encoder`
:   (Optional) Text encoding applied before adding highlight tags. Accepts
    `default` (no encoding) or `html` (HTML-escapes snippet text). Defaults to
    `default`. As in the `_search` API, this value is case-sensitive, so `html` is valid
    but `HTML` is rejected. `boundary_scanner` and `order` are case-insensitive.

`analyzer`
:   (Optional) The analyzer used to process query terms and field values. Defaults
    to `standard`. Only built-in and node-level plugin analyzers are supported;
    analyzers defined in index settings cannot be used. If individual full-text
    search functions specify their own `analyzer`, each function's analyzer applies
    to its targeted field, while this option acts as the default for any remaining fields.

`number_of_fragments`
:   (Optional) Maximum number of snippets (fragments) to return per field. Set to `0` to return the entire
    field value with matching terms highlighted without fragmenting. Must be `>= 0`.
    Defaults to `5`.

`fragment_size`
:   (Optional) Approximate character length of each snippet. Must be `>= 0`.
    Defaults to `100`.

`no_match_size`
:   (Optional) Approximate number of leading characters to return from the field
    when there are no matching terms. This is a minimum, not an exact limit: the
    returned text extends to the next boundary set by `boundary_scanner`, so the
    result can be longer than the requested size. Must be `>= 0`. Defaults to `0`
    (returns `null`).

`boundary_scanner`
:   (Optional) Boundary scanner used to split text into fragments. Accepts
    `sentence` or `word`, case-insensitively. Defaults to `sentence`.

`boundary_scanner_locale`
:   (Optional) Locale used by the boundary scanner, given as an
    [IETF BCP 47](https://www.rfc-editor.org/info/bcp47) language tag such as `en-US` or
    `ja-JP`. Use hyphens as separators. Defaults to the root locale. This is the same
    format accepted by the `_search` API's
    [`boundary_scanner_locale`](/reference/elasticsearch/rest-apis/highlighting-settings.md#boundary_scanner_locale).

`order`
:   (Optional) Sort order of returned fragments. Accepts `none` (preserves document
    order) or `score` (orders fragments by descending relevance score),
    case-insensitively. Defaults to `none`.

`max_analyzed_offset`
:   (Optional) Maximum number of characters to analyze per field value. Accepts a
    positive integer, or `-1` to leave the limit unset. Defaults to `-1`.
    `HIGHLIGHT` analyzes at most 1 million characters per field value regardless of
    this setting, and the index's `index.highlight.max_analyzed_offset` setting does
    not apply. Text beyond the effective offset is not highlighted.

## Description

Use `HIGHLIGHT` to find and display matching snippets in text fields, typically
after filtering rows with a full-text search condition in `WHERE`.

`HIGHLIGHT` processes each row, analyzes the specified text fields against the
query, and generates new keyword columns containing matching terms wrapped in
highlight tags. By default, output columns are named `highlight_<field>`. If a
field contains no matching terms, the result is `null` unless you specify
`no_match_size`.

Because `HIGHLIGHT` re-analyzes text values at query time, you can highlight
source fields from an index as well as computed columns created by earlier
commands like `EVAL`, `DISSECT`, `GROK`, `STATS`, `ENRICH`, or `LOOKUP JOIN`.

For multivalued fields, each value is highlighted independently:
* Phrase queries and fragment boundaries do not cross values.
* When a field produces multiple fragments, the output column contains a multivalued list of snippets.
* Multivalued `keyword` fields loaded from doc values are sorted and deduplicated before highlighting, which can result in a different snippet order compared to the `_search` API.

### Reuse a query from WHERE [esql-highlight-implicit-query]

Most search queries filter rows with a full-text condition in `WHERE`, then
highlight matching terms in those same fields. To avoid repeating your search
query, you can omit the query from `HIGHLIGHT`. When you do, `HIGHLIGHT`
automatically finds and reuses full-text search conditions from earlier
[`WHERE`](/reference/query-languages/esql/commands/where.md) commands.

This works with any positive full-text search function, including
[`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md),
[`MATCH_PHRASE`](/reference/query-languages/esql/functions-operators/search-functions/match_phrase.md),
[`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md),
[`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md),
and the match operator `:`.

You can include intermediate commands between `WHERE` and `HIGHLIGHT` as long as
each row still represents an individual document. For example, commands like
`KEEP`, `DROP`, `RENAME`, `EVAL`, `GROK`, `DISSECT`, `LIMIT`, `SORT`,
`MV_EXPAND`, and `INLINE STATS` pass through without issue.

However, commands that summarize, aggregate, or join rows—such as `STATS`,
`LOOKUP JOIN`, or `FORK`—change the document context. If you use any of these
commands between `WHERE` and `HIGHLIGHT`, you must provide the query explicitly
in `HIGHLIGHT`.

If your query contains multiple `WHERE` clauses, `HIGHLIGHT` combines all of
their full-text search conditions so that every searched field can produce
snippets, even though the `WHERE` clauses filter your rows together using `AND`.

When a reused search condition specifies an `analyzer` or `quote_analyzer`,
`HIGHLIGHT` retains those settings. Each highlighted column uses the analyzer
from the condition that searched it, defaulting to `standard` if no analyzer was
specified. If you also configure `WITH { "analyzer": ... }`, that analyzer serves
as the fallback for any columns not explicitly targeted by a search condition.

The following search conditions cannot be automatically reused:

* Negated conditions, such as `NOT MATCH(...)` (there are no positive matches to highlight)
* Conditions combined with non-text filters using `OR`, such as `MATCH(title, "fox") OR year > 2020`
* Conflicting analyzers targeting the same column, such as
  `MATCH(title, "fox", {"analyzer": "english"}) OR MATCH(title, "fox", {"analyzer": "whitespace"})`.
  A single column cannot use multiple analyzers during highlighting. However,
  using different analyzers across *different* columns (such as `english` on `title`
  and `whitespace` on `author`) is supported.

If your query relies solely on conditions that cannot be reused, specify the
query explicitly in `HIGHLIGHT`.

If you provide an explicit query in `HIGHLIGHT`, it takes precedence, and any
conditions from earlier `WHERE` commands are ignored for highlighting.

When checking the execution plan with `EXPLAIN`, if all reused conditions share
the same non-default analyzer, the plan shows that analyzer configured on
`HIGHLIGHT` (such as `WITH {"analyzer": "english"}`). When conditions specify
different analyzers for different columns, the execution plan lists the analyzers
per column instead (for example, `{title=english, author=standard}`).

### Choose fields with ON [esql-highlight-on-fields]

The `ON` clause specifies which columns to highlight. You can choose specific
columns, highlight all available text columns, or let {{esql}} determine the
columns automatically:

* **Highlight specific fields**: Use `ON field1, field2` to highlight only the
  specified columns.
* **Highlight all text and keyword fields**: Use `ON *` to highlight every
  `text` and `keyword` column in the current table, including multi-fields
  (such as `author.keyword`) and `semantic_text` fields (highlighted lexically).
  Metadata columns such as `_id` and `_index` are not included.
* **Let {{esql}} determine fields**: If you omit `ON`, `HIGHLIGHT` chooses the
  columns based on your query:
  * If the query targets a specific field (such as `MATCH(title, "fox")`),
    only that field is highlighted.
  * If the query does not name a specific field (such as a string literal,
    `QSTR`, or `KQL`), `HIGHLIGHT` checks all `text` and `keyword` columns in
    the table.

If a highlighted field does not match any query terms, its output is `null`
(or the leading text specified by `no_match_size`). If {{esql}} cannot find any
eligible `text` or `keyword` columns to highlight, you must provide an explicit
`ON` clause.

:::{tip}
Learn more about using [ES|QL for search use cases](docs-content://solutions/search/esql-for-search.md).
:::

## Limitations

* `HIGHLIGHT` re-analyzes text with the `standard` analyzer by default, rather than the analyzer configured in the index mapping. If your field uses a custom or language analyzer, specify it with the `analyzer` option in the `WITH` clause, or reuse it from a `WHERE` condition that specifies an `analyzer`.
* `HIGHLIGHT` only supports built-in and node-level plugin analyzers. Custom analyzers defined in index settings cannot be used with `HIGHLIGHT`, whether specified in `WITH` or reused from `WHERE`. If an index-level custom analyzer shares a name with a built-in analyzer (such as `english`), `HIGHLIGHT` uses the built-in definition.
* Highlight analysis runs at query time across both the search query and the row values, which can diverge from how fields were originally indexed in the mapping. For example, if a field is indexed using `standard` and queried using `MATCH(title, "tower", {"analyzer": "english"})`, `WHERE` will not match documents containing `"towers"` because `standard` indexed them without stemming. If a document matches through other criteria, however, `HIGHLIGHT` with `english` will highlight `"towers"` because it stems both the query term and snippet text.
* On `keyword` fields, `HIGHLIGHT` tokenizes text and breaks it into snippets like a text field, rather than treating the value as a single term.
* On `semantic_text` fields, `HIGHLIGHT` performs lexical matching against the underlying text. Semantic vector matches without literal keyword overlap are not highlighted.
* Fields are analyzed up to a maximum of 1 million characters. Text beyond this limit is not analyzed or highlighted.
* `HIGHLIGHT` cannot automatically reuse a `WHERE` query across commands that aggregate, summarize, or join rows, such as `STATS`, `LOOKUP JOIN`, or `FORK`. In those queries, specify the query directly on `HIGHLIGHT`.
* If you rename or drop a field between `WHERE` and `HIGHLIGHT`, the reused `WHERE` query still refers to the original field name. Provide an explicit query and `ON` clause that match the new column names in scope.

## Examples

The following examples show common ways to highlight search terms and customize snippet output.

### Highlight matches in a field

Wrap matching terms in the default `<em>` tags:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightSingleFieldForDocs.md
:::

### Highlight search results

Filter rows with a `WHERE` clause, then highlight matching terms in the output.
You can specify the search condition again in `HIGHLIGHT`:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightFromIndexAfterMatchForDocs.md
:::

### Automatically reuse a WHERE condition

To avoid repeating your search query, omit the query from `HIGHLIGHT`. When you
also omit `ON`, `HIGHLIGHT` automatically highlights matches in the field
searched by `WHERE` (in this case, creating `highlight_title`):

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightImplicitQueryBareFormForDocs.md
:::

To reuse the `WHERE` condition but choose which columns to highlight, provide
an explicit `ON` clause:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightImplicitQueryOnFieldForDocs.md
:::

### Highlight without an ON clause

When your query targets a specific field (such as `MATCH`), you can omit `ON`.
Only that field is highlighted, leaving other columns untouched:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightQueryWithoutOnForDocs.md
:::

When you use a query that doesn't target a specific field, such as a string
literal or `QSTR`, omitting `ON` highlights all `text` and `keyword` columns:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightLiteralWithoutOnForDocs.md
:::

### Highlight all text and keyword fields with ON *

Use `ON *` to highlight every `text` and `keyword` column in the table at once.
Columns that do not match the query evaluate to `null`:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightOnStarForDocs.md
:::

### Highlight phrases with MATCH_PHRASE

Use a full-text function like `MATCH_PHRASE` to highlight an exact phrase in a single tag pair:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightMatchPhraseFunctionForDocs.md
:::

### Highlight with query string syntax (QSTR)

Use [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md) to highlight terms using Lucene query syntax with boolean operators and field qualifiers:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightQueryStringFieldQualifiedAcrossFieldsForDocs.md
:::

### Highlight with Kibana Query Language (KQL)

Use [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md) to highlight terms using Kibana Query Language syntax, optionally combined with other full-text functions:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightKqlWildcardOrGroupedMatchForDocs.md
:::

### Highlight with a language analyzer

Use the `analyzer` option to apply language-specific stemming rules. In this example, the `english` analyzer stems `Rings` to `ring`:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightAnalyzerEnglishStemsMatchForDocs.md
:::

### Reuse analyzers from WHERE

When a `WHERE` condition specifies an analyzer, `HIGHLIGHT` automatically applies
that analyzer to highlight matches. In this example, the `english` analyzer in
`MATCH` stems `Rings` to match `ring`:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/implicitAnalyzerSynthesizesFromSingleLeafForDocs.md
:::

When multiple search conditions combined with `AND` or `OR` share the same
analyzer, that analyzer applies across all matched fields:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/implicitAnalyzerAndBothLeavesAgreeOnAnalyzerForDocs.md
:::

Fields without matching terms evaluate to `null`:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/implicitAnalyzerOrLeavesAgreeOnAnalyzerForDocs.md
:::

When using [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md)
with a `quote_analyzer`, `HIGHLIGHT` preserves both the primary search analyzer
and the quote analyzer for phrase parsing:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/implicitAnalyzerQuoteAnalyzerResolvesForDocs.md
:::

### Highlight multiple fields with different analyzers

When search conditions specify different analyzers for different fields,
`HIGHLIGHT` analyzes each field using its respective analyzer.

In this example, `title` uses the `english` analyzer specified in `MATCH`, while
`author` defaults to `standard`:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/implicitAnalyzerMixedEnglishAndDefaultForDocs.md
:::

You can also combine different non-default analyzers across fields. Here, `title`
uses `english` stemming, while `author` uses the `whitespace` analyzer to keep
hyphenated words intact:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/implicitAnalyzerMixedEnglishAndWhitespaceForDocs.md
:::

### Query-time and index-time analyzer differences

Setting an `analyzer` on a search function like `MATCH` only changes how the
query string is analyzed; the match itself runs against the terms created when
the document was indexed. Because `HIGHLIGHT` re-analyzes text at query time
using the query's analyzer, highlight behavior can differ from the initial filter.

In this example against the `books` index (where `title` was indexed using the
`standard` analyzer), searching for `"tower"` with the `english` analyzer does
not match `"towers"` at index time, so no rows are returned:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/implicitAnalyzerDivergenceWhereMissesRealIndexForDocs.md
:::

### Highlight multiple fields

Highlight multiple columns at once by listing them in `ON`:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightMultipleFieldsForDocs.md
:::

### Highlight an extracted or computed field

`HIGHLIGHT` re-analyzes field values at query time, so it works on columns created earlier in the pipeline:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightDissectExtractedFieldForDocs.md
:::

### HTML-encode text for safe display

Use `"encoder": "html"` to escape HTML tags and special characters in the text while keeping the highlight tags intact:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightHtmlEncoderForDocs.md
:::

### Return the full text without fragmenting

Set `"number_of_fragments": 0` to return the complete text value with matches highlighted rather than returning individual snippets:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightNumberOfFragmentsZeroForDocs.md
:::

### Customize highlight tags

Use `pre_tags` and `post_tags` to specify custom wrapping tags:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightCustomTagsForDocs.md
:::

### Customize output column names

Use `prefix` to change the column name prefix:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightCustomPrefixKeepsOriginalColumnForDocs.md
:::

### Overwrite the original column

Set an empty prefix (`prefix = ""`) to replace the source column with the highlighted output:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightEmptyPrefixForDocs.md
:::

### Return leading text when nothing matches

By default, non-matching fields evaluate to `null`. Set `no_match_size` to return text from the start of the field instead:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightNoMatchSizeForDocs.md
:::

### Order snippets by relevance score

Use `"order": "score"` to sort snippets by relevance score rather than document order:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightOrderByScoreForDocs.md
:::
