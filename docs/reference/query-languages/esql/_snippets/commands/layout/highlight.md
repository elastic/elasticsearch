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
HIGHLIGHT [prefix = "<prefix>"] query ON field [, field, ...] [WITH { "option": value [, ...] }]
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
:   The query used to find matching terms to highlight. This can be a string
    literal (which uses [`query_string`](/reference/query-languages/query-dsl/query-dsl-query-string-query.md)
    syntax) or a full-text search function such as
    [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md),
    [`MATCH_PHRASE`](/reference/query-languages/esql/functions-operators/search-functions/match_phrase.md),
    [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md),
    [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md),
    or the [match operator `:` ](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator).
    You can combine full-text functions using `AND`, `OR`, and `NOT`. Unqualified
    strings and `QSTR` expressions are evaluated against all fields listed in `ON`.
    All fields referenced in the query must also be listed in `ON`. If an unlisted field
    is referenced, the query is rejected before execution (for example,
    `HIGHLIGHT query field [title] is not in ON fields [body]`).

    Queries without positive match conditions (such as `NOT MATCH(...)`) have no terms
    to highlight and return `null`, unless `no_match_size` is configured.

`field`
:   One or more comma-separated columns to highlight. Fields must be `text` or
    `keyword` types (`semantic_text` fields are supported and treated as `text`).
    Wildcard column names are not supported. If a field has no matching terms,
    its output is `null` unless you set `no_match_size`.

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
:   (Optional) Analyzer used on both the query and field text. Defaults to the
    `standard` analyzer. Only built-in and node-level plugin analyzers are
    supported. If a full-text search function specifies its own `analyzer`, it
    must match the analyzer specified here.

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
    positive integer, or `-1` for no explicit limit. Defaults to `-1`. Because
    `HIGHLIGHT` runs on the coordinating node, it cannot read index settings: the
    effective limit is always capped at 1 million characters and the index's
    `index.highlight.max_analyzed_offset` setting is ignored. Text beyond the effective
    offset is ignored during highlighting.

## Description

Use `HIGHLIGHT` to find and display matching snippets in text fields, typically
after filtering rows with a full-text search condition in `WHERE`.

`HIGHLIGHT` processes each row, analyzes the specified fields against the
`query`, and generates new keyword columns containing matching terms wrapped in
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

::::{warning}
`HIGHLIGHT` is currently in preview. Note the following limitations:

* `HIGHLIGHT` re-analyzes text with the `standard` analyzer by default, rather than the analyzer configured in the index mapping. If your field uses a custom or language analyzer, specify it with the `analyzer` option in the `WITH` clause.
* The `analyzer` option only supports built-in and node-level plugin analyzers. Analyzers configured in index settings are not supported.
* On `keyword` fields, `HIGHLIGHT` tokenizes text and breaks it into snippets like a text field, rather than treating the value as a single term.
* On `semantic_text` fields, `HIGHLIGHT` performs lexical matching against the underlying text. Semantic vector matches without literal keyword overlap are not highlighted.
* Fields are analyzed up to a maximum of 1,000,000 characters. Text beyond this limit is not analyzed or highlighted.
::::

## Examples

The following examples show common ways to highlight search terms and customize snippet output.

### Highlight matches in a field

Wrap matching terms in the default `<em>` tags:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightSingleFieldForDocs.md
:::

### Highlight search results

Filter rows with a `WHERE` clause, then highlight the matching terms in the output:

:::{include} ../../generated/x-pack-esql/commands/examples/highlight.csv-spec/highlightFromIndexAfterMatchForDocs.md
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
