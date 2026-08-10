```yaml {applies_to}
stack: preview 9.6
serverless: preview
```

The `HIGHLIGHT` command runs a full-text query against one or more fields. For
each row, it adds a column containing the field text with matching terms wrapped
in highlight tags. It provides the same kind of highlighting as the
[`_search` API](/reference/elasticsearch/rest-apis/highlighting.md) in {{esql}}.

## Syntax

```esql
HIGHLIGHT [prefix = "<prefix>"] query ON field [, field, ...] [WITH { "option": value [, ...] }]
```

## Parameters

`prefix`
:   (Optional) String literal used to name the generated columns. Each highlighted
    field is written to `<prefix><field>`. Defaults to `highlight_`, so
    `HIGHLIGHT "fox" ON content` produces `highlight_content`. If a generated name
    collides with an existing column, the highlight column replaces it. Set an
    empty prefix (`prefix = ""`) to overwrite the source column in place instead
    of adding a new one.

`query`
:   The full-text query that identifies terms to highlight. Use a string literal,
    which follows [`query_string`](/reference/query-languages/query-dsl/query-dsl-query-string-query.md)
    semantics, or a full-text function such as
    [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md),
    [`MATCH_PHRASE`](/reference/query-languages/esql/functions-operators/search-functions/match_phrase.md),
    [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md),
    or [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md),
    or the [match operator](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator) `:`.
    You can combine full-text functions with `AND`, `OR`, and `NOT`. The field
    targeted by `MATCH`, `MATCH_PHRASE`, or the match operator must be listed in
    `ON`.

`field`
:   One or more columns to highlight. Each field must be `text` or `keyword`.
    `semantic_text` fields are supported and treated as `text`. Wildcard
    patterns are not supported. A field gets a highlighted value only when the
    query matches it. Otherwise, its result is `null`, unless you set
    `no_match_size`.

### `WITH` options

`pre_tags`
:   (Optional) Tag inserted before each highlighted term. Accepts a single tag
    only — unlike the `_search` API, multiple rotating tags are not supported.
    Defaults to `<em>`.

`post_tags`
:   (Optional) Tag inserted after each highlighted term. Like `pre_tags`,
    accepts a single tag only. Defaults to `</em>`.

`encoder`
:   (Optional) How to encode the returned text: `default` (no encoding) or `html`
    (HTML-escapes the text before inserting tags). Defaults to `default`.

`analyzer`
:   (Optional) Analyzer for both the query and field text. By default,
    `HIGHLIGHT` uses the `standard` analyzer, even when the field mapping uses a
    different analyzer. If a full-text function specifies its own `analyzer`,
    it must name the same analyzer selected here. Only built-in and node-level
    plugin analyzers are supported. For cross-cluster queries, install a plugin
    analyzer on every remote cluster that plans the query.

`number_of_fragments`
:   (Optional) Maximum number of fragments (snippets) to return per field. `0`
    disables fragmentation and returns the whole value with matches wrapped.
    Defaults to `5`.

`fragment_size`
:   (Optional) Approximate size, in characters, of each fragment. Defaults to `100`.

`no_match_size`
:   (Optional) When a field has no match, the number of leading characters of the
    field to return instead of `null`. `0` returns `null`. Defaults to `0`.

`boundary_scanner`
:   (Optional) How to find fragment boundaries. Valid values are `sentence` and
    `word`. Defaults to `sentence`.

`boundary_scanner_locale`
:   (Optional) BCP 47 language tag (for example `en-US`) used by the boundary
    scanner. Defaults to the root locale.

`order`
:   (Optional) Fragment order: `none` keeps document order, `score` orders fragments
    by descending relevance. Defaults to `none`.

`max_analyzed_offset`
:   (Optional) Maximum number of characters to analyze per field. Use a positive
    integer, or `-1` for the default of `1000000` characters. `1000000` is also
    the maximum. Larger values are capped, and `HIGHLIGHT` does not use the
    `index.highlight.max_analyzed_offset` index setting. Matches after this
    offset are not highlighted.

`boundary_chars`, `boundary_max_scan`, `phrase_limit`
:   (Optional) Accepted for compatibility with the
    [`_search` API's highlight options](/reference/elasticsearch/rest-apis/highlighting.md),
    but do not affect the result. They apply only to the fast vector highlighter,
    while `HIGHLIGHT` always uses the unified highlighter.

Other `_search` highlight options, such as `require_field_match`,
`matched_fields`, or `tags_schema`, are not supported and are rejected as
unknown options. `HIGHLIGHT` always behaves as if `require_field_match` were
`true`: only fields the query targets are highlighted.

## Description

The `HIGHLIGHT` command analyzes every field in `ON` for each input row, runs
the `query`, and wraps matching terms in the configured tags. It adds one keyword
column per field, named `highlight_<field>` by default. Fields with no match
return `null`. If `no_match_size` is greater than `0`, they return the beginning
of the field instead.

A multivalued field is highlighted as one continuous text, and the resulting
fragments are returned as a multivalued column value. Multivalued `keyword`
fields are loaded from doc values — sorted and deduplicated — so fragments can
differ in order from the `_search` API, which highlights values in their
original `_source` order.

Use `HIGHLIGHT` after a full-text `WHERE` filter to show *why* a document
matched, as you would with post-fetch highlighting in the `_search` API.

:::{warning}
`HIGHLIGHT` is in [preview](/reference/query-languages/esql/limitations.md), and
its behavior may change in future releases. Current limitations include:

* `HIGHLIGHT` does not use the analyzer configured in a field mapping. It
  re-analyzes field text and the query with the `standard` analyzer unless you set
  the `analyzer` option. A field mapped with a language or custom analyzer can
  therefore return a `null` highlight for a row that a pushed-down `MATCH`
  selected.
* The `analyzer` option accepts only built-in and node-level plugin analyzers, not
  analyzers defined in index settings.
* On `keyword` fields, `HIGHLIGHT` applies text-field match and fragmentation
  semantics rather than the whole-value semantics of the `_search` API.
* On `semantic_text` fields, `HIGHLIGHT` matches literal terms in the field
  text. It does not use the semantic highlighter of the `_search` API, so a row
  selected by a semantic `MATCH` in `WHERE` can still return a `null`
  highlight when the query terms don't appear literally in the text.
* `HIGHLIGHT` analyzes at most 1,000,000 characters per field value. It does not
  highlight matches after that offset. The `_search` API returns an error instead.
:::

## Examples

### Highlight matches in a field

Wrap the matching term in the default `<em>` tags:

```esql
ROW content = "The quick brown fox jumps over the lazy dog."
| HIGHLIGHT "fox" ON content
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| `The quick brown <em>fox</em> jumps over the lazy dog.` |

### Highlight the results of a full-text search

Use `MATCH` to filter rows, then highlight the matching field:

```esql
FROM books
| WHERE MATCH(title, "Return")
| HIGHLIGHT "return" ON title
| KEEP book_no, highlight_title
| SORT book_no
```

| book_no:keyword | highlight_title:keyword |
| --- | --- |
| 2714 | `<em>Return</em> of the King Being the Third Part of The Lord of the Rings` |
| 7350 | `<em>Return</em> of the Shadow` |

### Use a full-text function as the query

You can also use a full-text function as the query. `MATCH_PHRASE` wraps the
entire matching phrase in one pair of tags:

```esql
FROM books
| WHERE MATCH(title, "Return")
| HIGHLIGHT MATCH_PHRASE(title, "Return of the") ON title
| KEEP book_no, highlight_title
| SORT book_no
```

| book_no:keyword | highlight_title:keyword |
| --- | --- |
| 2714 | `<em>Return of the</em> King Being the Third Part of The Lord of the Rings` |
| 7350 | `<em>Return of the</em> Shadow` |

### Customize the highlight tags

Use `pre_tags` and `post_tags` to change the wrapping tags:

```esql
ROW content = "The quick brown fox jumps over the lazy dog."
| HIGHLIGHT "fox" ON content WITH { "pre_tags": ["<b>"], "post_tags": ["</b>"] }
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| `The quick brown <b>fox</b> jumps over the lazy dog.` |

### Name the output column with a prefix

A non-empty `prefix` keeps the source column and adds a `<prefix><field>` column:

```esql
ROW content = "The One Ring was forged by Sauron."
| HIGHLIGHT prefix = "hl_" "ring" ON content
| KEEP content, hl_content
```

| content:keyword | hl_content:keyword |
| --- | --- |
| The One Ring was forged by Sauron. | `The One <em>Ring</em> was forged by Sauron.` |

### Return leading text when nothing matches

By default, a field with no match returns `null`. Set `no_match_size` to return
the beginning of the field instead:

```esql
ROW content = "Gardens and flowers bloom in spring."
| HIGHLIGHT "elasticsearch" ON content WITH { "no_match_size": 200 }
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| Gardens and flowers bloom in spring. |

### Order fragments by score

With `order` set to `score`, the highest-scoring fragments come first:

```esql
ROW content = ["fast search", "fast and fast results"]
| HIGHLIGHT "fast" ON content WITH { "order": "score" }
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| `[<em>fast</em> and <em>fast</em> results, <em>fast</em> search]` |
