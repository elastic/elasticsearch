```yaml {applies_to}
stack: preview 9.6
serverless: preview
```

The `HIGHLIGHT` command runs a full-text query against one or more fields and, for
each row, returns a new column containing the field text with the matching terms
wrapped in highlight tags. It brings the highlighting you get from the
[`_search` API](/reference/elasticsearch/rest-apis/highlighting.md) into {{esql}}.

## Syntax

```esql
HIGHLIGHT [prefix = "<prefix>"] query ON field [, field, ...] [WITH { "option": value [, ...] }]
```

## Parameters

`prefix`
:   (Optional) String literal used to name the generated columns. Each highlighted
    field is written to `<prefix><field>`. Defaults to `highlight_`, so
    `HIGHLIGHT "fox" ON content` produces `highlight_content`. Set an empty prefix
    (`prefix = ""`) to overwrite the source column in place instead of adding a new
    one.

`query`
:   The full-text query used to select the terms to highlight. This can be a string
    literal — interpreted with [`query_string`](/reference/query-languages/query-dsl/query-dsl-query-string-query.md)
    semantics — or a full-text function such as
    [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md),
    [`MATCH_PHRASE`](/reference/query-languages/esql/functions-operators/search-functions/match_phrase.md),
    [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md),
    or [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md),
    or the [match operator](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator) `:`.

`field`
:   One or more columns to highlight. Each field must be of type `text` or
    `keyword`; `semantic_text` fields are supported and treated as `text`. Only
    the fields the query actually matches produce a highlighted value; a field
    with no match yields `null` (unless `no_match_size` is set).

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
:   (Optional) Name of the analyzer used to analyze both the query and the field
    text. If omitted, `HIGHLIGHT` uses the `standard` analyzer, not the analyzer
    configured in the field mapping. Only built-in and node-level plugin analyzers
    are supported.

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
:   (Optional) How fragment boundaries are found: `sentence` or `word`. Defaults to
    `sentence`.

`boundary_scanner_locale`
:   (Optional) BCP 47 language tag (for example `en-US`) used by the boundary
    scanner. Defaults to the root locale.

`order`
:   (Optional) Fragment order: `none` keeps document order, `score` orders fragments
    by descending relevance. Defaults to `none`.

`max_analyzed_offset`
:   (Optional) Maximum number of characters analyzed per field. A positive
    integer, or `-1` for the default of `1000000` characters. `1000000` is also
    the upper bound: larger values are capped to it, and the
    `index.highlight.max_analyzed_offset` index setting is not consulted.
    Matches beyond this offset are not highlighted.

## Description

For every input row, `HIGHLIGHT` analyzes the text of each `ON` field, runs the
`query` against it, and wraps the matching terms in the configured tags. The result
is appended as a keyword column per field (`highlight_<field>` by default). Fields
with no match return `null`, unless `no_match_size` is greater than `0`, in which
case the field's leading text is returned instead.

A multivalued field is highlighted as one continuous text, and the resulting
fragments are returned as a multivalued column value.

`HIGHLIGHT` is typically used after a full-text `WHERE` filter to show *why* a
document matched, mirroring the post-fetch highlighting of the `_search` API.

:::{warning}
`HIGHLIGHT` is in [preview](/reference/query-languages/esql/limitations.md). Its
behavior may change in future releases. Be aware of these limitations:

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
* `HIGHLIGHT` analyzes at most 1,000,000 characters per field value. Matches
  beyond that offset are silently not highlighted, where the `_search` API
  would return an error.
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

Filter with `MATCH`, then highlight the matched field:

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

The query can also be a full-text function. `MATCH_PHRASE` wraps the whole
matched phrase in a single pair of tags:

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

By default a non-matching field returns `null`. Set `no_match_size` to return the
field's leading text instead:

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
