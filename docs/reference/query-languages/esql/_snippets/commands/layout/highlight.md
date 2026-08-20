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
:   (Optional) A string prefix used to name the output columns. Each highlighted
    field is written to `<prefix><field>`. Defaults to `highlight_` (for example,
    `HIGHLIGHT "fox" ON content` produces `highlight_content`). If a generated
    column name matches an existing column, the existing column is replaced. To
    overwrite the source column in place, specify an empty prefix (`prefix = ""`).

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
    Any field referenced inside the query must also be listed in `ON`. Queries
    without positive match conditions (such as `NOT MATCH(...)`) return `null`.

`field`
:   One or more comma-separated columns to highlight. Fields must be `text` or
    `keyword` types (`semantic_text` fields are supported and treated as `text`).
    Wildcard column names are not supported. If a field has no matching terms,
    its output is `null` unless you set `no_match_size`.

## WITH options

All option values passed in the `WITH` clause must be constant literals.

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
    `default`. This value is case-sensitive, so `html` is valid but `HTML` is rejected.

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
    `sentence` or `word`. Defaults to `sentence`.

`boundary_scanner_locale`
:   (Optional) BCP 47 language tag (such as `en-US`) used by the boundary scanner.
    Use hyphens as separators (`en-US`). Defaults to the root locale.

`order`
:   (Optional) Sort order of returned fragments. Accepts `none` (preserves document
    order) or `score` (orders fragments by descending relevance score). Defaults
    to `none`.

`max_analyzed_offset`
:   (Optional) Maximum number of characters to analyze per field value. Accepts
    a positive integer, or `-1` to use the index default. Values are capped at
    1 million characters. Defaults to `-1`. Text beyond the effective
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

```esql
ROW content = "The quick brown fox jumps over the lazy dog."
| HIGHLIGHT "fox" ON content
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| `The quick brown <em>fox</em> jumps over the lazy dog.` |

### Highlight search results

Filter rows with a `WHERE` clause, then highlight the matching terms in the output:

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

### Highlight phrases with MATCH_PHRASE

Use a full-text function like `MATCH_PHRASE` to highlight an exact phrase in a single tag pair:

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

### Highlight with query string syntax (QSTR)

Use [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md) to highlight terms using Lucene query syntax with boolean operators and field qualifiers:

```esql
ROW title = "The quick fox", body = "A loyal dog"
| HIGHLIGHT QSTR("title:fox OR body:dog") ON title, body
| KEEP highlight_title, highlight_body
```

| highlight_title:keyword | highlight_body:keyword |
| --- | --- |
| `The quick <em>fox</em>` | `A loyal <em>dog</em>` |

### Highlight with Kibana Query Language (KQL)

Use [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md) to highlight terms using Kibana Query Language syntax with nested grouping:

```esql
FROM books
| WHERE MATCH(title, "Return")
| HIGHLIGHT KQL("title: (\"return of the\" AND (king OR shadow))") ON title
| KEEP book_no, highlight_title
| SORT book_no
```

| book_no:keyword | highlight_title:keyword |
| --- | --- |
| 2714 | `<em>Return of the</em> <em>King</em> Being the Third Part of The Lord of the Rings` |
| 7350 | `<em>Return of the</em> <em>Shadow</em>` |

### Highlight with a language analyzer

Use the `analyzer` option to apply language-specific stemming rules. In this example, the `english` analyzer stems `Rings` to `ring`:

```esql
ROW title = "The Lord of the Rings"
| HIGHLIGHT "ring" ON title WITH { "analyzer": "english" }
| KEEP highlight_title
```

| highlight_title:keyword |
| --- |
| `The Lord of the <em>Rings</em>` |

### Highlight multiple fields

Highlight multiple columns at once by listing them in `ON`:

```esql
ROW title = "Return of the King", body = "Tolkien wrote the epic saga."
| HIGHLIGHT "king tolkien" ON title, body
| KEEP highlight_title, highlight_body
```

| highlight_title:keyword | highlight_body:keyword |
| --- | --- |
| `Return of the <em>King</em>` | `<em>Tolkien</em> wrote the epic saga.` |

### Highlight an extracted or computed field

`HIGHLIGHT` re-analyzes field values at query time, so it works on columns created earlier in the pipeline:

```esql
ROW raw = "2024 Sauron Mordor"
| DISSECT raw "%{yr} %{name} %{place}"
| HIGHLIGHT "sauron" ON name
| KEEP name, highlight_name
```

| name:keyword | highlight_name:keyword |
| --- | --- |
| Sauron | `<em>Sauron</em>` |

### HTML-encode text for safe display

Use `"encoder": "html"` to escape HTML tags and special characters in the text while keeping the highlight tags intact:

```esql
ROW content = "Use <b>bold</b> tags & special chars with the Ring."
| HIGHLIGHT "ring" ON content WITH { "encoder": "html" }
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| `Use &lt;b&gt;bold&lt;&#x2F;b&gt; tags &amp; special chars with the <em>Ring</em>.` |

### Return the full text without fragmenting

Set `"number_of_fragments": 0` to return the complete text value with matches highlighted rather than returning individual snippets:

```esql
ROW content = "Elasticsearch is fast. Elasticsearch is scalable. Elasticsearch is open."
| HIGHLIGHT "elasticsearch" ON content WITH { "number_of_fragments": 0 }
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| `<em>Elasticsearch</em> is fast. <em>Elasticsearch</em> is scalable. <em>Elasticsearch</em> is open.` |

### Customize highlight tags

Use `pre_tags` and `post_tags` to specify custom wrapping tags:

```esql
ROW content = "The quick brown fox jumps over the lazy dog."
| HIGHLIGHT "fox" ON content WITH { "pre_tags": ["<b>"], "post_tags": ["</b>"] }
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| `The quick brown <b>fox</b> jumps over the lazy dog.` |

### Customize output column names

Use `prefix` to change the column name prefix:

```esql
ROW content = "The One Ring was forged by Sauron."
| HIGHLIGHT prefix = "hl_" "ring" ON content
| KEEP content, hl_content
```

| content:keyword | hl_content:keyword |
| --- | --- |
| The One Ring was forged by Sauron. | `The One <em>Ring</em> was forged by Sauron.` |

### Overwrite the original column

Set an empty prefix (`prefix = ""`) to replace the source column with the highlighted output:

```esql
ROW content = "The quick brown fox jumps over the lazy dog."
| HIGHLIGHT prefix = "" "fox" ON content
| KEEP content
```

| content:keyword |
| --- |
| `The quick brown <em>fox</em> jumps over the lazy dog.` |

### Return leading text when nothing matches

By default, non-matching fields evaluate to `null`. Set `no_match_size` to return text from the start of the field instead:

```esql
ROW content = "Gardens and flowers bloom in spring."
| HIGHLIGHT "elasticsearch" ON content WITH { "no_match_size": 200 }
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| Gardens and flowers bloom in spring. |

### Order snippets by relevance score

Use `"order": "score"` to sort snippets by relevance score rather than document order:

```esql
ROW content = ["fast search", "fast and fast results"]
| HIGHLIGHT "fast" ON content WITH { "order": "score" }
| KEEP highlight_content
```

| highlight_content:keyword |
| --- |
| `[<em>fast</em> and <em>fast</em> results, <em>fast</em> search]` |
