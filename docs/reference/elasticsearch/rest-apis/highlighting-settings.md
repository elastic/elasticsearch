---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
applies_to:
  stack: all
  serverless: all
---

# Highlighting settings [highlighting-settings]

Highlighting settings control how {{es}} generates, ranks, and displays snippets that contain the matching terms.
You can set defaults once and override them for specific fields if needed.
Below, you’ll find descriptions of all available highlighting settings. For examples of how to apply them in practice, refer to [Highlighting examples](highlighting-examples.md).


$$$boundary_chars$$$

boundary_chars
:   A string that contains each boundary character. Defaults to `.,!? \t\n`.

$$$boundary_max_scan$$$

boundary_max_scan
:   How far to scan for boundary characters. Defaults to `20`.

$$$boundary-scanner$$$

boundary_scanner
:   Specifies how to break the highlighted fragments: `chars`, `sentence`, or `word`. Only valid for the `unified` and `fvh` highlighters. Defaults to `sentence` for the `unified` highlighter. Defaults to `chars` for the `fvh` highlighter.

    `chars`
    :   Use the characters specified by `boundary_chars` as highlighting boundaries. The `boundary_max_scan` setting controls how far to scan for boundary characters. Only valid for the `fvh` highlighter.

    `sentence`
    :   Break highlighted fragments at the next sentence boundary, as determined by Java’s [BreakIterator](https://docs.oracle.com/javase/8/docs/api/java/text/BreakIterator.md). You can specify the locale to use with `boundary_scanner_locale`.
        ::::{note}
        When used with the `unified` highlighter, the `sentence` scanner splits sentences bigger than `fragment_size` at the first word boundary next to `fragment_size`. You can set `fragment_size` to 0 to never split any sentence.
        ::::


    `word`
    :   Break highlighted fragments at the next word boundary, as determined by Java’s [BreakIterator](https://docs.oracle.com/javase/8/docs/api/java/text/BreakIterator.md). You can specify the locale to use with `boundary_scanner_locale`.


$$$boundary_scanner_locale$$$

boundary_scanner_locale
:   Controls which locale is used to search for sentence and word boundaries. This parameter takes a form of a language tag, e.g. `"en-US"`,  `"fr-FR"`, `"ja-JP"`. More info can be found in the [Locale Language Tag](https://docs.oracle.com/javase/8/docs/api/java/util/Locale.md#forLanguageTag-java.lang.String-) documentation. The default value is [ Locale.ROOT](https://docs.oracle.com/javase/8/docs/api/java/util/Locale.md#ROOT).

encoder
:   Indicates if the snippet should be HTML encoded: `default` (no encoding) or `html` (HTML-escape the snippet text and then insert the highlighting tags)

fields
:   Specifies the fields to retrieve highlights for. You can use wildcards to specify fields. For example, you could specify `comment_*` to get highlights for all [text](/reference/elasticsearch/mapping-reference/text.md), [match_only_text](/reference/elasticsearch/mapping-reference/match-only-text.md), [pattern_text](/reference/elasticsearch/mapping-reference/pattern-text.md), and [keyword](/reference/elasticsearch/mapping-reference/keyword.md) fields that start with `comment_`.

    ::::{note}
    Only text, match_only_text, pattern_text, and keyword fields are highlighted when you use wildcards. If you use a custom mapper and want to highlight on a field anyway, you must explicitly specify that field name.
    ::::

$$$fragmenter$$$

fragmenter
:   Specifies how text should be broken up in highlight snippets: `simple` or `span`. Only valid for the `plain` highlighter. Defaults to `span`.

    `simple`
    :   Breaks up text into same-sized fragments.

    `span`
    :   Breaks up text into same-sized fragments, but tries to avoid breaking up text between highlighted terms. This is helpful when you’re querying for phrases. Default.


fragment_offset
:   Controls the margin from which you want to start highlighting. Only valid when using the `fvh` highlighter.

$$$fragment_size$$$

fragment_size
:   The size of the highlighted fragment in characters. Defaults to 100.

highlight_query
:   Highlight matches for a query other than the search query. This is especially useful if you use a rescore query because those are not taken into account by highlighting by default.

    ::::{important}
    {{es}} does not validate that `highlight_query` contains the search query in any way so it is possible to define it so legitimate query results are not highlighted. Generally, you should include the search query as part of the `highlight_query`.
    ::::


matched_fields
:   Combine matches on multiple fields to highlight a single field. This is most intuitive for multifields that analyze the same string in different ways. Valid for the `unified` and fvh` highlighters, but the behavior of this option is different for each highlighter.

For the `unified` highlighter:

* `matched_fields` array should **not** contain the original field that you want to highlight. The original field will be automatically added to the `matched_fields`, and there is no way to exclude its matches when highlighting.
* `matched_fields` and the original field can be indexed with different strategies (with or without `offsets`, with or without `term_vectors`).
* only the original field to which the matches are combined is loaded so only that field benefits from having `store` set to `yes`

For the `fvh` highlighter:

* `matched_fields` array may or may not contain the original field depending on your needs. If you want to include the original field’s matches in highlighting, add it to the `matched_fields` array.
* all `matched_fields` must have `term_vector` set to `with_positions_offsets`
* only the original field to which the matches are combined is loaded so only that field benefits from having `store` set to `yes`.

    no_match_size
    :   The amount of text you want to return from the beginning of the field if there are no matching fragments to highlight. Defaults to 0 (nothing is returned).

    $$$number_of_fragments$$$

    number_of_fragments
    :   The maximum number of fragments to return. If the number of fragments is set to 0, no fragments are returned. Instead, the entire field contents are highlighted and returned. This can be handy when you need to highlight short texts such as a title or address, but fragmentation is not required. If `number_of_fragments` is 0, `fragment_size` is ignored. Defaults to 5.

    order
    :   Sorts highlighted fragments by score when set to `score`. By default, fragments will be output in the order they appear in the field (order: `none`). Setting this option to `score` will output the most relevant fragments first. Each highlighter applies its own logic to compute relevancy scores. See the document [How highlighters work internally](how-es-highlighters-work-internally.md) for more details how different highlighters find the best fragments.

    phrase_limit
    :   Controls the number of matching phrases in a document that are considered. Prevents the `fvh` highlighter from analyzing too many phrases and consuming too much memory. When using `matched_fields`, `phrase_limit` phrases per matched field are considered. Raising the limit increases query time and consumes more memory. Only supported by the `fvh` highlighter. Defaults to 256.

    $$$pre_tags$$$

    pre_tags
    :   Use in conjunction with `post_tags` to define the HTML tags to use for the highlighted text. By default, highlighted text is wrapped in `<em>` and `</em>` tags. Specify as an array of strings.

    $$$post_tags$$$

    post_tags
    :   Use in conjunction with `pre_tags` to define the HTML tags to use for the highlighted text. By default, highlighted text is wrapped in `<em>` and `</em>` tags. Specify as an array of strings.

    require_field_match
    :   By default, only fields that contains a query match are highlighted. Set `require_field_match` to `false` to highlight all fields. Defaults to `true`.


$$$max-analyzed-offset$$$

max_analyzed_offset
:   By default, the maximum number of characters analyzed for a highlight request is bounded by the value defined in the [`index.highlight.max_analyzed_offset`](/reference/elasticsearch/index-settings/index-modules.md#index-max-analyzed-offset) setting, and when the number of characters exceeds this limit an error is returned. If this setting is set to a positive value, the highlighting stops at this defined maximum limit, and the rest of the text is not processed, thus not highlighted and no error is returned. If it is specifically set to -1 then the value of [`index.highlight.max_analyzed_offset`](/reference/elasticsearch/index-settings/index-modules.md#index-max-analyzed-offset) is used instead. For values < -1 or 0, an error is returned. The [`max_analyzed_offset`](#max-analyzed-offset) query setting does **not** override the [`index.highlight.max_analyzed_offset`](/reference/elasticsearch/index-settings/index-modules.md#index-max-analyzed-offset) which prevails when it’s set to lower value than the query setting.

tags_schema
:   Set to `styled` to use the built-in tag schema. The `styled` schema defines the following `pre_tags` and defines `post_tags` as `</em>`.

    ```html
    <em class="hlt1">, <em class="hlt2">, <em class="hlt3">,
    <em class="hlt4">, <em class="hlt5">, <em class="hlt6">,
    <em class="hlt7">, <em class="hlt8">, <em class="hlt9">,
    <em class="hlt10">
    ```


$$$highlighter-type$$$

type
:   The highlighter to use: `unified`, `plain`, or `fvh`. Defaults to `unified`.
