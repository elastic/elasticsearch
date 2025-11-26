---
applies_to:
  stack:
  serverless:
navigation_title: "Pattern Text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/pattern-text.html
---

# Pattern text field type [pattern-text-field-type]
```{applies_to}
serverless: preview
stack: preview 9.2
```
:::{note}
This feature requires an Enterprise subscription.
:::

The `pattern_text` field type is a variant of [`text`](/reference/elasticsearch/mapping-reference/text.md) with improved space efficiency for log data.
Internally, it decomposes values into static parts that are likely to be shared among many values, and dynamic parts that tend to vary.
The static parts usually come from the explanatory text of a log message, while the dynamic parts are the variables that were interpolated into the logs.
This decomposition allows for improved compression on log-like data.

We call the static portion of the value the `template`.
Although the template cannot be accessed directly, a separate field called `<field_name>.template_id` is accessible.
This field is a hash of the template and can be used to group similar values.

Analysis is configurable but defaults to a delimiter-based analyzer.
This analyzer applies a lowercase filter and then splits on whitespace and the following delimiters: `=`, `?`, `:`, `[`, `]`, `{`, `}`, `"`, `\`, `'`.

## Limitations

Unlike most mapping types, `pattern_text` does not support multiple values for a given field per document.
If a document is created with multiple values for a pattern_text field, an error will be returned.

[span queries](/reference/query-languages/query-dsl/span-queries.md) are not supported with this field, use [interval queries](/reference/query-languages/query-dsl/query-dsl-intervals-query.md) instead, or the [`text`](/reference/elasticsearch/mapping-reference/text.md) field type if you absolutely need span queries.

Like `text`, `pattern_text` does not support sorting and has only limited support for aggregations.

## Phrase matching
Pattern text supports an `index_options` parameter with valid values of `docs` and `positions`.
The default value is `docs`, which makes `pattern_text` behave similarly to `match_only_text` for phrase queries.
Specifically, positions are not stored, which reduces the index size at the cost of slowing down phrase queries.
If `index_options` is set to `positions`, positions are stored and `pattern_text` will support fast phrase queries.
In both cases, all queries return a constant score of 1.0.

## Index sorting for improved compression
The compression provided by `pattern_text` can be significantly improved if the index is sorted by the `template_id` field.


### Default sorting for LogsDB
```{applies_to}
serverless: preview
stack: preview 9.3
```
This index sorting is not applied by default, but can be enabled for the `message` field of LogsDB indices (assuming it is of type `pattern_text`) by setting the index setting `index.logsdb.default_sort_on_message_template` to `true`.
This will cause the index to be sorted by `host.name` (if present), then `message.template_id`, and finally by `@timestamp`.


### Custom sorting
If the index is not LogsDB or the `pattern_text` field is named something other than `message`, index sorting can still be manually applied as shown in the following example.

```console
PUT logs
{
  "settings": {
    "index": {
      "sort.field": [ "notice.template_id", "@timestamp" ],
      "sort.order": [ "asc", "desc" ]
    }
  },
  "mappings": {
    "properties": {
      "@timestamp": {
        "type": "date"
      },
      "notice": {
        "type": "pattern_text"
      }
    }
  }
}
```



## Parameters for pattern text fields [pattern-text-params]

The following mapping parameters are accepted:

[`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md)
:   The [analyzer](docs-content://manage-data/data-store/text-analysis.md) which should be used for the `pattern_text` field, both at index-time and at search-time (unless overridden by the  [`search_analyzer`](/reference/elasticsearch/mapping-reference/search-analyzer.md)).
Supports a delimiter-based analyzer and the standard analyzer, as is used in `match_only_text` mappings.
Defaults to the delimiter-based analyzer, which applies a lowercase filter and then splits on whitespace and the following delimiters: `=`, `?`, `:`, `[`, `]`, `{`, `}`, `"`, `\`, `'`.

[`index_options`](/reference/elasticsearch/mapping-reference/index-options.md)
:   What information should be stored in the index, for search and highlighting purposes. Valid values are `docs` and `positions`. Defaults to `docs`.

[`meta`](/reference/elasticsearch/mapping-reference/mapping-field-meta.md)
:   Metadata about the field.

