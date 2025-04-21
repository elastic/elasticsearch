---
navigation_title: "Pattern replace"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-pattern_replace-tokenfilter.html
---

# Pattern replace token filter [analysis-pattern_replace-tokenfilter]


Uses a regular expression to match and replace token substrings.

The `pattern_replace` filter uses [Java’s regular expression syntax](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md). By default, the filter replaces matching substrings with an empty substring (`""`). Replacement substrings can use Java’s [`$g` syntax](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Matcher.md#appendReplacement-java.lang.StringBuffer-java.lang.String-) to reference capture groups from the original token text.

::::{warning}
A poorly-written regular expression may run slowly or return a StackOverflowError, causing the node running the expression to exit suddenly.

Read more about [pathological regular expressions and how to avoid them](https://www.regular-expressions.info/catastrophic.html).

::::


This filter uses Lucene’s [PatternReplaceFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/pattern/PatternReplaceFilter.html).

## Example [analysis-pattern-replace-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `pattern_replace` filter to prepend `watch` to the substring `dog` in `foxes jump lazy dogs`.

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [
    {
      "type": "pattern_replace",
      "pattern": "(dog)",
      "replacement": "watch$1"
    }
  ],
  "text": "foxes jump lazy dogs"
}
```

The filter produces the following tokens.

```text
[ foxes, jump, lazy, watchdogs ]
```


## Configurable parameters [analysis-pattern-replace-tokenfilter-configure-parms]

`all`
:   (Optional, Boolean) If `true`, all substrings matching the `pattern` parameter’s regular expression are replaced. If `false`, the filter replaces only the first matching substring in each token. Defaults to `true`.

`pattern`
:   (Required, string) Regular expression, written in [Java’s regular expression syntax](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md). The filter replaces token substrings matching this pattern with the substring in the `replacement` parameter.

`replacement`
:   (Optional, string) Replacement substring. Defaults to an empty substring (`""`).


## Customize and add to an analyzer [analysis-pattern-replace-tokenfilter-customize]

To customize the `pattern_replace` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request configures a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md) using a custom `pattern_replace` filter, `my_pattern_replace_filter`.

The `my_pattern_replace_filter` filter uses the regular expression `[£|€]` to match and remove the currency symbols `£` and `€`. The filter’s `all` parameter is `false`, meaning only the first matching symbol in each token is removed.

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "keyword",
          "filter": [
            "my_pattern_replace_filter"
          ]
        }
      },
      "filter": {
        "my_pattern_replace_filter": {
          "type": "pattern_replace",
          "pattern": "[£|€]",
          "replacement": "",
          "all": false
        }
      }
    }
  }
}
```


