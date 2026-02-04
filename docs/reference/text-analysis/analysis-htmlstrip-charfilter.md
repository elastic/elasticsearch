---
navigation_title: "HTML strip"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-htmlstrip-charfilter.html
---

# HTML strip character filter [analysis-htmlstrip-charfilter]


Strips HTML elements from a text and replaces HTML entities with their decoded value (e.g, replaces `&amp;` with `&`).

The `html_strip` filter uses Lucene’s [HTMLStripCharFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/charfilter/HTMLStripCharFilter.md).

## Example [analysis-htmlstrip-charfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `html_strip` filter to change the text `<p>I&apos;m so <b>happy</b>!</p>` to `\nI'm so happy!\n`.

```console
GET /_analyze
{
  "tokenizer": "keyword",
  "char_filter": [
    "html_strip"
  ],
  "text": "<p>I&apos;m so <b>happy</b>!</p>"
}
```

The filter produces the following text:

```text
[ \nI'm so happy!\n ]
```


## Add to an analyzer [analysis-htmlstrip-charfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `html_strip` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "keyword",
          "char_filter": [
            "html_strip"
          ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-htmlstrip-charfilter-configure-parms]

`escaped_tags`
:   (Optional, array of strings) Array of HTML elements without enclosing angle brackets (`< >`). The filter skips these HTML elements when stripping HTML from the text. For example, a value of `[ "p" ]` skips the `<p>` HTML element.


## Customize [analysis-htmlstrip-charfilter-customize]

To customize the `html_strip` filter, duplicate it to create the basis for a new custom character filter. You can modify the filter using its configurable parameters.

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request configures a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md) using a custom `html_strip` filter, `my_custom_html_strip_char_filter`.

The `my_custom_html_strip_char_filter` filter skips the removal of the `<b>` HTML element.

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "keyword",
          "char_filter": [
            "my_custom_html_strip_char_filter"
          ]
        }
      },
      "char_filter": {
        "my_custom_html_strip_char_filter": {
          "type": "html_strip",
          "escaped_tags": [
            "b"
          ]
        }
      }
    }
  }
}
```


