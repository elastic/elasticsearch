---
navigation_title: "Reverse"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-reverse-tokenfilter.html
---

# Reverse token filter [analysis-reverse-tokenfilter]


Reverses each token in a stream. For example, you can use the `reverse` filter to change `cat` to `tac`.

Reversed tokens are useful for suffix-based searches, such as finding words that end in `-ion` or searching file names by their extension.

This filter uses Lucene’s [ReverseStringFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/reverse/ReverseStringFilter.html).

## Example [analysis-reverse-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `reverse` filter to reverse each token in `quick fox jumps`:

```console
GET _analyze
{
  "tokenizer" : "standard",
  "filter" : ["reverse"],
  "text" : "quick fox jumps"
}
```

The filter produces the following tokens:

```text
[ kciuq, xof, spmuj ]
```


## Add to an analyzer [analysis-reverse-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `reverse` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT reverse_example
{
  "settings" : {
    "analysis" : {
      "analyzer" : {
        "whitespace_reverse" : {
          "tokenizer" : "whitespace",
          "filter" : ["reverse"]
        }
      }
    }
  }
}
```


