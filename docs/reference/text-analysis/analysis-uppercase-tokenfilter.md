---
navigation_title: "Uppercase"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-uppercase-tokenfilter.html
---

# Uppercase token filter [analysis-uppercase-tokenfilter]


Changes token text to uppercase. For example, you can use the `uppercase` filter to change `the Lazy DoG` to `THE LAZY DOG`.

This filter uses Lucene’s [UpperCaseFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/core/UpperCaseFilter.md).

::::{warning}
Depending on the language, an uppercase character can map to multiple lowercase characters. Using the `uppercase` filter could result in the loss of lowercase character information.

To avoid this loss but still have a consistent letter case, use the [`lowercase`](/reference/text-analysis/analysis-lowercase-tokenfilter.md) filter instead.

::::


## Example [analysis-uppercase-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the default `uppercase` filter to change the `the Quick FoX JUMPs` to uppercase:

```console
GET _analyze
{
  "tokenizer" : "standard",
  "filter" : ["uppercase"],
  "text" : "the Quick FoX JUMPs"
}
```

The filter produces the following tokens:

```text
[ THE, QUICK, FOX, JUMPS ]
```


## Add to an analyzer [analysis-uppercase-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `uppercase` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT uppercase_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "whitespace_uppercase": {
          "tokenizer": "whitespace",
          "filter": [ "uppercase" ]
        }
      }
    }
  }
}
```


