---
navigation_title: "Apostrophe"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-apostrophe-tokenfilter.html
---

# Apostrophe token filter [analysis-apostrophe-tokenfilter]


Strips all characters after an apostrophe, including the apostrophe itself.

This filter is included in {{es}}'s built-in [Turkish language analyzer](/reference/text-analysis/analysis-lang-analyzer.md#turkish-analyzer). It uses Lucene’s [ApostropheFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/tr/ApostropheFilter.html), which was built for the Turkish language.

## Example [analysis-apostrophe-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request demonstrates how the apostrophe token filter works.

```console
GET /_analyze
{
  "tokenizer" : "standard",
  "filter" : ["apostrophe"],
  "text" : "Istanbul'a veya Istanbul'dan"
}
```

The filter produces the following tokens:

```text
[ Istanbul, veya, Istanbul ]
```


## Add to an analyzer [analysis-apostrophe-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the apostrophe token filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /apostrophe_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_apostrophe": {
          "tokenizer": "standard",
          "filter": [ "apostrophe" ]
        }
      }
    }
  }
}
```


