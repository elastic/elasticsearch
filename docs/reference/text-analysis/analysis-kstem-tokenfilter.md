---
navigation_title: "KStem"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-kstem-tokenfilter.html
---

# KStem token filter [analysis-kstem-tokenfilter]


Provides [KStem](https://ciir.cs.umass.edu/pubfiles/ir-35.pdf)-based stemming for the English language. The `kstem` filter combines [algorithmic stemming](docs-content://manage-data/data-store/text-analysis/stemming.md#algorithmic-stemmers) with a built-in [dictionary](docs-content://manage-data/data-store/text-analysis/stemming.md#dictionary-stemmers).

The `kstem` filter tends to stem less aggressively than other English stemmer filters, such as the [`porter_stem`](/reference/text-analysis/analysis-porterstem-tokenfilter.md) filter.

The `kstem` filter is equivalent to the [`stemmer`](/reference/text-analysis/analysis-stemmer-tokenfilter.md) filter’s [`light_english`](/reference/text-analysis/analysis-stemmer-tokenfilter.md#analysis-stemmer-tokenfilter-language-parm) variant.

This filter uses Lucene’s [KStemFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/en/KStemFilter.md).

## Example [analysis-kstem-tokenfilter-analyze-ex]

The following analyze API request uses the `kstem` filter to stem `the foxes jumping quickly` to `the fox jump quick`:

```console
GET /_analyze
{
  "tokenizer": "standard",
  "filter": [ "kstem" ],
  "text": "the foxes jumping quickly"
}
```

The filter produces the following tokens:

```text
[ the, fox, jump, quick ]
```


## Add to an analyzer [analysis-kstem-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `kstem` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

::::{important}
To work properly, the `kstem` filter requires lowercase tokens. To ensure tokens are lowercased, add the [`lowercase`](/reference/text-analysis/analysis-lowercase-tokenfilter.md) filter before the `kstem` filter in the analyzer configuration.

::::


```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "whitespace",
          "filter": [
            "lowercase",
            "kstem"
          ]
        }
      }
    }
  }
}
```


