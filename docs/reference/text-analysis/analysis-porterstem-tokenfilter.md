---
navigation_title: "Porter stem"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-porterstem-tokenfilter.html
---

# Porter stem token filter [analysis-porterstem-tokenfilter]


Provides [algorithmic stemming](docs-content://manage-data/data-store/text-analysis/stemming.md#algorithmic-stemmers) for the English language, based on the [Porter stemming algorithm](https://snowballstem.org/algorithms/porter/stemmer.html).

This filter tends to stem more aggressively than other English stemmer filters, such as the [`kstem`](/reference/text-analysis/analysis-kstem-tokenfilter.md) filter.

The `porter_stem` filter is equivalent to the [`stemmer`](/reference/text-analysis/analysis-stemmer-tokenfilter.md) filter’s [`english`](/reference/text-analysis/analysis-stemmer-tokenfilter.md#analysis-stemmer-tokenfilter-language-parm) variant.

The `porter_stem` filter uses Lucene’s [PorterStemFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/en/PorterStemFilter.html).

## Example [analysis-porterstem-tokenfilter-analyze-ex]

The following analyze API request uses the `porter_stem` filter to stem `the foxes jumping quickly` to `the fox jump quickli`:

```console
GET /_analyze
{
  "tokenizer": "standard",
  "filter": [ "porter_stem" ],
  "text": "the foxes jumping quickly"
}
```

The filter produces the following tokens:

```text
[ the, fox, jump, quickli ]
```


## Add to an analyzer [analysis-porterstem-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `porter_stem` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

::::{important}
To work properly, the `porter_stem` filter requires lowercase tokens. To ensure tokens are lowercased, add the [`lowercase`](/reference/text-analysis/analysis-lowercase-tokenfilter.md) filter before the `porter_stem` filter in the analyzer configuration.

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
            "porter_stem"
          ]
        }
      }
    }
  }
}
```


