---
navigation_title: "ASCII folding"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-asciifolding-tokenfilter.html
---

# ASCII folding token filter [analysis-asciifolding-tokenfilter]


Converts alphabetic, numeric, and symbolic characters that are not in the Basic Latin Unicode block (first 127 ASCII characters) to their ASCII equivalent, if one exists. For example, the filter changes `à` to `a`.

This filter uses Lucene’s [ASCIIFoldingFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/miscellaneous/ASCIIFoldingFilter.html).

## Example [analysis-asciifolding-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `asciifolding` filter to drop the diacritical marks in `açaí à la carte`:

```console
GET /_analyze
{
  "tokenizer" : "standard",
  "filter" : ["asciifolding"],
  "text" : "açaí à la carte"
}
```

The filter produces the following tokens:

```text
[ acai, a, la, carte ]
```


## Add to an analyzer [analysis-asciifolding-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `asciifolding` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /asciifold_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_asciifolding": {
          "tokenizer": "standard",
          "filter": [ "asciifolding" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-asciifolding-tokenfilter-configure-parms]

`preserve_original`
:   (Optional, Boolean) If `true`, emit both original tokens and folded tokens. Defaults to `false`.


## Customize [analysis-asciifolding-tokenfilter-customize]

To customize the `asciifolding` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom `asciifolding` filter with `preserve_original` set to true:

```console
PUT /asciifold_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_asciifolding": {
          "tokenizer": "standard",
          "filter": [ "my_ascii_folding" ]
        }
      },
      "filter": {
        "my_ascii_folding": {
          "type": "asciifolding",
          "preserve_original": true
        }
      }
    }
  }
}
```


