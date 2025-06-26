---
navigation_title: "Lowercase"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-lowercase-tokenfilter.html
---

# Lowercase token filter [analysis-lowercase-tokenfilter]


Changes token text to lowercase. For example, you can use the `lowercase` filter to change `THE Lazy DoG` to `the lazy dog`.

In addition to a default filter, the `lowercase` token filter provides access to Lucene’s language-specific lowercase filters for Greek, Irish, and Turkish.

## Example [analysis-lowercase-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the default `lowercase` filter to change the `THE Quick FoX JUMPs` to lowercase:

```console
GET _analyze
{
  "tokenizer" : "standard",
  "filter" : ["lowercase"],
  "text" : "THE Quick FoX JUMPs"
}
```

The filter produces the following tokens:

```text
[ the, quick, fox, jumps ]
```


## Add to an analyzer [analysis-lowercase-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `lowercase` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT lowercase_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "whitespace_lowercase": {
          "tokenizer": "whitespace",
          "filter": [ "lowercase" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-lowercase-tokenfilter-configure-parms]

`language`
:   (Optional, string) Language-specific lowercase token filter to use. Valid values include:

`greek`
:   Uses Lucene’s [GreekLowerCaseFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/el/GreekLowerCaseFilter.md)

`irish`
:   Uses Lucene’s [IrishLowerCaseFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/ga/IrishLowerCaseFilter.md)

`turkish`
:   Uses Lucene’s [TurkishLowerCaseFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/tr/TurkishLowerCaseFilter.md)

If not specified, defaults to Lucene’s [LowerCaseFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/core/LowerCaseFilter.md).



## Customize [analysis-lowercase-tokenfilter-customize]

To customize the `lowercase` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom `lowercase` filter for the Greek language:

```console
PUT custom_lowercase_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "greek_lowercase_example": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["greek_lowercase"]
        }
      },
      "filter": {
        "greek_lowercase": {
          "type": "lowercase",
          "language": "greek"
        }
      }
    }
  }
}
```


