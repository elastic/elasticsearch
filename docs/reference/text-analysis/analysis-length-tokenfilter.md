---
navigation_title: "Length"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-length-tokenfilter.html
---

# Length token filter [analysis-length-tokenfilter]


Removes tokens shorter or longer than specified character lengths. For example, you can use the `length` filter to exclude tokens shorter than 2 characters and tokens longer than 5 characters.

This filter uses Lucene’s [LengthFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/miscellaneous/LengthFilter.md).

::::{tip}
The `length` filter removes entire tokens. If you’d prefer to shorten tokens to a specific length, use the [`truncate`](/reference/text-analysis/analysis-truncate-tokenfilter.md) filter.

::::


## Example [analysis-length-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `length` filter to remove tokens longer than 4 characters:

```console
GET _analyze
{
  "tokenizer": "whitespace",
  "filter": [
    {
      "type": "length",
      "min": 0,
      "max": 4
    }
  ],
  "text": "the quick brown fox jumps over the lazy dog"
}
```

The filter produces the following tokens:

```text
[ the, fox, over, the, lazy, dog ]
```


## Add to an analyzer [analysis-length-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `length` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT length_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_length": {
          "tokenizer": "standard",
          "filter": [ "length" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-length-tokenfilter-configure-parms]

`min`
:   (Optional, integer) Minimum character length of a token. Shorter tokens are excluded from the output. Defaults to `0`.

`max`
:   (Optional, integer) Maximum character length of a token. Longer tokens are excluded from the output. Defaults to `Integer.MAX_VALUE`, which is `2^31 - 1` or `2147483647`.


## Customize [analysis-length-tokenfilter-customize]

To customize the `length` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom `length` filter that removes tokens shorter than 2 characters and tokens longer than 10 characters:

```console
PUT length_custom_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "whitespace_length_2_to_10_char": {
          "tokenizer": "whitespace",
          "filter": [ "length_2_to_10_char" ]
        }
      },
      "filter": {
        "length_2_to_10_char": {
          "type": "length",
          "min": 2,
          "max": 10
        }
      }
    }
  }
}
```


