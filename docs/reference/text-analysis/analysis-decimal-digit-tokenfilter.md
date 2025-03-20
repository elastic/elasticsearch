---
navigation_title: "Decimal digit"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-decimal-digit-tokenfilter.html
---

# Decimal digit token filter [analysis-decimal-digit-tokenfilter]


Converts all digits in the Unicode `Decimal_Number` General Category to `0-9`. For example, the filter changes the Bengali numeral `৩` to `3`.

This filter uses Lucene’s [DecimalDigitFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/core/DecimalDigitFilter.md).

## Example [analysis-decimal-digit-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `decimal_digit` filter to convert Devanagari numerals to `0-9`:

```console
GET /_analyze
{
  "tokenizer" : "whitespace",
  "filter" : ["decimal_digit"],
  "text" : "१-one two-२ ३"
}
```

The filter produces the following tokens:

```text
[ 1-one, two-2, 3]
```


## Add to an analyzer [analysis-decimal-digit-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `decimal_digit` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /decimal_digit_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "whitespace_decimal_digit": {
          "tokenizer": "whitespace",
          "filter": [ "decimal_digit" ]
        }
      }
    }
  }
}
```


