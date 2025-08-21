---
navigation_title: "Trim"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-trim-tokenfilter.html
---

# Trim token filter [analysis-trim-tokenfilter]


Removes leading and trailing whitespace from each token in a stream. While this can change the length of a token, the `trim` filter does *not* change a token’s offsets.

The `trim` filter uses Lucene’s [TrimFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/miscellaneous/TrimFilter.md).

::::{tip}
Many commonly used tokenizers, such as the [`standard`](/reference/text-analysis/analysis-standard-tokenizer.md) or [`whitespace`](/reference/text-analysis/analysis-whitespace-tokenizer.md) tokenizer, remove whitespace by default. When using these tokenizers, you don’t need to add a separate `trim` filter.

::::


## Example [analysis-trim-tokenfilter-analyze-ex]

To see how the `trim` filter works, you first need to produce a token containing whitespace.

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the [`keyword`](/reference/text-analysis/analysis-keyword-tokenizer.md) tokenizer to produce a token for `" fox "`.

```console
GET _analyze
{
  "tokenizer" : "keyword",
  "text" : " fox "
}
```

The API returns the following response. Note the `" fox "` token contains the original text’s whitespace. Note that despite changing the token’s length, the `start_offset` and `end_offset` remain the same.

```console-result
{
  "tokens": [
    {
      "token": " fox ",
      "start_offset": 0,
      "end_offset": 5,
      "type": "word",
      "position": 0
    }
  ]
}
```

To remove the whitespace, add the `trim` filter to the previous analyze API request.

```console
GET _analyze
{
  "tokenizer" : "keyword",
  "filter" : ["trim"],
  "text" : " fox "
}
```

The API returns the following response. The returned `fox` token does not include any leading or trailing whitespace.

```console-result
{
  "tokens": [
    {
      "token": "fox",
      "start_offset": 0,
      "end_offset": 5,
      "type": "word",
      "position": 0
    }
  ]
}
```


## Add to an analyzer [analysis-trim-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `trim` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT trim_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "keyword_trim": {
          "tokenizer": "keyword",
          "filter": [ "trim" ]
        }
      }
    }
  }
}
```


