---
navigation_title: "Shingle"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-shingle-tokenfilter.html
---

# Shingle token filter [analysis-shingle-tokenfilter]


Add shingles, or word [n-grams](https://en.wikipedia.org/wiki/N-gram), to a token stream by concatenating adjacent tokens. By default, the `shingle` token filter outputs two-word shingles and unigrams.

For example, many tokenizers convert `the lazy dog` to `[ the, lazy, dog ]`. You can use the `shingle` filter to add two-word shingles to this stream: `[ the, the lazy, lazy, lazy dog, dog ]`.

::::{tip}
Shingles are often used to help speed up phrase queries, such as [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md). Rather than creating shingles using the `shingles` filter, we recommend you use the [`index-phrases`](/reference/elasticsearch/mapping-reference/index-phrases.md) mapping parameter on the appropriate [text](/reference/elasticsearch/mapping-reference/text.md) field instead.
::::


This filter uses Lucene’s [ShingleFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/shingle/ShingleFilter.html).

## Example [analysis-shingle-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `shingle` filter to add two-word shingles to the token stream for `quick brown fox jumps`:

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [ "shingle" ],
  "text": "quick brown fox jumps"
}
```

The filter produces the following tokens:

```text
[ quick, quick brown, brown, brown fox, fox, fox jumps, jumps ]
```

To produce shingles of 2-3 words, add the following arguments to the analyze API request:

* `min_shingle_size`: `2`
* `max_shingle_size`: `3`

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [
    {
      "type": "shingle",
      "min_shingle_size": 2,
      "max_shingle_size": 3
    }
  ],
  "text": "quick brown fox jumps"
}
```

The filter produces the following tokens:

```text
[ quick, quick brown, quick brown fox, brown, brown fox, brown fox jumps, fox, fox jumps, jumps ]
```

To only include shingles in the output, add an `output_unigrams` argument of `false` to the request.

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [
    {
      "type": "shingle",
      "min_shingle_size": 2,
      "max_shingle_size": 3,
      "output_unigrams": false
    }
  ],
  "text": "quick brown fox jumps"
}
```

The filter produces the following tokens:

```text
[ quick brown, quick brown fox, brown fox, brown fox jumps, fox jumps ]
```


## Add to an analyzer [analysis-shingle-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `shingle` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_shingle": {
          "tokenizer": "standard",
          "filter": [ "shingle" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-shingle-tokenfilter-configure-parms]

`max_shingle_size`
:   (Optional, integer) Maximum number of tokens to concatenate when creating shingles. Defaults to `2`.

    ::::{note}
    This value cannot be lower than the `min_shingle_size` argument, which defaults to `2`. The difference between this value and the `min_shingle_size` argument cannot exceed the [`index.max_shingle_diff`](/reference/elasticsearch/index-settings/index-modules.md#index-max-shingle-diff) index-level setting, which defaults to `3`.
    ::::


`min_shingle_size`
:   (Optional, integer) Minimum number of tokens to concatenate when creating shingles. Defaults to `2`.

    ::::{note}
    This value cannot exceed the `max_shingle_size` argument, which defaults to `2`. The difference between the `max_shingle_size` argument and this value cannot exceed the [`index.max_shingle_diff`](/reference/elasticsearch/index-settings/index-modules.md#index-max-shingle-diff) index-level setting, which defaults to `3`.
    ::::


`output_unigrams`
:   (Optional, Boolean) If `true`, the output includes the original input tokens. If `false`, the output only includes shingles; the original input tokens are removed. Defaults to `true`.

`output_unigrams_if_no_shingles`
:   If `true`, the output includes the original input tokens only if no shingles are produced; if shingles are produced, the output only includes shingles. Defaults to `false`.

    ::::{important}
    If both this and the `output_unigrams` parameter are `true`, only the `output_unigrams` argument is used.
    ::::


`token_separator`
:   (Optional, string) Separator used to concatenate adjacent tokens to form a shingle. Defaults to a space (`" "`).

`filler_token`
:   (Optional, string) String used in shingles as a replacement for empty positions that do not contain a token. This filler token is only used in shingles, not original unigrams. Defaults to an underscore (`_`).

Some token filters, such as the `stop` filter, create empty positions when removing stop words with a position increment greater than one.

::::{dropdown} Example
In the following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request, the `stop` filter removes the stop word `a` from `fox jumps a lazy dog`, creating an empty position. The subsequent `shingle` filter replaces this empty position with a plus sign (`+`) in shingles.

```console
GET /_analyze
{
  "tokenizer": "whitespace",
  "filter": [
    {
      "type": "stop",
      "stopwords": [ "a" ]
    },
    {
      "type": "shingle",
      "filler_token": "+"
    }
  ],
  "text": "fox jumps a lazy dog"
}
```

The filter produces the following tokens:

```text
[ fox, fox jumps, jumps, jumps +, + lazy, lazy, lazy dog, dog ]
```

::::




## Customize [analysis-shingle-tokenfilter-customize]

To customize the `shingle` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses a custom `shingle` filter, `my_shingle_filter`, to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

The `my_shingle_filter` filter uses a `min_shingle_size` of `2` and a `max_shingle_size` of `5`, meaning it produces shingles of 2-5 words. The filter also includes a `output_unigrams` argument of `false`, meaning that only shingles are included in the output.

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "en": {
          "tokenizer": "standard",
          "filter": [ "my_shingle_filter" ]
        }
      },
      "filter": {
        "my_shingle_filter": {
          "type": "shingle",
          "min_shingle_size": 2,
          "max_shingle_size": 5,
          "output_unigrams": false
        }
      }
    }
  }
}
```


