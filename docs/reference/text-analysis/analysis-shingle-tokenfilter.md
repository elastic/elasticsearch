---
navigation_title: "Shingle"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-shingle-tokenfilter.html
applies_to:
  stack: ga
  serverless: ga
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

To set these parameters, [create a custom `shingle` filter](#analysis-shingle-tokenfilter-customize).

### Shingle sizes and unigrams [analysis-shingle-tokenfilter-size-limits]

`min_shingle_size`
:   (Optional, integer) Minimum number of tokens in a shingle. Defaults to `2`.

`max_shingle_size`
:   (Optional, integer) Maximum number of tokens in a shingle. Defaults to `2`. Must be at least `min_shingle_size`.

`output_unigrams`
:   (Optional, Boolean) Whether to include the original input tokens alongside the shingles. Defaults to `true`.

`output_unigrams_if_no_shingles`
:   (Optional, Boolean) Whether to output the original input tokens when the filter produces no shingles. Applies when `output_unigrams` is `false`. Defaults to `false`.

The filter subtracts `min_shingle_size` from `max_shingle_size` and adds `1` if `output_unigrams` is `true`. The result must be at most the size limit, which defaults to `3`. With that default, the largest size difference is `2` with unigrams and `3` without them.

{applies_to}`serverless: unavailable` To change the limit, set [`index.max_shingle_diff`](/reference/elasticsearch/index-settings/index-modules.md#index-max-shingle-diff).

### Token separator and filler

`token_separator`
:   (Optional, string) Separator between tokens in a shingle. Defaults to a space (`" "`).

`filler_token`
:   (Optional, string) String used to fill gaps when building shingles. A position increment greater than `1` indicates a gap, which can occur when the `stop` filter removes a stop word. The filler appears only in shingles. Defaults to an underscore (`_`).

::::{dropdown} Filler token example
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

The filter produces shingles of two to five tokens. It sets `output_unigrams` to `false`, so the size difference of `3` fits the default limit.

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


