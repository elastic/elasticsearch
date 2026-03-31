---
navigation_title: "Keep types"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-keep-types-tokenfilter.html
---

# Keep types token filter [analysis-keep-types-tokenfilter]


Keeps or removes tokens of a specific type. For example, you can use this filter to change `3 quick foxes` to `quick foxes` by keeping only `<ALPHANUM>` (alphanumeric) tokens.

::::{admonition} Token types
:class: note

Token types are set by the [tokenizer](/reference/text-analysis/tokenizer-reference.md) when converting characters to tokens. Token types can vary between tokenizers.

For example, the [`standard`](/reference/text-analysis/analysis-standard-tokenizer.md) tokenizer can produce a variety of token types, including `<ALPHANUM>`, `<HANGUL>`, and `<NUM>`. Simpler analyzers, like the [`lowercase`](/reference/text-analysis/analysis-lowercase-tokenizer.md) tokenizer, only produce the `word` token type.

Certain token filters can also add token types. For example, the [`synonym`](/reference/text-analysis/analysis-synonym-tokenfilter.md) filter can add the `<SYNONYM>` token type.

Some tokenizers don’t support this token filter, for example keyword, simple_pattern, and simple_pattern_split tokenizers, as they don’t support setting the token type attribute.

::::


This filter uses Lucene’s [TypeTokenFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/core/TypeTokenFilter.md).

## Include example [analysis-keep-types-tokenfilter-analyze-include-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `keep_types` filter to keep only `<NUM>` (numeric) tokens from `1 quick fox 2 lazy dogs`.

```console
GET _analyze
{
  "tokenizer": "standard",
  "filter": [
    {
      "type": "keep_types",
      "types": [ "<NUM>" ]
    }
  ],
  "text": "1 quick fox 2 lazy dogs"
}
```

The filter produces the following tokens:

```text
[ 1, 2 ]
```


## Exclude example [analysis-keep-types-tokenfilter-analyze-exclude-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `keep_types` filter to remove `<NUM>` tokens from `1 quick fox 2 lazy dogs`. Note the `mode` parameter is set to `exclude`.

```console
GET _analyze
{
  "tokenizer": "standard",
  "filter": [
    {
      "type": "keep_types",
      "types": [ "<NUM>" ],
      "mode": "exclude"
    }
  ],
  "text": "1 quick fox 2 lazy dogs"
}
```

The filter produces the following tokens:

```text
[ quick, fox, lazy, dogs ]
```


## Configurable parameters [analysis-keep-types-tokenfilter-configure-parms]

`types`
:   (Required, array of strings) List of token types to keep or remove.

`mode`
:   (Optional, string) Indicates whether to keep or remove the specified token types. Valid values are:

    `include`
    :   (Default) Keep only the specified token types.

    `exclude`
    :   Remove the specified token types.



## Customize and add to an analyzer [analysis-keep-types-tokenfilter-customize]

To customize the `keep_types` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses a custom `keep_types` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md). The custom `keep_types` filter keeps only `<ALPHANUM>` (alphanumeric) tokens.

```console
PUT keep_types_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "filter": [ "extract_alpha" ]
        }
      },
      "filter": {
        "extract_alpha": {
          "type": "keep_types",
          "types": [ "<ALPHANUM>" ]
        }
      }
    }
  }
}
```


