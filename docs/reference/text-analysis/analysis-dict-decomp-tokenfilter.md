---
navigation_title: "Dictionary decompounder"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-dict-decomp-tokenfilter.html
---

# Dictionary decompounder token filter [analysis-dict-decomp-tokenfilter]


::::{note}
In most cases, we recommend using the faster [`hyphenation_decompounder`](/reference/data-analysis/text-analysis/analysis-hyp-decomp-tokenfilter.md) token filter in place of this filter. However, you can use the `dictionary_decompounder` filter to check the quality of a word list before implementing it in the `hyphenation_decompounder` filter.

::::


Uses a specified list of words and a brute force approach to find subwords in compound words. If found, these subwords are included in the token output.

This filter uses Lucene’s [DictionaryCompoundWordTokenFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/compound/DictionaryCompoundWordTokenFilter.md), which was built for Germanic languages.

## Example [analysis-dict-decomp-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `dictionary_decompounder` filter to find subwords in `Donaudampfschiff`. The filter then checks these subwords against the specified list of words: `Donau`, `dampf`, `meer`, and `schiff`.

```console
GET _analyze
{
  "tokenizer": "standard",
  "filter": [
    {
      "type": "dictionary_decompounder",
      "word_list": ["Donau", "dampf", "meer", "schiff"]
    }
  ],
  "text": "Donaudampfschiff"
}
```

The filter produces the following tokens:

```text
[ Donaudampfschiff, Donau, dampf, schiff ]
```


## Configurable parameters [analysis-dict-decomp-tokenfilter-configure-parms]

`word_list`
:   (Required*, array of strings) A list of subwords to look for in the token stream. If found, the subword is included in the token output.

Either this parameter or `word_list_path` must be specified.


`word_list_path`
:   (Required*, string) Path to a file that contains a list of subwords to find in the token stream. If found, the subword is included in the token output.

This path must be absolute or relative to the `config` location, and the file must be UTF-8 encoded. Each token in the file must be separated by a line break.

Either this parameter or `word_list` must be specified.


`max_subword_size`
:   (Optional, integer) Maximum subword character length. Longer subword tokens are excluded from the output. Defaults to `15`.

`min_subword_size`
:   (Optional, integer) Minimum subword character length. Shorter subword tokens are excluded from the output. Defaults to `2`.

`min_word_size`
:   (Optional, integer) Minimum word character length. Shorter word tokens are excluded from the output. Defaults to `5`.

`only_longest_match`
:   (Optional, Boolean) If `true`, only include the longest matching subword. Defaults to `false`.


## Customize and add to an analyzer [analysis-dict-decomp-tokenfilter-customize]

To customize the `dictionary_decompounder` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses a custom `dictionary_decompounder` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

The custom `dictionary_decompounder` filter find subwords in the `analysis/example_word_list.txt` file. Subwords longer than 22 characters are excluded from the token output.

```console
PUT dictionary_decompound_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_dictionary_decompound": {
          "tokenizer": "standard",
          "filter": [ "22_char_dictionary_decompound" ]
        }
      },
      "filter": {
        "22_char_dictionary_decompound": {
          "type": "dictionary_decompounder",
          "word_list_path": "analysis/example_word_list.txt",
          "max_subword_size": 22
        }
      }
    }
  }
}
```


