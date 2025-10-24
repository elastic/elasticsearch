---
navigation_title: "Hyphenation decompounder"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-hyp-decomp-tokenfilter.html
---

# Hyphenation decompounder token filter [analysis-hyp-decomp-tokenfilter]


Uses XML-based hyphenation patterns to find potential subwords in compound words. These subwords are then checked against the specified word list. Subwords not in the list are excluded from the token output.

This filter uses Lucene’s [HyphenationCompoundWordTokenFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/compound/HyphenationCompoundWordTokenFilter.md), which was built for Germanic languages.

## Example [analysis-hyp-decomp-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `hyphenation_decompounder` filter to find subwords in `Kaffeetasse` based on German hyphenation patterns in the `analysis/hyphenation_patterns.xml` file. The filter then checks these subwords against a list of specified words: `kaffee`, `zucker`, and `tasse`.

```console
GET _analyze
{
  "tokenizer": "standard",
  "filter": [
    {
      "type": "hyphenation_decompounder",
      "hyphenation_patterns_path": "analysis/hyphenation_patterns.xml",
      "word_list": ["Kaffee", "zucker", "tasse"]
    }
  ],
  "text": "Kaffeetasse"
}
```
% TEST[skip: requires a valid hyphenation_patterns.xml file for DE-DR]

The filter produces the following tokens:

```text
[ Kaffeetasse, Kaffee, tasse ]
```


## Configurable parameters [analysis-hyp-decomp-tokenfilter-configure-parms]

`hyphenation_patterns_path`
:   (Required, string) Path to an Apache FOP (Formatting Objects Processor) XML hyphenation pattern file.

This path must be absolute or relative to the `config` location. Only FOP v1.2 compatible files are supported.

For example FOP XML hyphenation pattern files, refer to:

* [Objects For Formatting Objects (OFFO) Sourceforge project](http://offo.sourceforge.net/#FOP+XML+Hyphenation+Patterns)
* [offo-hyphenation_v1.2.zip direct download](https://sourceforge.net/projects/offo/files/offo-hyphenation/1.2/offo-hyphenation_v1.2.zip/download) (v2.0 and above hyphenation pattern files are not supported)


`word_list`
:   (Required*, array of strings) A list of subwords. Subwords found using the hyphenation pattern but not in this list are excluded from the token output.

You can use the [`dictionary_decompounder`](/reference/text-analysis/analysis-dict-decomp-tokenfilter.md) filter to test the quality of word lists before implementing them.

Either this parameter or `word_list_path` must be specified.


`word_list_path`
:   (Required*, string) Path to a file containing a list of subwords. Subwords found using the hyphenation pattern but not in this list are excluded from the token output.

This path must be absolute or relative to the `config` location, and the file must be UTF-8 encoded. Each token in the file must be separated by a line break.

You can use the [`dictionary_decompounder`](/reference/text-analysis/analysis-dict-decomp-tokenfilter.md) filter to test the quality of word lists before implementing them.

Either this parameter or `word_list` must be specified.


`max_subword_size`
:   (Optional, integer) Maximum subword character length. Longer subword tokens are excluded from the output. Defaults to `15`.

`min_subword_size`
:   (Optional, integer) Minimum subword character length. Shorter subword tokens are excluded from the output. Defaults to `2`.

`min_word_size`
:   (Optional, integer) Minimum word character length. Shorter word tokens are excluded from the output. Defaults to `5`.

`only_longest_match`
:   (Optional, Boolean) If `true`, only include the longest matching subword. Defaults to `false`.

`no_sub_matches`
:   (Optional, Boolean) If `true`, do not match sub tokens in tokens that are in the word list. Defaults to `false`.

`no_overlapping_matches`
:   (Optional, Boolean) If `true`, do not allow overlapping tokens. Defaults to `false`.

Typically users will only want to include one of the three flags as enabling `no_overlapping_matches` is the most restrictive and `no_sub_matches` is more restrictive than `only_longest_match`. When enabling a more restrictive option the state of the less restrictive does not have any effect.


## Customize and add to an analyzer [analysis-hyp-decomp-tokenfilter-customize]

To customize the `hyphenation_decompounder` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses a custom `hyphenation_decompounder` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

The custom `hyphenation_decompounder` filter find subwords based on hyphenation patterns in the `analysis/hyphenation_patterns.xml` file. The filter then checks these subwords against the list of words specified in the `analysis/example_word_list.txt` file. Subwords longer than 22 characters are excluded from the token output.

```console
PUT hyphenation_decompound_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "standard_hyphenation_decompound": {
          "tokenizer": "standard",
          "filter": [ "22_char_hyphenation_decompound" ]
        }
      },
      "filter": {
        "22_char_hyphenation_decompound": {
          "type": "hyphenation_decompounder",
          "word_list_path": "analysis/example_word_list.txt",
          "hyphenation_patterns_path": "analysis/hyphenation_patterns.xml",
          "max_subword_size": 22
        }
      }
    }
  }
}
```


