---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-smartcn.html
sub:
  plugin-name: analysis-smartcn
---

# Smart Chinese analysis plugin [analysis-smartcn]

The Smart Chinese Analysis plugin integrates Lucene’s Smart Chinese analysis module into elasticsearch.

It provides an analyzer for Chinese or mixed Chinese-English text. This analyzer uses probabilistic knowledge to find the optimal word segmentation for Simplified Chinese text. The text is first broken into sentences, then each sentence is segmented into words.


## Installation [analysis-smartcn-install]

:::{include} _snippets/plugin-install.md
:::


## Removal [analysis-smartcn-remove]

:::{include} _snippets/plugin-remove.md
:::


## `smartcn` tokenizer and token filter [analysis-smartcn-tokenizer]

The plugin provides the `smartcn` analyzer, `smartcn_tokenizer` tokenizer, and `smartcn_stop` token filter which are not configurable.

::::{note}
The `smartcn_word` token filter and `smartcn_sentence` have been deprecated.
::::




