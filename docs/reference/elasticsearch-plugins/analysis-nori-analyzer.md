---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-nori-analyzer.html
---

# nori analyzer [analysis-nori-analyzer]

The `nori` analyzer consists of the following tokenizer and token filters:

* [`nori_tokenizer`](/reference/elasticsearch-plugins/analysis-nori-tokenizer.md)
* [`nori_part_of_speech`](/reference/elasticsearch-plugins/analysis-nori-speech.md) token filter
* [`nori_readingform`](/reference/elasticsearch-plugins/analysis-nori-readingform.md) token filter
* [`lowercase`](/reference/text-analysis/analysis-lowercase-tokenfilter.md) token filter

It supports the `decompound_mode` and `user_dictionary` settings from [`nori_tokenizer`](/reference/elasticsearch-plugins/analysis-nori-tokenizer.md) and the `stoptags` setting from [`nori_part_of_speech`](/reference/elasticsearch-plugins/analysis-nori-speech.md).

