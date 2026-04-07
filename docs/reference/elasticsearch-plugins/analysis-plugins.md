---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis.html
---

# Analysis plugins [analysis]

Analysis plugins extend Elasticsearch by adding new analyzers, tokenizers, token filters, or character filters to Elasticsearch.


## Core analysis plugins [_core_analysis_plugins]

The core analysis plugins are:

[ICU](/reference/elasticsearch-plugins/analysis-icu.md)
:   Adds extended Unicode support using the [ICU](http://site.icu-project.org/) libraries, including better analysis of Asian languages, Unicode normalization, Unicode-aware case folding, collation support, and transliteration.

[Kuromoji](/reference/elasticsearch-plugins/analysis-kuromoji.md)
:   Advanced analysis of Japanese using the [Kuromoji analyzer](https://www.atilika.org/).

[Nori](/reference/elasticsearch-plugins/analysis-nori.md)
:   Morphological analysis of Korean using the Lucene Nori analyzer.

[Phonetic](/reference/elasticsearch-plugins/analysis-phonetic.md)
:   Analyzes tokens into their phonetic equivalent using Soundex, Metaphone, Caverphone, and other codecs.

[SmartCN](/reference/elasticsearch-plugins/analysis-smartcn.md)
:   An analyzer for Chinese or mixed Chinese-English text. This analyzer uses probabilistic knowledge to find the optimal word segmentation for Simplified Chinese text. The text is first broken into sentences, then each sentence is segmented into words.

[Stempel](/reference/elasticsearch-plugins/analysis-stempel.md)
:   Provides high quality stemming for Polish.

[Ukrainian](/reference/elasticsearch-plugins/analysis-ukrainian.md)
:   Provides stemming for Ukrainian.


## Community contributed analysis plugins [_community_contributed_analysis_plugins]

A number of analysis plugins have been contributed by our community:

* [IK Analysis Plugin](https://github.com/medcl/elasticsearch-analysis-ik) (by Medcl)
* [Pinyin Analysis Plugin](https://github.com/medcl/elasticsearch-analysis-pinyin) (by Medcl)
* [Vietnamese Analysis Plugin](https://github.com/duydo/elasticsearch-analysis-vietnamese) (by Duy Do)
* [STConvert Analysis Plugin](https://github.com/medcl/elasticsearch-analysis-stconvert) (by Medcl)








