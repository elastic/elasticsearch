---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu-analyzer.html
---

# ICU analyzer [analysis-icu-analyzer]

The `icu_analyzer` analyzer performs basic normalization, tokenization and character folding, using the `icu_normalizer` char filter, `icu_tokenizer` and `icu_folding` token filter

The following parameters are accepted:

`method`
:   Normalization method. Accepts `nfkc`, `nfc` or `nfkc_cf` (default)

`mode`
:   Normalization mode. Accepts `compose` (default) or `decompose`.

