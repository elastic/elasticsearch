---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-analyzers.html
---

# Analyzer reference [analysis-analyzers]

Elasticsearch ships with a wide range of built-in analyzers, which can be used in any index without further configuration:

[Standard Analyzer](/reference/text-analysis/analysis-standard-analyzer.md)
:   The `standard` analyzer divides text into terms on word boundaries, as defined by the Unicode Text Segmentation algorithm. It removes most punctuation, lowercases terms, and supports removing stop words.

[Simple Analyzer](/reference/text-analysis/analysis-simple-analyzer.md)
:   The `simple` analyzer divides text into terms whenever it encounters a character which is not a letter. It lowercases all terms.

[Whitespace Analyzer](/reference/text-analysis/analysis-whitespace-analyzer.md)
:   The `whitespace` analyzer divides text into terms whenever it encounters any whitespace character. It does not lowercase terms.

[Stop Analyzer](/reference/text-analysis/analysis-stop-analyzer.md)
:   The `stop` analyzer is like the `simple` analyzer, but also supports removal of stop words.

[Keyword Analyzer](/reference/text-analysis/analysis-keyword-analyzer.md)
:   The `keyword` analyzer is a noop analyzer that accepts whatever text it is given and outputs the exact same text as a single term.

[Pattern Analyzer](/reference/text-analysis/analysis-pattern-analyzer.md)
:   The `pattern` analyzer uses a regular expression to split the text into terms. It supports lower-casing and stop words.

[Language Analyzers](/reference/text-analysis/analysis-lang-analyzer.md)
:   Elasticsearch provides many language-specific analyzers like `english` or `french`.

[Fingerprint Analyzer](/reference/text-analysis/analysis-fingerprint-analyzer.md)
:   The `fingerprint` analyzer is a specialist analyzer which creates a fingerprint which can be used for duplicate detection.


## Custom analyzers [_custom_analyzers]

If you do not find an analyzer suitable for your needs, you can create a [`custom`](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md) analyzer which combines the appropriate [character filters](/reference/text-analysis/character-filter-reference.md), [tokenizer](/reference/text-analysis/tokenizer-reference.md), and [token filters](/reference/text-analysis/token-filter-reference.md).









