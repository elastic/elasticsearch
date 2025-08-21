---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/index.html
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-guide.html
products:
  - id: painless
---

# Painless [painless-guide]

*Painless* is a simple, secure scripting language designed specifically for use with Elasticsearch. It is the default scripting language for Elasticsearch and can safely be used for inline and stored scripts. For a jump start into Painless, see [A Brief Painless Walkthrough](/reference/scripting-languages/painless/brief-painless-walkthrough.md). For a detailed description of the Painless syntax and language features, see the [Painless Language Specification](/reference/scripting-languages/painless/painless-language-specification.md).

You can use Painless anywhere scripts are used in Elasticsearch. Painless provides:

* Fast performance: Painless scripts [ run several times faster](https://benchmarks.elastic.co/index.md#search_qps_scripts) than the alternatives.
* Safety: Fine-grained allowlist with method call/field granularity.
* Optional typing: Variables and parameters can use explicit types or the dynamic `def` type.
* Syntax: Extends a subset of Java’s syntax to provide additional scripting language features.
* Optimizations: Designed specifically for Elasticsearch scripting.








