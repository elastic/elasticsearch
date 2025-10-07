---
navigation_title: "Text type family"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/text-type-family.html
---

# Text type family [text]


The text family includes the following field types:

* [`text`](/reference/elasticsearch/mapping-reference/text.md), the traditional field type for full-text content such as the body of an email or the description of a product.
* [`match_only_text`](/reference/elasticsearch/mapping-reference/match-only-text.md), a space-optimized variant of `text` that disables scoring and performs slower on queries that need positions. It is best suited for indexing log messages.
* [`pattern_text`](/reference/elasticsearch/mapping-reference/pattern-text.md), a variant of `text` with improved space efficiency when storing log messages.

