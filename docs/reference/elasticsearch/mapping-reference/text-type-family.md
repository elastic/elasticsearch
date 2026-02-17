---
applies_to:
  stack:
  serverless:
navigation_title: "Text type family"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/text-type-family.html
---

# Text type family [text]


The text family includes the following field types:

* [`text`](/reference/elasticsearch/mapping-reference/text.md), the traditional field type for full-text content such as the body of an email or the description of a product.
* [`match_only_text`](/reference/elasticsearch/mapping-reference/match-only-text.md), a variant of `text` field type with limited functionality. Scoring is always disabled and the `standard` analyzer is always used. It suited for match only free text uses cases. Meaning that the fact that there is a match is important, but scoring and where the match happens is not relevant. Note that positional queries are possible, but are slow.
* [`pattern_text`](/reference/elasticsearch/mapping-reference/pattern-text.md), a variant of `text` which is optimized for space efficient storage of log messages. Pattern text reduces space usage for messages that contain many repeated sequences, like the explanatory text of a log message. Pattern text also disables scoring, but unlike `match_only_text`, positional data can be stored for fast phrase queries.

