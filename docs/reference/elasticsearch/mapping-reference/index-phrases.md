---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-phrases.html
---

# index_phrases [index-phrases]

If enabled, two-term word combinations (*shingles*) are indexed into a separate field. This allows exact phrase queries (no slop) to run more efficiently, at the expense of a larger index. Note that this works best when stopwords are not removed, as phrases containing stopwords will not use the subsidiary field and will fall back to a standard phrase query. Accepts `true` or `false` (default).

