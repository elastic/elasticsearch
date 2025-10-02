---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/source-index-settings.html
navigation_title: Source settings
---

# Source index settings [source-index-settings]

All settings around the _source metadata field.

$$$source-mode$$$

`index.source.mode`
: (Static, string) The source mode for the index. Valid values are [`synthetic`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source), [`disabled`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#disable-source-field) or `stored`. Defaults to `stored`. The `stored` source mode always stores the source metadata field on disk.

$$$recovery-use_synthetic_source$$$

`index.recovery.use_synthetic_source`
: (Static, boolean) If synthetic source mode is used, whether the recovery source should also be synthesized instead of stored to disk. Defaults to `true`. This setting can only be configured if synthetic source mode is enabled.

$$$synthetic-source-keep$$$

`index.mapping.synthetic_source_keep`
: (Static, string) Controls how to retain accuracy of fields at the index level. Valid values are `none` or `arrays`.This is a subset of [synthetic source keep mapping attribute](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source-keep). Defaults to `arrays` if `index.mode` is `logsdb` or otherwise `none`.
