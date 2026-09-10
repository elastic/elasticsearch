---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-murmur3.html
sub:
  plugin-name: mapper-murmur3
---

# Mapper murmur3 plugin [mapper-murmur3]

The mapper-murmur3 plugin provides the ability to compute hash of field values at index-time and store them in the index. This can sometimes be helpful when running cardinality aggregations on high-cardinality and large string fields.


## Installation [mapper-murmur3-install]

:::{include} _snippets/plugin-install.md
:::


## Removal [mapper-murmur3-remove]

:::{include} _snippets/plugin-remove.md
:::


