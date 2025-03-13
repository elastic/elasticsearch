---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper.html
---

# Mapper plugins [mapper]

Mapper plugins allow new field data types to be added to Elasticsearch.


## Core mapper plugins [_core_mapper_plugins]

The core mapper plugins are:

[Mapper size plugin](/reference/elasticsearch-plugins/mapper-size.md)
:   The mapper-size plugin provides the `_size` metadata field which, when enabled, indexes the size in bytes of the original [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field.

[Mapper murmur3 plugin](/reference/elasticsearch-plugins/mapper-murmur3.md)
:   The mapper-murmur3 plugin allows hashes to be computed at index-time and stored in the index for later use with the `cardinality` aggregation.

[Mapper annotated text plugin](/reference/elasticsearch-plugins/mapper-annotated-text.md)
:   The annotated text plugin provides the ability to index text that is a combination of free-text and special markup that is typically used to identify items of interest such as people or organisations (see NER or Named Entity Recognition tools).




