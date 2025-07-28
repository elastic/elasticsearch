---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-fields.html
---

# Document metadata fields [mapping-fields]

Each document has metadata associated with it, such as the `_index` and `_id` metadata fields. The behavior of some of these metadata fields can be customized when a mapping is created.


## Identity metadata fields [_identity_metadata_fields]

[`_index`](/reference/elasticsearch/mapping-reference/mapping-index-field.md)
:   The index to which the document belongs.

[`_id`](/reference/elasticsearch/mapping-reference/mapping-id-field.md)
:   The document’s ID.


## Document source metadata fields [_document_source_metadata_fields]

[`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md)
:   The original JSON representing the body of the document.

[`_size`](/reference/elasticsearch-plugins/mapper-size.md)
:   The size of the `_source` field in bytes, provided by the [`mapper-size` plugin](/reference/elasticsearch-plugins/mapper-size.md).


## Doc count metadata field [_doc_count_metadata_field]

[`_doc_count`](/reference/elasticsearch/mapping-reference/mapping-doc-count-field.md)
:   A custom field used for storing doc counts when a document represents pre-aggregated data.


## Indexing metadata fields [_indexing_metadata_fields]

[`_field_names`](/reference/elasticsearch/mapping-reference/mapping-field-names-field.md)
:   All fields in the document which contain non-null values.

[`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md)
:   All fields in the document that have been ignored at index time because of [`ignore_malformed`](/reference/elasticsearch/mapping-reference/ignore-malformed.md).


## Routing metadata field [_routing_metadata_field]

[`_routing`](/reference/elasticsearch/mapping-reference/mapping-routing-field.md)
:   A custom routing value which routes a document to a particular shard.


## Other metadata field [_other_metadata_field]

[`_meta`](/reference/elasticsearch/mapping-reference/mapping-meta-field.md)
:   Application specific metadata.

[`_tier`](/reference/elasticsearch/mapping-reference/mapping-tier-field.md)
:   The current data tier preference of the index to which the document belongs.










