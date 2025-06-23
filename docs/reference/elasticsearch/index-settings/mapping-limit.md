---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-settings-limit.html
navigation_title: Mapping limit
---

# Mapping limit settings [mapping-settings-limit]

Use the following settings to limit the number of field mappings (created manually or dynamically) and prevent documents from causing a mapping explosion:

`index.mapping.total_fields.limit`
:   The maximum number of fields in an index. Field and object mappings, as well as field aliases count towards this limit. Mapped runtime fields count towards this limit as well. The default value is `1000`.

    ::::{important}
    The limit is in place to prevent mappings and searches from becoming too large. Higher values can lead to performance degradations and memory issues, especially in clusters with a high load or few resources.

    If you increase this setting, we recommend you also increase the [`indices.query.bool.max_clause_count`](/reference/elasticsearch/configuration-reference/search-settings.md) setting, which limits the maximum number of clauses in a query.

    ::::


    ::::{tip}
    If your field mappings contain a large, arbitrary set of keys, consider using the [flattened](/reference/elasticsearch/mapping-reference/flattened.md) data type, or setting the index setting `index.mapping.total_fields.ignore_dynamic_beyond_limit` to `true`.

    ::::


`index.mapping.total_fields.ignore_dynamic_beyond_limit`
:   This setting determines what happens when a dynamically mapped field would exceed the total fields limit. When set to `false` (the default), the index request of the document that tries to add a dynamic field to the mapping will fail with the message `Limit of total fields [X] has been exceeded`. When set to `true`, the index request will not fail. Instead, fields that would exceed the limit are not added to the mapping, similar to [`dynamic: false`](/reference/elasticsearch/mapping-reference/dynamic.md). The fields that were not added to the mapping will be added to the [`_ignored` field](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md). The default value is `false`.

`index.mapping.depth.limit`
:   The maximum depth for a field, which is measured as the number of inner objects. For instance, if all fields are defined at the root object level, then the depth is `1`. If there is one object mapping, then the depth is `2`, etc. Default is `20`.

`index.mapping.nested_fields.limit`
:   The maximum number of distinct `nested` mappings in an index. The `nested` type should only be used in special cases, when arrays of objects need to be queried independently of each other. To safeguard against poorly designed mappings, this setting limits the number of unique `nested` types per index. Default is `50`.

`index.mapping.nested_objects.limit`
:   The maximum number of nested JSON objects that a single document can contain across all `nested` types. This limit helps to prevent out of memory errors when a document contains too many nested objects. Default is `10000`.

`index.mapping.field_name_length.limit`
:   Setting for the maximum length of a field name. This setting isn’t really something that addresses mappings explosion but might still be useful if you want to limit the field length. It usually shouldn’t be necessary to set this setting. The default is okay unless a user starts to add a huge number of fields with really long names. Default is `Long.MAX_VALUE` (no limit).

`index.mapping.dimension_fields.limit`
:   (Dynamic, integer) Maximum number of [time series dimensions](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension) for the index. Defaults to `32768`.

