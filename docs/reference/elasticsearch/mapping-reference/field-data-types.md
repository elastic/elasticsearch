---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
---

# Field data types [mapping-types]

Each field has a *field data type*, or *field type*. This type indicates the kind of data the field contains, such as strings or boolean values, and its intended use. For example, you can index strings to both `text` and `keyword` fields. However, `text` field values are [analyzed](docs-content://manage-data/data-store/text-analysis.md) for full-text search while `keyword` strings are left as-is for filtering and sorting.

Field types are grouped by *family*. Types in the same family have exactly the same search behavior but may have different space usage or performance characteristics.

Currently, there are two type families, `keyword` and `text`. Other type families have only a single field type. For example, the `boolean` type family consists of one field type: `boolean`.


### Common types [_core_datatypes]

[`binary`](/reference/elasticsearch/mapping-reference/binary.md)
:   Binary value encoded as a Base64 string.

[`boolean`](/reference/elasticsearch/mapping-reference/boolean.md)
:   `true` and `false` values.

[Keywords](/reference/elasticsearch/mapping-reference/keyword.md)
:   The keyword family, including `keyword`, `constant_keyword`, and `wildcard`.

[Numbers](/reference/elasticsearch/mapping-reference/number.md)
:   Numeric types, such as `long` and `double`, used to express amounts.

Dates
:   Date types, including [`date`](/reference/elasticsearch/mapping-reference/date.md) and [`date_nanos`](/reference/elasticsearch/mapping-reference/date_nanos.md).

[`alias`](/reference/elasticsearch/mapping-reference/field-alias.md)
:   Defines an alias for an existing field.


### Objects and relational types [object-types]

[`object`](/reference/elasticsearch/mapping-reference/object.md)
:   A JSON object.

[`flattened`](/reference/elasticsearch/mapping-reference/flattened.md)
:   An entire JSON object as a single field value.

[`nested`](/reference/elasticsearch/mapping-reference/nested.md)
:   A JSON object that preserves the relationship between its subfields.

[`join`](/reference/elasticsearch/mapping-reference/parent-join.md)
:   Defines a parent/child relationship for documents in the same index.

[`passthrough`](/reference/elasticsearch/mapping-reference/passthrough.md)
:   Provides aliases for sub-fields at the same level.


### Structured data types [structured-data-types]

[Range](/reference/elasticsearch/mapping-reference/range.md)
:   Range types, such as `long_range`, `double_range`, `date_range`, and `ip_range`.

[`ip`](/reference/elasticsearch/mapping-reference/ip.md)
:   IPv4 and IPv6 addresses.

[`version`](/reference/elasticsearch/mapping-reference/version.md)
:   Software versions. Supports [Semantic Versioning](https://semver.org/) precedence rules.

[`murmur3`](/reference/elasticsearch-plugins/mapper-murmur3.md)
:   Compute and stores hashes of values.


### Aggregate data types [aggregated-data-types]

[`aggregate_metric_double`](/reference/elasticsearch/mapping-reference/aggregate-metric-double.md)
:   Pre-aggregated metric values.

[`histogram`](/reference/elasticsearch/mapping-reference/histogram.md)
:   Pre-aggregated numerical values in the form of a histogram.


### Text search types [text-search-types]

[`text` fields](/reference/elasticsearch/mapping-reference/text.md)
:   The text family, including `text` and `match_only_text`. Analyzed, unstructured text.

[`annotated-text`](/reference/elasticsearch-plugins/mapper-annotated-text.md)
:   Text containing special markup. Used for identifying named entities.

[`completion`]
:   Used for auto-complete suggestions. For more information about completion suggesters, refer to [](/reference/elasticsearch/rest-apis/search-suggesters.md).

[`search_as_you_type`](/reference/elasticsearch/mapping-reference/search-as-you-type.md)
:   `text`-like type for as-you-type completion.

[`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md)
:   Used for performing [semantic search](docs-content://solutions/search/semantic-search.md).

[`token_count`](/reference/elasticsearch/mapping-reference/token-count.md)
:   A count of tokens in a text.


### Document ranking types [document-ranking-types]

[`dense_vector`](/reference/elasticsearch/mapping-reference/dense-vector.md)
:   Records dense vectors of float values.

[`sparse_vector`](/reference/elasticsearch/mapping-reference/sparse-vector.md)
:   Records sparse vectors of float values.

[`rank_feature`](/reference/elasticsearch/mapping-reference/rank-feature.md)
:   Records a numeric feature to boost hits at query time.

[`rank_features`](/reference/elasticsearch/mapping-reference/rank-features.md)
:   Records numeric features to boost hits at query time.


### Spatial data types [spatial_datatypes]

[`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md)
:   Latitude and longitude points.

[`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md)
:   Complex shapes, such as polygons.

[`point`](/reference/elasticsearch/mapping-reference/point.md)
:   Arbitrary cartesian points.

[`shape`](/reference/elasticsearch/mapping-reference/shape.md)
:   Arbitrary cartesian geometries.


### Other types [other-types]

[`percolator`](/reference/elasticsearch/mapping-reference/percolator.md)
:   Indexes queries written in [Query DSL](/reference/query-languages/querydsl.md).


## Arrays [types-array-handling]

In {{es}}, arrays do not require a dedicated field data type. Any field can contain zero or more values by default, however, all values in the array must be of the same field type. See [Arrays](/reference/elasticsearch/mapping-reference/array.md).


## Multi-fields [types-multi-fields]

It is often useful to index the same field in different ways for different purposes. For instance, a `string` field could be mapped as a `text` field for full-text search, and as a `keyword` field for sorting or aggregations. Alternatively, you could index a text field with the [`standard` analyzer](/reference/text-analysis/analysis-standard-analyzer.md), the [`english`](/reference/text-analysis/analysis-lang-analyzer.md#english-analyzer) analyzer, and the [`french` analyzer](/reference/text-analysis/analysis-lang-analyzer.md#french-analyzer).

This is the purpose of *multi-fields*. Most field types support multi-fields via the [`fields`](/reference/elasticsearch/mapping-reference/multi-fields.md) parameter.



































