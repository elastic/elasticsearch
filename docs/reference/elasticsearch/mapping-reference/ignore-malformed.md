---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-malformed.html
---

# ignore_malformed [ignore-malformed]

Sometimes you don’t have much control over the data that you receive. One user may send a `login` field that is a [`date`](/reference/elasticsearch/mapping-reference/date.md), and another sends a `login` field that is an email address.

Trying to index the wrong data type into a field throws an exception by default, and rejects the whole document. The `ignore_malformed` parameter, if set to `true`, allows the exception to be ignored. The malformed field is not indexed, but other fields in the document are processed normally.

For example:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "number_one": {
        "type": "integer",
        "ignore_malformed": true
      },
      "number_two": {
        "type": "integer"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "text":       "Some text value",
  "number_one": "foo" <1>
}

PUT my-index-000001/_doc/2
{
  "text":       "Some text value",
  "number_two": "foo" <2>
}
```
% TEST[catch:bad_request]

1. This document will have the `text` field indexed, but not the `number_one` field.
2. This document will be rejected because `number_two` does not allow malformed values.


The `ignore_malformed` setting is currently supported by the following [mapping types](/reference/elasticsearch/mapping-reference/field-data-types.md):

[Numeric](/reference/elasticsearch/mapping-reference/number.md)
:   `long`, `integer`, `short`, `byte`, `double`, `float`, `half_float`, `scaled_float`

[Boolean](/reference/elasticsearch/mapping-reference/boolean.md)
:   `boolean`

[Date](/reference/elasticsearch/mapping-reference/date.md)
:   `date`

[Date nanoseconds](/reference/elasticsearch/mapping-reference/date_nanos.md)
:   `date_nanos`

[Geopoint](/reference/elasticsearch/mapping-reference/geo-point.md)
:   `geo_point` for lat/lon points, although there is a [special case](#_ignore_malformed_geo_point) for out-of-range values

[Geoshape](/reference/elasticsearch/mapping-reference/geo-shape.md)
:   `geo_shape` for complex shapes like polygons

[IP](/reference/elasticsearch/mapping-reference/ip.md)
:   `ip` for IPv4 and IPv6 addresses

::::{tip}
The `ignore_malformed` setting value can be updated on existing fields using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
::::


## Index-level default [ignore-malformed-setting]

The `index.mapping.ignore_malformed` setting can be set on the index level to ignore malformed content globally across all allowed mapping types. Mapping types that don’t support the setting will ignore it if set on the index level.

```console
PUT my-index-000001
{
  "settings": {
    "index.mapping.ignore_malformed": true <1>
  },
  "mappings": {
    "properties": {
      "number_one": { <1>
        "type": "byte"
      },
      "number_two": {
        "type": "integer",
        "ignore_malformed": false <2>
      }
    }
  }
}
```

1. The `number_one` field inherits the index-level setting.
2. The `number_two` field overrides the index-level setting to turn off `ignore_malformed`.



## Dealing with malformed fields [_dealing_with_malformed_fields]

Malformed fields are silently ignored at indexing time when `ignore_malformed` is turned on.
Whenever possible it is recommended to keep the number of documents that have a malformed field contained,
or queries on this field will become meaningless.
Elasticsearch makes it easy to check how many documents have malformed fields by using `exists`,
`term` or `terms` queries on the special [`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md) field.

## The special case of `geo_point` fields [_ignore_malformed_geo_point]

With [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md) fields,
there is the special case of values that have a syntactically valid format,
but the numerical values for `latitude` and `longitude` are out of range.
If `ignore_malformed` is `false`, an exception will be thrown as usual. But if it is `true`,
the document will be indexed correctly, by normalizing the latitude and longitude values into the valid range.
The special [`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md) field will not be set.
The original source document will remain as before, but indexed values, doc-values and stored fields will all be normalized.

## Limits for JSON Objects [json-object-limits]

You can’t use `ignore_malformed` with the following data types:

* [Nested data type](/reference/elasticsearch/mapping-reference/nested.md)
* [Object data type](/reference/elasticsearch/mapping-reference/object.md)
* [Range data types](/reference/elasticsearch/mapping-reference/range.md)

You also can’t use `ignore_malformed` to ignore JSON objects submitted to fields of the wrong data type. A JSON object is any data surrounded by curly brackets `"{}"` and includes data mapped to the nested, object, and range data types.

If you submit a JSON object to an unsupported field, {{es}} will return an error and reject the entire document regardless of the `ignore_malformed` setting.


