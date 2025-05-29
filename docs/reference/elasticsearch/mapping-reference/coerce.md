---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/coerce.html
---

# coerce [coerce]

Data is not always clean. Depending on how it is produced a number might be rendered in the JSON body as a true JSON number, e.g. `5`, but it might also be rendered as a string, e.g. `"5"`. Alternatively, a number that should be an integer might instead be rendered as a floating point, e.g. `5.0`, or even `"5.0"`.

Coercion attempts to clean up dirty values to fit the data type of a field. For instance:

* Strings will be coerced to numbers.
* Floating points will be truncated for integer values.

For instance:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "number_one": {
        "type": "integer"
      },
      "number_two": {
        "type": "integer",
        "coerce": false
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "number_one": "10" <1>
}

PUT my-index-000001/_doc/2
{
  "number_two": "10" <2>
}
```
% TEST[catch:bad_request]

1. The `number_one` field will contain the integer `10`.
2. This document will be rejected because coercion is disabled.


::::{tip}
The `coerce` setting value can be updated on existing fields using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
::::


## Index-level default [coerce-setting]

The `index.mapping.coerce` setting can be set on the index level to disable coercion globally across all mapping types:

```console
PUT my-index-000001
{
  "settings": {
    "index.mapping.coerce": false
  },
  "mappings": {
    "properties": {
      "number_one": {
        "type": "integer",
        "coerce": true
      },
      "number_two": {
        "type": "integer"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{ "number_one": "10" } <1>

PUT my-index-000001/_doc/2
{ "number_two": "10" } <2>
```
% TEST[catch:bad_request]

1. The `number_one` field overrides the index level setting to enable coercion.
2. This document will be rejected because the `number_two` field inherits the index-level coercion setting.



