---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/properties.html
---

# properties [properties]

Type mappings, [`object` fields](/reference/elasticsearch/mapping-reference/object.md) and [`nested` fields](/reference/elasticsearch/mapping-reference/nested.md) contain sub-fields, called `properties`. These properties may be of any [data type](/reference/elasticsearch/mapping-reference/field-data-types.md), including `object` and `nested`. Properties can be added:

* explicitly by defining them when [creating an index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create).
* explicitly by defining them when adding or updating a mapping type with the [update mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) API.
* [dynamically](docs-content://manage-data/data-store/mapping/dynamic-mapping.md) just by indexing documents containing new fields.

Below is an example of adding `properties` to a mapping type, an `object` field, and a `nested` field:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": { <1>
      "manager": {
        "properties": { <2>
          "age":  { "type": "integer" },
          "name": { "type": "text"  }
        }
      },
      "employees": {
        "type": "nested",
        "properties": { <3>
          "age":  { "type": "integer" },
          "name": { "type": "text"  }
        }
      }
    }
  }
}

PUT my-index-000001/_doc/1 <4>
{
  "region": "US",
  "manager": {
    "name": "Alice White",
    "age": 30
  },
  "employees": [
    {
      "name": "John Smith",
      "age": 34
    },
    {
      "name": "Peter Brown",
      "age": 26
    }
  ]
}
```

1. Properties in the top-level mappings definition.
2. Properties under the `manager` object field.
3. Properties under the `employees` nested field.
4. An example document which corresponds to the above mapping.


::::{tip}
The `properties` setting is allowed to have different settings for fields of the same name in the same index. New properties can be added to existing fields using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
::::


## Dot notation [_dot_notation]

Inner fields can be referred to in queries, aggregations, etc., using *dot notation*:

```console
GET my-index-000001/_search
{
  "query": {
    "match": {
      "manager.name": "Alice White"
    }
  },
  "aggs": {
    "Employees": {
      "nested": {
        "path": "employees"
      },
      "aggs": {
        "Employee Ages": {
          "histogram": {
            "field": "employees.age",
            "interval": 5
          }
        }
      }
    }
  }
}
```

::::{important}
The full path to the inner field must be specified.
::::



