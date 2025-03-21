---
navigation_title: "Alias"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/field-alias.html
---

# Alias field type [field-alias]


An `alias` mapping defines an alternate name for a field in the index. The alias can be used in place of the target field in [search](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-search) requests, and selected other APIs like [field capabilities](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-field-caps).

```console
PUT trips
{
  "mappings": {
    "properties": {
      "distance": {
        "type": "long"
      },
      "route_length_miles": {
        "type": "alias",
        "path": "distance" <1>
      },
      "transit_mode": {
        "type": "keyword"
      }
    }
  }
}

GET _search
{
  "query": {
    "range" : {
      "route_length_miles" : {
        "gte" : 39
      }
    }
  }
}
```

1. The path to the target field. Note that this must be the full path, including any parent objects (e.g. `object1.object2.field`).


Almost all components of the search request accept field aliases. In particular, aliases can be used in queries, aggregations, and sort fields, as well as when requesting `docvalue_fields`, `stored_fields`, suggestions, and highlights. Scripts also support aliases when accessing field values. Please see the section on [unsupported APIs](#unsupported-apis) for exceptions.

In some parts of the search request and when requesting field capabilities, field wildcard patterns can be provided. In these cases, the wildcard pattern will match field aliases in addition to concrete fields:

```console
GET trips/_field_caps?fields=route_*,transit_mode
```

## Alias targets [alias-targets]

There are a few restrictions on the target of an alias:

* The target must be a concrete field, and not an object or another field alias.
* The target field must exist at the time the alias is created.
* If nested objects are defined, a field alias must have the same nested scope as its target.

Additionally, a field alias can only have one target. This means that it is not possible to use a field alias to query over multiple target fields in a single clause.

An alias can be changed to refer to a new target through a mappings update. A known limitation is that if any stored percolator queries contain the field alias, they will still refer to its original target. More information can be found in the [percolator documentation](/reference/elasticsearch/mapping-reference/percolator.md).


## Unsupported APIs [unsupported-apis]

Writes to field aliases are not supported: attempting to use an alias in an index or update request will result in a failure. Likewise, aliases cannot be used as the target of `copy_to` or in multi-fields.

Because alias names are not present in the document source, aliases cannot be used when performing source filtering. For example, the following request will return an empty result for `_source`:

```console
GET /_search
{
  "query" : {
    "match_all": {}
  },
  "_source": "route_length_miles"
}
```

Currently only the search and field capabilities APIs will accept and resolve field aliases. Other APIs that accept field names, such as [term vectors](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-termvectors), cannot be used with field aliases.

Finally, some queries, such as `terms`, `geo_shape`, and `more_like_this`, allow for fetching query information from an indexed document. Because field aliases aren’t supported when fetching documents, the part of the query that specifies the lookup path cannot refer to a field by its alias.


