---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/similarity.html
---

# similarity [similarity]

{{es}} allows you to configure a text scoring algorithm or *similarity* per field. The `similarity` setting provides a simple way of choosing a text similarity algorithm other than the default `BM25`, such as `boolean`.

Only text-based field types like [`text`](/reference/elasticsearch/mapping-reference/text.md) and [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) support this configuration.

Custom similarities can be configured by tuning the parameters of the built-in similarities. For more details about this expert options, see the [similarity module](/reference/elasticsearch/index-settings/similarity.md).

The only similarities which can be used out of the box, without any further configuration are:

`BM25`
:   The [Okapi BM25 algorithm](https://en.wikipedia.org/wiki/Okapi_BM25). The algorithm used by default in {{es}} and Lucene.

`boolean`
:   A simple boolean similarity, which is used when full-text ranking is not needed and the score should only be based on whether the query terms match or not. Boolean similarity gives terms a score equal to their query boost.

The `similarity` can be set on the field level when a field is first created, as follows:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "default_field": { <1>
        "type": "text"
      },
      "boolean_sim_field": {
        "type": "text",
        "similarity": "boolean" <2>
      }
    }
  }
}
```

1. The `default_field` uses the `BM25` similarity.
2. The `boolean_sim_field` uses the `boolean` similarity.


