---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-field-context.html
products:
  - id: painless
---

# Field context [painless-field-context]

Use a Painless script to create a [script field](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#script-fields) to return a customized value for each document in the results of a query.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the specified document where each field is a `List` of values.

[`params['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) (`Map`, read-only)
:   Contains extracted JSON in a `Map` and `List` structure for the fields existing in a stored document.

**Return**

`Object`
:   The customized value for each document.

**API**

Both the standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) and [Specialized Field API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-field.html) are available.

**Example**

To run this example, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

You can then use these two example scripts to compute custom information for each search hit and output it to two new fields.

The first script gets the doc value for the `datetime` field and calls the `getDayOfWeekEnum` function to determine the corresponding day of the week.

```painless
doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)
```

The second script calculates the number of actors. Actors' names are stored as a keyword array in the `actors` field.

```painless
doc['actors'].size()  <1>
```

1. By default, doc values are not available for `text` fields. If `actors` was a `text` field, you could still calculate the number of actors by extracting values from `_source` with `params['_source']['actors'].size()`.


The following request returns the calculated day of week and the number of actors that appear in each play:

```console
GET seats/_search
{
  "size": 2,
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "day-of-week": {
      "script": {
        "source": "doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ENGLISH)"
      }
    },
    "number-of-actors": {
      "script": {
        "source": "doc['actors'].size()"
      }
    }
  }
}
```

```console-result
{
  "took" : 68,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 11,
      "relation" : "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "seats",
        "_id" : "1",
        "_score" : 1.0,
        "fields" : {
          "day-of-week" : [
            "Thursday"
          ],
          "number-of-actors" : [
            4
          ]
        }
      },
      {
        "_index" : "seats",
        "_id" : "2",
        "_score" : 1.0,
        "fields" : {
          "day-of-week" : [
            "Thursday"
          ],
          "number-of-actors" : [
            1
          ]
        }
      }
    ]
  }
}
```

