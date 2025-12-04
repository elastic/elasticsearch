---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Accessing Doc Values from Painless [_accessing_doc_values_from_painless]

Document values can be accessed from a `Map` named `doc`.

For example, the following script calculates a player’s total goals. This example uses a strongly typed `int` and a `for` loop.

```console
GET hockey/_search
{
  "query": {
    "function_score": {
      "script_score": {
        "script": {
          "lang": "painless",
          "source": """
            int total = 0;
            for (int i = 0; i < doc['goals'].length; ++i) {
              total += doc['goals'][i];
            }
            return total;
          """
        }
      }
    }
  }
}
```

Alternatively, you could do the same thing using a script field instead of a function score:

```console
GET hockey/_search
{
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "total_goals": {
      "script": {
        "lang": "painless",
        "source": """
          int total = 0;
          for (int i = 0; i < doc['goals'].length; ++i) {
            total += doc['goals'][i];
          }
          return total;
        """
      }
    }
  }
}
```

The following example uses a Painless script to sort the players by their combined first and last names. The names are accessed using `doc['first'].value` and `doc['last'].value`.

```console
GET hockey/_search
{
  "query": {
    "match_all": {}
  },
  "sort": {
    "_script": {
      "type": "string",
      "order": "asc",
      "script": {
        "lang": "painless",
        "source": "doc['first.keyword'].value + ' ' + doc['last.keyword'].value"
      }
    }
  }
}
```
