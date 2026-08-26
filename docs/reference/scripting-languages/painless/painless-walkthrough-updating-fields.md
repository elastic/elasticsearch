---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Updating Fields with Painless [_updating_fields_with_painless]

You can also easily update fields. You access the original source for a field as `ctx._source.<field-name>`.

First, let’s look at the source data for a player by submitting the following request:

```console
GET hockey/_search
{
  "query": {
    "term": {
      "_id": 1
    }
  }
}
```

To change player 1’s last name to `hockey`, simply set `ctx._source.last` to the new value:

```console
POST hockey/_update/1
{
  "script": {
    "lang": "painless",
    "source": "ctx._source.last = params.last",
    "params": {
      "last": "hockey"
    }
  }
}
```

You can also add fields to a document. For example, this script adds a new field that contains the player’s nickname,  *hockey*.

```console
POST hockey/_update/1
{
  "script": {
    "lang": "painless",
    "source": """
      ctx._source.last = params.last;
      ctx._source.nick = params.nick
    """,
    "params": {
      "last": "gaudreau",
      "nick": "hockey"
    }
  }
}
```
