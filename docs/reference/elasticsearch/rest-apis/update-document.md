---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/8.18/docs-update.html
applies_to:
  stack: all
navigation_title: Update a document
---

# Update a document [update-document]

The [Update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) enables you to script document updates, allowing you to update, delete, or skip modifying a document. 

The following examples show common use cases, such as incrementing a counter and adding or removing elements from a list, as well as how to:

- [Update part of a document](#update-part-document)
- [Detect noop updates](#detect-noop-updates)
- [Upsert](#upsert)
- [Doc as upsert](#scripted-upsert)

First, let's index a simple doc:

```console
PUT test/_doc/1
{
  "counter" : 1,
  "tags" : ["red"]
}
```
% TESTSETUP

To increment the counter, you can submit an update request with the
following script:

```console
POST test/_update/1
{
  "script" : {
    "source": "ctx._source.counter += params.count",
    "lang": "painless",
    "params" : {
      "count" : 4
    }
  }
}
```

Similarly, you could use and update script to add a tag to the list of tags
(this is just a list, so the tag is added even it exists):

```console
POST test/_update/1
{
  "script": {
    "source": "ctx._source.tags.add(params.tag)",
    "lang": "painless",
    "params": {
      "tag": "blue"
    }
  }
}
```

You could also remove a tag from the list of tags. The Painless
function to `remove` a tag takes the array index of the element
you want to remove. To avoid a possible runtime error, you first need to
make sure the tag exists. If the list contains duplicates of the tag, this
script just removes one occurrence.

```console
POST test/_update/1
{
  "script": {
    "source": "if (ctx._source.tags.contains(params.tag)) { ctx._source.tags.remove(ctx._source.tags.indexOf(params.tag)) }",
    "lang": "painless",
    "params": {
      "tag": "blue"
    }
  }
}
```

You can also add and remove fields from a document. For example, this script
adds the field `new_field`:

```console
POST test/_update/1
{
  "script" : "ctx._source.new_field = 'value_of_new_field'"
}
```

Conversely, this script removes the field `new_field`:

```console
POST test/_update/1
{
  "script" : "ctx._source.remove('new_field')"
}
```
% TEST[continued]

The following script removes a subfield from an object field:

```console
PUT test/_doc/1?refresh
{
  "my-object": {
    "my-subfield": true
  }
}
```

```console
POST test/_update/1
{
  "script": "ctx._source['my-object'].remove('my-subfield')"
}
```
% TEST[continued]

Instead of updating the document, you can also change the operation that is
executed from within the script. For example, this request deletes the doc if
the `tags` field contains `green`, otherwise it does nothing (`noop`):

```console
POST test/_update/1
{
  "script": {
    "source": "if (ctx._source.tags.contains(params.tag)) { ctx.op = 'delete' } else { ctx.op = 'noop' }",
    "lang": "painless",
    "params": {
      "tag": "green"
    }
  }
}
```

## Update part of a document [update-part-document]

The following partial update adds a new field to the
existing document:

```console
POST test/_update/1
{
  "doc": {
    "name": "new_name"
  }
}
```

If both `doc` and `script` are specified, then `doc` is ignored. If you
specify a scripted update, include the fields you want to update in the script.


## Detect noop updates [detect-noop-updates]

By default updates that don't change anything detect that they don't change
anything and return `"result": "noop"`:

```console
POST test/_update/1
{
  "doc": {
    "name": "new_name"
  }
}
```
% TEST[continued]

If the value of `name` is already `new_name`, the update
request is ignored and the `result` element in the response returns `noop`:

```console

{
   "_shards": {
        "total": 0,
        "successful": 0,
        "failed": 0
   },
   "_index": "test",
   "_id": "1",
   "_version": 2,
   "_primary_term": 1,
   "_seq_no": 1,
   "result": "noop"
}
```

You can disable this behavior by setting `"detect_noop": false`:

```console
POST test/_update/1
{
  "doc": {
    "name": "new_name"
  },
  "detect_noop": false
}
```

## Upsert [upsert]

An upsert operation lets you update an existing document or insert a new one if it doesn't exist, in a single request.

In this example, if the product with ID `1` exists, its price will be updated to `100`. If the product does not exist, a new document with ID `1` and a price of `50` will be inserted.

```console
POST /test/_update/1
{
  "doc": {
    "product_price": 100
  },
  "upsert": {
    "product_price": 50
  }
}
```

## Scripted upsert [scripted-upsert]

To run the script whether or not the document exists, set `scripted_upsert` to
`true`:

```console
POST test/_update/1
{
  "scripted_upsert": true,
  "script": {
    "source": """
      if ( ctx.op == 'create' ) {
        ctx._source.counter = params.count
      } else {
        ctx._source.counter += params.count
      }
    """,
    "params": {
      "count": 4
    }
  },
  "upsert": {}
}
```

## Doc as upsert [scripted-upsert]

Instead of sending a partial `doc` plus an `upsert` doc, you can set
`doc_as_upsert` to `true` to use the contents of `doc` as the `upsert`
value:

```console
POST test/_update/1
{
  "doc": {
    "name": "new_name"
  },
  "doc_as_upsert": true
}
```

::::{note}
Using [ingest pipelines](https://www.elastic.co/guide/en/elasticsearch/reference/8.18/ingest.html) with `doc_as_upsert` is not supported.
::::
