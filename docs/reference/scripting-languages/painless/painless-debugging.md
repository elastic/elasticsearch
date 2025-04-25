---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-debugging.html
---

# Painless debugging [painless-debugging]

## Debug.Explain [_debug_explain]

Painless doesn’t have a [REPL](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop) and while it’d be nice for it to have one day, it wouldn’t tell you the whole story around debugging painless scripts embedded in Elasticsearch because the data that the scripts have access to or "context" is so important. For now the best way to debug embedded scripts is by throwing exceptions at choice places. While you can throw your own exceptions (`throw new Exception('whatever')`), Painless’s sandbox prevents you from accessing useful information like the type of an object. So Painless has a utility method, `Debug.explain` which throws the exception for you. For example, you can use [`_explain`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-explain) to explore the context available to a [script query](/reference/query-languages/query-dsl/query-dsl-script-query.md).

```console
PUT /hockey/_doc/1?refresh
{"first":"johnny","last":"gaudreau","goals":[9,27,1],"assists":[17,46,0],"gp":[26,82,1]}

POST /hockey/_explain/1
{
  "query": {
    "script": {
      "script": "Debug.explain(doc.goals)"
    }
  }
}
```

Which shows that the class of `doc.first` is `org.elasticsearch.index.fielddata.ScriptDocValues.Longs` by responding with:

```console-result
{
   "error": {
      "type": "script_exception",
      "to_string": "[1, 9, 27]",
      "painless_class": "org.elasticsearch.index.fielddata.ScriptDocValues.Longs",
      "java_class": "org.elasticsearch.index.fielddata.ScriptDocValues$Longs",
      ...
   },
   "status": 400
}
```

You can use the same trick to see that `_source` is a `LinkedHashMap` in the `_update` API:

```console
POST /hockey/_update/1
{
  "script": "Debug.explain(ctx._source)"
}
```

The response looks like:

```console-result
{
  "error" : {
    "root_cause": ...,
    "type": "illegal_argument_exception",
    "reason": "failed to execute script",
    "caused_by": {
      "type": "script_exception",
      "to_string": "{gp=[26, 82, 1], last=gaudreau, assists=[17, 46, 0], first=johnny, goals=[9, 27, 1]}",
      "painless_class": "java.util.LinkedHashMap",
      "java_class": "java.util.LinkedHashMap",
      ...
    }
  },
  "status": 400
}
```

Once you have a class you can go to [*Painless API Reference*](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference.html) to see a list of available methods.


