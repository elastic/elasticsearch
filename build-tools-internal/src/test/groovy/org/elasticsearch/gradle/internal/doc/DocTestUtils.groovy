/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc;

class DocTestUtils {
    public static Map<String,String> SAMPLE_TEST_DOCS = Map.of(
        "mapping-charfilter.asciidoc", """
[[mapper-annotated-text]]
=== Mapper annotated text plugin

experimental[]

The mapper-annotated-text plugin provides the ability to index text that is a
combination of free-text and special markup that is typically used to identify
items of interest such as people or organisations (see NER or Named Entity Recognition
tools).


The elasticsearch markup allows one or more additional tokens to be injected, unchanged, into the token
stream at the same position as the underlying text it annotates.

:plugin_name: mapper-annotated-text
include::install_remove.asciidoc[]

[[mapper-annotated-text-usage]]
==== Using the `annotated-text` field

The `annotated-text` tokenizes text content as per the more common {ref}/text.html[`text`] field (see
"limitations" below) but also injects any marked-up annotation tokens directly into
the search index:

[source,console]
--------------------------
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_field": {
        "type": "annotated_text"
      }
    }
  }
}
--------------------------

Such a mapping would allow marked-up text eg wikipedia articles to be indexed as both text
and structured tokens. The annotations use a markdown-like syntax using URL encoding of
one or more values separated by the `&` symbol.


We can use the "_analyze" api to test how an example annotation would be stored as tokens
in the search index:


[source,js]
--------------------------
GET my-index-000001/_analyze
{
  "field": "my_field",
  "text":"Investors in [Apple](Apple+Inc.) rejoiced."
}
--------------------------
// NOTCONSOLE

Response:

[source,js]
--------------------------------------------------
{
  "tokens": [
    {
      "token": "investors",
      "start_offset": 0,
      "end_offset": 9,
      "type": "<ALPHANUM>",
      "position": 0
    },
    {
      "token": "in",
      "start_offset": 10,
      "end_offset": 12,
      "type": "<ALPHANUM>",
      "position": 1
    },
    {
      "token": "Apple Inc.", <1>
      "start_offset": 13,
      "end_offset": 18,
      "type": "annotation",
      "position": 2
    },
    {
      "token": "apple",
      "start_offset": 13,
      "end_offset": 18,
      "type": "<ALPHANUM>",
      "position": 2
    },
    {
      "token": "rejoiced",
      "start_offset": 19,
      "end_offset": 27,
      "type": "<ALPHANUM>",
      "position": 3
    }
  ]
}
--------------------------------------------------
// NOTCONSOLE

<1> Note the whole annotation token `Apple Inc.` is placed, unchanged as a single token in
the token stream and at the same position (position 2) as the text token (`apple`) it annotates.


We can now perform searches for annotations using regular `term` queries that don't tokenize
the provided search values. Annotations are a more precise way of matching as can be seen
in this example where a search for `Beck` will not match `Jeff Beck` :

[source,console]
--------------------------
# Example documents
PUT my-index-000001/_doc/1
{
  "my_field": "[Beck](Beck) announced a new tour"<1>
}

PUT my-index-000001/_doc/2
{
  "my_field": "[Jeff Beck](Jeff+Beck&Guitarist) plays a strat"<2>
}

# Example search
GET my-index-000001/_search
{
  "query": {
    "term": {
        "my_field": "Beck" <3>
    }
  }
}
--------------------------

<1> As well as tokenising the plain text into single words e.g. `beck`, here we
inject the single token value `Beck` at the same position as `beck` in the token stream.
<2> Note annotations can inject multiple tokens at the same position - here we inject both
the very specific value `Jeff Beck` and the broader term `Guitarist`. This enables
broader positional queries e.g. finding mentions of a `Guitarist` near to `strat`.
<3> A benefit of searching with these carefully defined annotation tokens is that a query for
`Beck` will not match document 2 that contains the tokens `jeff`, `beck` and `Jeff Beck`

WARNING: Any use of `=` signs in annotation values eg `[Prince](person=Prince)` will
cause the document to be rejected with a parse failure. In future we hope to have a use for
the equals signs so wil actively reject documents that contain this today.


[[mapper-annotated-text-tips]]
==== Data modelling tips
===== Use structured and unstructured fields

Annotations are normally a way of weaving structured information into unstructured text for
higher-precision search.

`Entity resolution` is a form of document enrichment undertaken by specialist software or people
where references to entities in a document are disambiguated by attaching a canonical ID.
The ID is used to resolve any number of aliases or distinguish between people with the
same name. The hyperlinks connecting Wikipedia's articles are a good example of resolved
entity IDs woven into text.

These IDs can be embedded as annotations in an annotated_text field but it often makes
sense to include them in dedicated structured fields to support discovery via aggregations:

[source,console]
--------------------------
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_unstructured_text_field": {
        "type": "annotated_text"
      },
      "my_structured_people_field": {
        "type": "text",
        "fields": {
          "keyword" : {
            "type": "keyword"
          }
        }
      }
    }
  }
}
--------------------------

Applications would then typically provide content and discover it as follows:

[source,console]
--------------------------
# Example documents
PUT my-index-000001/_doc/1
{
  "my_unstructured_text_field": "[Shay](%40kimchy) created elasticsearch",
  "my_twitter_handles": ["@kimchy"] <1>
}

GET my-index-000001/_search
{
  "query": {
    "query_string": {
        "query": "elasticsearch OR logstash OR kibana",<2>
        "default_field": "my_unstructured_text_field"
    }
  },
  "aggregations": {
  \t"top_people" :{
  \t    "significant_terms" : { <3>
\t       "field" : "my_twitter_handles.keyword"
  \t    }
  \t}
  }
}
--------------------------

<1> Note the `my_twitter_handles` contains a list of the annotation values
also used in the unstructured text. (Note the annotated_text syntax requires escaping).
By repeating the annotation values in a structured field this application has ensured that
the tokens discovered in the structured field can be used for search and highlighting
in the unstructured field.
<2> In this example we search for documents that talk about components of the elastic stack
<3> We use the `my_twitter_handles` field here to discover people who are significantly
associated with the elastic stack.

===== Avoiding over-matching annotations
By design, the regular text tokens and the annotation tokens co-exist in the same indexed
field but in rare cases this can lead to some over-matching.

The value of an annotation often denotes a _named entity_ (a person, place or company).
The tokens for these named entities are inserted untokenized, and differ from typical text
tokens because they are normally:

* Mixed case e.g. `Madonna`
* Multiple words e.g. `Jeff Beck`
* Can have punctuation or numbers e.g. `Apple Inc.` or `@kimchy`

This means, for the most part, a search for a named entity in the annotated text field will
not have any false positives e.g. when selecting `Apple Inc.` from an aggregation result
you can drill down to highlight uses in the text without "over matching" on any text tokens
like the word `apple` in this context:

    the apple was very juicy

However, a problem arises if your named entity happens to be a single term and lower-case e.g. the
company `elastic`. In this case, a search on the annotated text field for the token `elastic`
may match a text document such as this:

    they fired an elastic band

To avoid such false matches users should consider prefixing annotation values to ensure
they don't name clash with text tokens e.g.

    [elastic](Company_elastic) released version 7.0 of the elastic stack today




[[mapper-annotated-text-highlighter]]
==== Using the `annotated` highlighter

The `annotated-text` plugin includes a custom highlighter designed to mark up search hits
in a way which is respectful of the original markup:

[source,console]
--------------------------
# Example documents
PUT my-index-000001/_doc/1
{
  "my_field": "The cat sat on the [mat](sku3578)"
}

GET my-index-000001/_search
{
  "query": {
    "query_string": {
        "query": "cats"
    }
  },
  "highlight": {
    "fields": {
      "my_field": {
        "type": "annotated", <1>
        "require_field_match": false
      }
    }
  }
}
--------------------------

<1> The `annotated` highlighter type is designed for use with annotated_text fields

The annotated highlighter is based on the `unified` highlighter and supports the same
settings but does not use the `pre_tags` or `post_tags` parameters. Rather than using
html-like markup such as `<em>cat</em>` the annotated highlighter uses the same
markdown-like syntax used for annotations and injects a key=value annotation where `_hit_term`
is the key and the matched search term is the value e.g.

    The [cat](_hit_term=cat) sat on the [mat](sku3578)

The annotated highlighter tries to be respectful of any existing markup in the original
text:

* If the search term matches exactly the location of an existing annotation then the
`_hit_term` key is merged into the url-like syntax used in the `(...)` part of the
existing annotation.
* However, if the search term overlaps the span of an existing annotation it would break
the markup formatting so the original annotation is removed in favour of a new annotation
with just the search hit information in the results.
* Any non-overlapping annotations in the original text are preserved in highlighter
selections


[[mapper-annotated-text-limitations]]
==== Limitations

The annotated_text field type supports the same mapping settings as the `text` field type
but with the following exceptions:

* No support for `fielddata` or `fielddata_frequency_filter`
* No support for `index_prefixes` or `index_phrases` indexing

""",
        "painless-field-context.asciidoc", """
[[painless-field-context]]
=== Field context

Use a Painless script to create a
{ref}/search-fields.html#script-fields[script field] to return
a customized value for each document in the results of a query.

*Variables*

`params` (`Map`, read-only)::
        User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)::
        Contains the fields of the specified document where each field is a
        `List` of values.

{ref}/mapping-source-field.html[`params['_source']`] (`Map`, read-only)::
        Contains extracted JSON in a `Map` and `List` structure for the fields
        existing in a stored document.

*Return*

`Object`::
        The customized value for each document.

*API*

Both the standard <<painless-api-reference-shared, Painless API>> and
<<painless-api-reference-field, Specialized Field API>> are available.


*Example*

To run this example, first follow the steps in
<<painless-context-examples, context examples>>.

You can then use these two example scripts to compute custom information
for each search hit and output it to two new fields.

The first script gets the doc value for the `datetime` field and calls
the `getDayOfWeekEnum` function to determine the corresponding day of the week.

[source,Painless]
----
doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)
----

The second script calculates the number of actors. Actors' names are stored
as a keyword array in the `actors` field.

[source,Painless]
----
doc['actors'].size()  <1>
----

<1> By default, doc values are not available for `text` fields. If `actors` was
a `text` field, you could still calculate the number of actors by extracting
values from `_source` with `params['_source']['actors'].size()`.

The following request returns the calculated day of week and the number of
actors that appear in each play:

[source,console]
----
GET seats/_search
{
  "size": 2,
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "day-of-week": {
      "script": {
        "source": "doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)"
      }
    },
    "number-of-actors": {
      "script": {
        "source": "doc['actors'].size()"
      }
    }
  }
}
----
// TEST[setup:seats]

[source,console-result]
----
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
----
// TESTRESPONSE[s/"took" : 68/"took" : "\$body.took"/]
""",
        "painless-field-context.mdx", """---
id: enElasticsearchPainlessPainlessFieldContext
slug: /en/elasticsearch/painless/painless-field-context
title: Field context
description: Description to be written
tags: []
---

<div id="painless-field-context"></div>

Use a Painless script to create a
[script field](((ref))/search-fields.html#script-fields) to return
a customized value for each document in the results of a query.

**Variables**

`params` (`Map`, read-only)
    : User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
    : Contains the fields of the specified document where each field is a
    `List` of values.

[`params['_source']`](((ref))/mapping-source-field.html) (`Map`, read-only)
    : Contains extracted JSON in a `Map` and `List` structure for the fields
    existing in a stored document.

**Return**

`Object`
    : The customized value for each document.

**API**

Both the standard <DocLink id="enElasticsearchPainlessPainlessApiReferenceShared">Painless API</DocLink> and
<DocLink id="enElasticsearchPainlessPainlessApiReferenceField">Specialized Field API</DocLink> are available.

**Example**

To run this example, first follow the steps in
<DocLink id="enElasticsearchPainlessPainlessContextExamples">context examples</DocLink>.

You can then use these two example scripts to compute custom information
for each search hit and output it to two new fields.

The first script gets the doc value for the `datetime` field and calls
the `getDayOfWeekEnum` function to determine the corresponding day of the week.

```Painless
doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)
```

The second script calculates the number of actors. Actors' names are stored
as a keyword array in the `actors` field.

```Painless
doc['actors'].size()   [^1]
```
[^1]: By default, doc values are not available for `text` fields. If `actors` was
a `text` field, you could still calculate the number of actors by extracting
values from `_source` with `params['_source']['actors'].size()`.

The following request returns the calculated day of week and the number of
actors that appear in each play:

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
        "source": "doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)"
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
{/* TEST[setup:seats] */}

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
{/* TESTRESPONSE[s/"took" : 68/"took" : "\$body.took"/] */}
""",
        "painless-field-context-different.mdx", """---
id: enElasticsearchPainlessPainlessFieldContext
slug: /en/elasticsearch/painless/painless-field-context
title: Field context
description: Description to be written
tags: []
---

<div id="painless-field-context"></div>

Use a Painless script to create a
[script field](((ref))/search-fields.html#script-fields) to return
a customized value for each document in the results of a query.

**Variables**

`params` (`Map`, read-only)
    : User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
    : Contains the fields of the specified document where each field is a
    `List` of values.

[`params['_source']`](((ref))/mapping-source-field.html) (`Map`, read-only)
    : Contains extracted JSON in a `Map` and `List` structure for the fields
    existing in a stored document.

**Return**

`Object`
    : The customized value for each document.

**API**

Both the standard <DocLink id="enElasticsearchPainlessPainlessApiReferenceShared">Painless API</DocLink> and
<DocLink id="enElasticsearchPainlessPainlessApiReferenceField">Specialized Field API</DocLink> are available.

**Example**

To run this example, first follow the steps in
<DocLink id="enElasticsearchPainlessPainlessContextExamples">context examples</DocLink>.

You can then use these two example scripts to compute custom information
for each search hit and output it to two new fields.

The first script gets the doc value for the `datetime` field and calls
the `getDayOfWeekEnum` function to determine the corresponding day of the week.

```Painless
doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)
```

The second script calculates the number of actors. Actors' names are stored
as a keyword array in the `actors` field.

```Painless
doc['actresses'].size()   [^1]
```
[^1]: By default, doc values are not available for `text` fields. If `actors` was
a `text` field, you could still calculate the number of actors by extracting
values from `_source` with `params['_source']['actors'].size()`.

The following request returns the calculated day of week and the number of
actors that appear in each play:

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
        "source": "doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)"
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
{/* TEST[setup:seats] */}

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
{/* TESTRESPONSE[s/"took" : 68/"took" : "\$body.took"/] */}
""",
        "docs/reference/sql/getting-started.asciidoc", """
[role="xpack"]
[[sql-getting-started]]
== Getting Started with SQL

To start using {es-sql}, create
an index with some data to experiment with:

[source,console]
--------------------------------------------------
PUT /library/_bulk?refresh
{"index":{"_id": "Leviathan Wakes"}}
{"name": "Leviathan Wakes", "author": "James S.A. Corey", "release_date": "2011-06-02", "page_count": 561}
{"index":{"_id": "Hyperion"}}
{"name": "Hyperion", "author": "Dan Simmons", "release_date": "1989-05-26", "page_count": 482}
{"index":{"_id": "Dune"}}
{"name": "Dune", "author": "Frank Herbert", "release_date": "1965-06-01", "page_count": 604}
--------------------------------------------------

And now you can execute SQL using the <<sql-search-api,SQL search API>>:

[source,console]
--------------------------------------------------
POST /_sql?format=txt
{
  "query": "SELECT * FROM library WHERE release_date < '2000-01-01'"
}
--------------------------------------------------
// TEST[continued]

Which should return something along the lines of:

[source,text]
--------------------------------------------------
    author     |     name      |  page_count   | release_date
---------------+---------------+---------------+------------------------
Dan Simmons    |Hyperion       |482            |1989-05-26T00:00:00.000Z
Frank Herbert  |Dune           |604            |1965-06-01T00:00:00.000Z
--------------------------------------------------
// TESTRESPONSE[s/\\|/\\\\|/ s/\\+/\\\\+/]
// TESTRESPONSE[non_json]

You can also use the <<sql-cli>>. There is a script to start it
shipped in x-pack's bin directory:

[source,bash]
--------------------------------------------------
\$ ./bin/elasticsearch-sql-cli
--------------------------------------------------

From there you can run the same query:

[source,sqlcli]
--------------------------------------------------
sql> SELECT * FROM library WHERE release_date < '2000-01-01';
    author     |     name      |  page_count   | release_date
---------------+---------------+---------------+------------------------
Dan Simmons    |Hyperion       |482            |1989-05-26T00:00:00.000Z
Frank Herbert  |Dune           |604            |1965-06-01T00:00:00.000Z
--------------------------------------------------
""",
        "docs/ml-update-snapshot.asciidoc",             """
[role="xpack"]
[[ml-update-snapshot]]
= Update model snapshots API
++++
<titleabbrev>Update model snapshots</titleabbrev>
++++

Updates certain properties of a snapshot.

[[ml-update-snapshot-request]]
== {api-request-title}

`POST _ml/anomaly_detectors/<job_id>/model_snapshots/<snapshot_id>/_update`

[[ml-update-snapshot-prereqs]]
== {api-prereq-title}

Requires the `manage_ml` cluster privilege. This privilege is included in the
`machine_learning_admin` built-in role.

[[ml-update-snapshot-path-parms]]
== {api-path-parms-title}

`<job_id>`::
(Required, string)
include::{es-repo-dir}/ml/ml-shared.asciidoc[tag=job-id-anomaly-detection]

`<snapshot_id>`::
(Required, string)
include::{es-repo-dir}/ml/ml-shared.asciidoc[tag=snapshot-id]

[[ml-update-snapshot-request-body]]
== {api-request-body-title}

The following properties can be updated after the model snapshot is created:

`description`::
(Optional, string) A description of the model snapshot.

`retain`::
(Optional, Boolean)
include::{es-repo-dir}/ml/ml-shared.asciidoc[tag=retain]


[[ml-update-snapshot-example]]
== {api-examples-title}

[source,console]
--------------------------------------------------
POST
_ml/anomaly_detectors/it_ops_new_logs/model_snapshots/1491852978/_update
{
  "description": "Snapshot 1",
  "retain": true
}
--------------------------------------------------
// TEST[skip:todo]

When the snapshot is updated, you receive the following results:
[source,js]
----
{
  "acknowledged": true,
  "model": {
    "job_id": "it_ops_new_logs",
    "timestamp": 1491852978000,
    "description": "Snapshot 1",
...
    "retain": true
  }
}
----

"""
,"docs/painless-debugging.asciidoc",             """

[[painless-debugging]]
=== Painless Debugging

==== Debug.Explain

Painless doesn't have a
{wikipedia}/Read%E2%80%93eval%E2%80%93print_loop[REPL]
and while it'd be nice for it to have one day, it wouldn't tell you the
whole story around debugging painless scripts embedded in Elasticsearch because
the data that the scripts have access to or "context" is so important. For now
the best way to debug embedded scripts is by throwing exceptions at choice
places. While you can throw your own exceptions
(`throw new Exception('whatever')`), Painless's sandbox prevents you from
accessing useful information like the type of an object. So Painless has a
utility method, `Debug.explain` which throws the exception for you. For
example, you can use {ref}/search-explain.html[`_explain`] to explore the
context available to a {ref}/query-dsl-script-query.html[script query].

[source,console]
---------------------------------------------------------
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
---------------------------------------------------------
// TEST[s/_explain\\/1/_explain\\/1?error_trace=false/ catch:/painless_explain_error/]
// The test system sends error_trace=true by default for easier debugging so
// we have to override it to get a normal shaped response

Which shows that the class of `doc.first` is
`org.elasticsearch.index.fielddata.ScriptDocValues.Longs` by responding with:

[source,console-result]
---------------------------------------------------------
{
   "error": {
      "type": "script_exception",
      "to_string": "[1, 9, 27]",
      "painless_class": "org.elasticsearch.index.fielddata.ScriptDocValues.Longs",
      "java_class": "org.elasticsearch.index.fielddata.ScriptDocValues\$Longs",
      ...
   },
   "status": 400
}
---------------------------------------------------------
// TESTRESPONSE[s/\\.\\.\\./"script_stack": \$body.error.script_stack, "script": \$body.error.script, "lang": \$body.error.lang, "position": \$body.error.position, "caused_by": \$body.error.caused_by, "root_cause": \$body.error.root_cause, "reason": \$body.error.reason/]

You can use the same trick to see that `_source` is a `LinkedHashMap`
in the `_update` API:

[source,console]
---------------------------------------------------------
POST /hockey/_update/1
{
  "script": "Debug.explain(ctx._source)"
}
---------------------------------------------------------
// TEST[continued s/_update\\/1/_update\\/1?error_trace=false/ catch:/painless_explain_error/]

The response looks like:

[source,console-result]
---------------------------------------------------------
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
---------------------------------------------------------
// TESTRESPONSE[s/"root_cause": \\.\\.\\./"root_cause": \$body.error.root_cause/]
// TESTRESPONSE[s/\\.\\.\\./"script_stack": \$body.error.caused_by.script_stack, "script": \$body.error.caused_by.script, "lang": \$body.error.caused_by.lang, "position": \$body.error.caused_by.position, "caused_by": \$body.error.caused_by.caused_by, "reason": \$body.error.caused_by.reason/]
// TESTRESPONSE[s/"to_string": ".+"/"to_string": \$body.error.caused_by.to_string/]

Once you have a class you can go to <<painless-api-reference>> to see a list of
available methods.

""", "docs/reference/security/authorization/run-as-privilege.asciidoc",             """[role="xpack"]
[[run-as-privilege]]
= Submitting requests on behalf of other users

{es} roles support a `run_as` privilege that enables an authenticated user to
submit requests on behalf of other users. For example, if your external
application is trusted to authenticate users, {es} can authenticate the external
application and use the _run as_ mechanism to issue authorized requests as
other users without having to re-authenticate each user.

To "run as" (impersonate) another user, the first user (the authenticating user)
must be authenticated by a mechanism that supports run-as delegation. The second
user (the `run_as` user) must be authorized by a mechanism that supports
delegated run-as lookups by username.

The `run_as` privilege essentially operates like a secondary form of
<<authorization_realms,delegated authorization>>. Delegated authorization applies
to the authenticating user, and the `run_as` privilege applies to the user who
is being impersonated.

Authenticating user::
--
For the authenticating user, the following realms (plus API keys) all support
`run_as` delegation: `native`, `file`, Active Directory, JWT, Kerberos, LDAP and
PKI.

Service tokens, the {es} Token Service, SAML 2.0, and OIDC 1.0 do not
support `run_as` delegation.
--

`run_as` user::
--
{es} supports `run_as` for any realm that supports user lookup.
Not all realms support user lookup. Refer to the list of <<user-lookup,supported realms>>
and ensure that the realm you wish to use is configured in a manner that
supports user lookup.

The `run_as` user must be retrieved from a <<realms,realm>> - it is not
possible to run as a
<<service-accounts,service account>>,
<<token-authentication-api-key,API key>> or
<<token-authentication-access-token,access token>>.
--

To submit requests on behalf of other users, you need to have the `run_as`
privilege in your <<defining-roles,roles>>. For example, the following request
creates a `my_director` role that grants permission to submit request on behalf
of `jacknich` or `redeniro`:

[source,console]
----
POST /_security/role/my_director?refresh=true
{
  "cluster": ["manage"],
  "indices": [
    {
      "names": [ "index1", "index2" ],
      "privileges": [ "manage" ]
    }
  ],
  "run_as": [ "jacknich", "rdeniro" ],
  "metadata" : {
    "version" : 1
  }
}
----

To submit a request as another user, you specify the user in the
`es-security-runas-user` request header. For example:

[source,sh]
----
curl -H "es-security-runas-user: jacknich" -u es-admin -X GET http://localhost:9200/
----

The `run_as` user passed in through the `es-security-runas-user` header must be
available from a realm that supports delegated authorization lookup by username.
Realms that don't support user lookup can't be used by `run_as` delegation from
other realms.

For example, JWT realms can authenticate external users specified in JWTs, and
execute requests as a `run_as` user in the `native` realm. {es} will retrieve the
indicated `runas` user and execute the request as that user using their roles.

[[run-as-privilege-apply]]
== Apply the `run_as` privilege to roles
You can apply the `run_as` privilege when creating roles with the
<<security-api-put-role,create or update roles API>>. Users who are assigned
a role that contains the `run_as` privilege inherit all privileges from their
role, and can also submit requests on behalf of the indicated users.

NOTE: Roles for the authenticated user and the `run_as` user are not merged. If
a user authenticates without specifying the `run_as` parameter, only the
authenticated user's roles are used. If a user authenticates and their roles
include the `run_as` parameter, only the `run_as` user's roles are used.

After a user successfully authenticates to {es}, an authorization process determines whether the user behind an incoming request is allowed to run
that request. If the authenticated user has the `run_as` privilege in their list
of permissions and specifies the run-as header, {es} _discards_ the authenticated
user and associated roles. It then looks in each of the configured realms in the
realm chain until it finds the username that's associated with the `run_as` user,
and uses those roles to execute any requests.

Consider an admin role and an analyst role. The admin role has higher privileges,
but might also want to submit requests as another user to test and verify their
permissions.

First, we'll create an admin role named `my_admin_role`. This role has `manage`
<<security-privileges,privileges>> on the entire cluster, and on a subset of
indices. This role also contains the `run_as` privilege, which enables any user
with this role to submit requests on behalf of the specified `analyst_user`.

[source,console]
----
POST /_security/role/my_admin_role?refresh=true
{
  "cluster": ["manage"],
  "indices": [
    {
      "names": [ "index1", "index2" ],
      "privileges": [ "manage" ]
    }
  ],
  "applications": [
    {
      "application": "myapp",
      "privileges": [ "admin", "read" ],
      "resources": [ "*" ]
    }
  ],
  "run_as": [ "analyst_user" ],
  "metadata" : {
    "version" : 1
  }
}
----

Next, we'll create an analyst role named `my_analyst_role`, which has more
restricted `monitor` cluster privileges and `manage` privileges on a subset of
indices.

[source,console]
----
POST /_security/role/my_analyst_role?refresh=true
{
  "cluster": [ "monitor"],
  "indices": [
    {
      "names": [ "index1", "index2" ],
      "privileges": ["manage"]
    }
  ],
  "applications": [
    {
      "application": "myapp",
      "privileges": [ "read" ],
      "resources": [ "*" ]
    }
  ],
  "metadata" : {
    "version" : 1
  }
}
----

We'll create an administrator user and assign them the role named `my_admin_role`,
which allows this user to submit requests as the `analyst_user`.

[source,console]
----
POST /_security/user/admin_user?refresh=true
{
  "password": "l0ng-r4nd0m-p@ssw0rd",
  "roles": [ "my_admin_role" ],
  "full_name": "Eirian Zola",
  "metadata": { "intelligence" : 7}
}
----

We can also create an analyst user and assign them the role named
`my_analyst_role`.

[source,console]
----
POST /_security/user/analyst_user?refresh=true
{
  "password": "l0nger-r4nd0mer-p@ssw0rd",
  "roles": [ "my_analyst_role" ],
  "full_name": "Monday Jaffe",
  "metadata": { "innovation" : 8}
}
----

You can then authenticate to {es} as the `admin_user` or `analyst_user`. However, the `admin_user` could optionally submit requests on
behalf of the `analyst_user`. The following request authenticates to {es} with a
`Basic` authorization token and submits the request as the `analyst_user`:

[source,sh]
----
curl -s -X GET -H "Authorization: Basic YWRtaW5fdXNlcjpsMG5nLXI0bmQwbS1wQHNzdzByZA==" -H "es-security-runas-user: analyst_user" https://localhost:9200/_security/_authenticate
----

The response indicates that the `analyst_user` submitted this request, using the
`my_analyst_role` that's assigned to that user. When the `admin_user` submitted
the request, {es} authenticated that user, discarded their roles, and then used
the roles of the `run_as` user.

[source,sh]
----
{"username":"analyst_user","roles":["my_analyst_role"],"full_name":"Monday Jaffe","email":null,
"metadata":{"innovation":8},"enabled":true,"authentication_realm":{"name":"native",
"type":"native"},"lookup_realm":{"name":"native","type":"native"},"authentication_type":"realm"}
%
----

The `authentication_realm` and `lookup_realm` in the response both specify
the `native` realm because both the `admin_user` and `analyst_user` are from
that realm. If the two users are in different realms, the values for
`authentication_realm` and `lookup_realm` are different (such as `pki` and
`native`).
""")
}
