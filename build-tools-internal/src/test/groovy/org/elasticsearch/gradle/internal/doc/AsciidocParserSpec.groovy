/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc

import static org.elasticsearch.gradle.internal.doc.AsciidocSnippetParser.matchSource

class AsciidocParserSpec extends AbstractSnippetParserSpec {

    def testMatchSource() {
        expect:
        with(matchSource("[source,console]")) {
            matches == true
            language == "console"
            name == null
        }

        with(matchSource("[source,console,id=snippet-name-1]")) {
            matches == true
            language == "console"
            name == "snippet-name-1"
        }

        with(matchSource("[source, console, id=snippet-name-1]")) {
            matches == true
            language == "console"
            name == "snippet-name-1"
        }

        with(matchSource("[source, console, id=snippet-name-1]")) {
            matches == true
            language == "console"
            name == "snippet-name-1"
        }

        with(matchSource("[source,console,attr=5,id=snippet-name-1,attr2=6]")) {
            matches == true
            language == "console"
            name == "snippet-name-1"
        }

        with(matchSource("[source,console, attr=5, id=snippet-name-1, attr2=6]")) {
            matches == true
            language == "console"
            name == "snippet-name-1"
        }

        with(matchSource("[\"source\",\"console\",id=\"snippet-name-1\"]")) {
            matches == true
            language == "console"
            name == "snippet-name-1"
        }

        with(matchSource("[source,console,id=\"snippet-name-1\"]")) {
            matches == true
            language == "console"
            name == "snippet-name-1"
        }
        with(matchSource("[source.merge.styled,esql]")) {
            matches == true
            language == "esql"
        }

        with(matchSource("[source.merge.styled,foo-bar]")) {
            matches == true
            language == "foo-bar"
        }
    }

    @Override
    SnippetParser parser() {
        return new AsciidocSnippetParser([:]);
    }

    @Override
    String docSnippetWithTest() {
        return """[source,console]
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
// TEST[teardown:some_teardown]
// TEST[setup:seats]
// TEST[warning:some_warning]
// TEST[skip_shard_failures]

"""
    }

    @Override
    String docSnippetWithRepetitiveSubstiutions() {
        return """
[source,console]
--------------------------------------------------
GET /_cat/snapshots/repo1?v=true&s=id
--------------------------------------------------
// TEST[s/^/PUT \\/_snapshot\\/repo1\\/snap1?wait_for_completion=true\\n/]
// TEST[s/^/PUT \\/_snapshot\\/repo1\\/snap2?wait_for_completion=true\\n/]
// TEST[s/^/PUT \\/_snapshot\\/repo1\\n{"type": "fs", "settings": {"location": "repo\\/1"}}\\n/]
"""
    }

    @Override
    String docSnippetWithConsole() {
        return """
[source,console]
----
// CONSOLE
----
"""
    }

    @Override
    String docSnippetWithNotConsole() {
        return """
[source,console]
----
// NOTCONSOLE
----
"""
    }

    @Override
    String docSnippetWithMixedConsoleNotConsole() {
        return """
[source,console]
----
// NOTCONSOLE
// CONSOLE
----
"""
    }

    @Override
    String docSnippetWithTestResponses() {
        return """
[source,console-result]
----
{
  "docs" : [
    {
      "processor_results" : [
        {
          "processor_type" : "set",
          "status" : "success",
          "doc" : {
            "_index" : "index",
            "_id" : "id",
            "_version": "-3",
            "_source" : {
              "field2" : "_value2",
              "foo" : "bar"
            },
            "_ingest" : {
              "pipeline" : "_simulate_pipeline",
              "timestamp" : "2020-07-30T01:21:24.251836Z"
            }
          }
        },
        {
          "processor_type" : "set",
          "status" : "success",
          "doc" : {
            "_index" : "index",
            "_id" : "id",
            "_version": "-3",
            "_source" : {
              "field3" : "_value3",
              "field2" : "_value2",
              "foo" : "bar"
            },
            "_ingest" : {
              "pipeline" : "_simulate_pipeline",
              "timestamp" : "2020-07-30T01:21:24.251836Z"
            }
          }
        }
      ]
    },
    {
      "processor_results" : [
        {
          "processor_type" : "set",
          "status" : "success",
          "doc" : {
            "_index" : "index",
            "_id" : "id",
            "_version": "-3",
            "_source" : {
              "field2" : "_value2",
              "foo" : "rab"
            },
            "_ingest" : {
              "pipeline" : "_simulate_pipeline",
              "timestamp" : "2020-07-30T01:21:24.251863Z"
            }
          }
        },
        {
          "processor_type" : "set",
          "status" : "success",
          "doc" : {
            "_index" : "index",
            "_id" : "id",
            "_version": "-3",
            "_source" : {
              "field3" : "_value3",
              "field2" : "_value2",
              "foo" : "rab"
            },
            "_ingest" : {
              "pipeline" : "_simulate_pipeline",
              "timestamp" : "2020-07-30T01:21:24.251863Z"
            }
          }
        }
      ]
    }
  ]
}
----
// TESTRESPONSE[s/"2020-07-30T01:21:24.251836Z"/\$body.docs.0.processor_results.0.doc._ingest.timestamp/]
// TESTRESPONSE[s/"2020-07-30T01:21:24.251836Z"/\$body.docs.0.processor_results.1.doc._ingest.timestamp/]
// TESTRESPONSE[s/"2020-07-30T01:21:24.251863Z"/\$body.docs.1.processor_results.0.doc._ingest.timestamp/]
// TESTRESPONSE[s/"2020-07-30T01:21:24.251863Z"/\$body.docs.1.processor_results.1.doc._ingest.timestamp/]
// TESTRESPONSE[skip:some_skip_message]
"""
    }

}
