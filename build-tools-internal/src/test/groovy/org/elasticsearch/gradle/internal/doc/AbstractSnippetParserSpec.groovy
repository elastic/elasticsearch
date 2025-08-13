/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc

import spock.lang.Specification

import org.gradle.api.InvalidUserDataException

import java.nio.file.Path

abstract class AbstractSnippetParserSpec extends Specification {

    abstract SnippetParser parser()
    abstract String docSnippetWithTestResponses()
    abstract String docSnippetWithTest()
    abstract String docSnippetWithRepetitiveSubstiutions()
    abstract String docSnippetWithConsole()
    abstract String docSnippetWithNotConsole()
    abstract String docSnippetWithMixedConsoleNotConsole()

    def "can parse snippet with console"() {
        when:
        def snippets = parse(docSnippetWithConsole())
        then:
        snippets*.console() == [true]
    }

    def "can parse snippet with notconsole"() {
        when:
        def snippets = parse(docSnippetWithNotConsole())
        then:
        snippets*.console() == [false]
    }

    def "fails on mixing console and notconsole"() {
        when:
        def snippets = parse(docSnippetWithMixedConsoleNotConsole())
        then:
        def e = thrown(SnippetParserException)
        e.message.matches("Error parsing snippet in acme.xyz at line \\d")
        e.file.name == "acme.xyz"
        e.lineNumber > 0
    }

    def "can parse snippet with test"() {
        when:
        def snippets = parse(docSnippetWithTest())
        then:
        snippets*.test() == [true]
        snippets*.testResponse() == [false]
        snippets*.language() == ["console"]
        snippets*.catchPart() == ["/painless_explain_error/"]
        snippets*.teardown() == ["some_teardown"]
        snippets*.setup() == ["seats"]
        snippets*.warnings() == [["some_warning"]]
        snippets*.contents() == ["""PUT /hockey/_doc/1?refresh
{"first":"johnny","last":"gaudreau","goals":[9,27,1],"assists":[17,46,0],"gp":[26,82,1]}

POST /hockey/_explain/1?error_trace=false
{
  "query": {
    "script": {
      "script": "Debug.explain(doc.goals)"
    }
  }
}
"""]
    }

    def "can parse snippet with test responses"() {
        when:
        def snippets = parse(docSnippetWithTestResponses())
        then:
        snippets*.testResponse() == [true]
        snippets*.test() == [false]
        snippets*.language() == ["console-result"]
        snippets*.skip() == ["some_skip_message"]
        snippets*.contents() == ["""{
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
              "timestamp" : \$body.docs.0.processor_results.0.doc._ingest.timestamp
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
              "timestamp" : \$body.docs.0.processor_results.0.doc._ingest.timestamp
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
              "timestamp" : \$body.docs.1.processor_results.0.doc._ingest.timestamp
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
              "timestamp" : \$body.docs.1.processor_results.0.doc._ingest.timestamp
            }
          }
        }
      ]
    }
  ]
}
"""]
    }

    def "can parse snippet with repetitive regex substitutions"() {
        when:
        def snippets = parse(docSnippetWithRepetitiveSubstiutions())
        then:
        snippets*.test() == [true]
        snippets*.testResponse() == [false]
        snippets*.language() == ["console"]
        snippets*.contents() == ["""PUT /_snapshot/repo1
{"type": "fs", "settings": {"location": "repo/1"}}
PUT /_snapshot/repo1/snap2?wait_for_completion=true
PUT /_snapshot/repo1/snap1?wait_for_completion=true
GET /_cat/snapshots/repo1?v=true&s=id
"""]
    }

    List<Snippet> parse(String docSnippet) {
        List<Snippet> snippets = new ArrayList<>()
        def lines = docSnippet.lines().toList()
        parser().parseLines(new File("acme.xyz"), lines, snippets)
        return snippets
    }

}
