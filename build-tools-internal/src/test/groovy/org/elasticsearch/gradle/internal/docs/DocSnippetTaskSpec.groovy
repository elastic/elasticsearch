/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.docs

import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import org.gradle.api.InvalidUserDataException
import org.gradle.testfixtures.ProjectBuilder

import static org.elasticsearch.gradle.internal.docs.DocSnippetTask.matchSource

class DocSnippetTaskSpec extends Specification {

    @TempDir
    File tempDir

    def testMatchSource() {
        expect:
        with(matchSource("[source,console]")) {
            matches == true;
            language == "console";
            name == null;
        }

        with(matchSource("[source,console,id=snippet-name-1]")) {
            matches == true;
            language == "console";
            name == "snippet-name-1";
        }

        with(matchSource("[source, console, id=snippet-name-1]")) {
            matches == true;
            language == "console";
            name == "snippet-name-1";
        }

        with(matchSource("[source, console, id=snippet-name-1]")) {
            matches == true;
            language == "console";
            name == "snippet-name-1";
        }

        with(matchSource("[source,console,attr=5,id=snippet-name-1,attr2=6]")) {
            matches == true;
            language == "console";
            name == "snippet-name-1";
        }

        with(matchSource("[source,console, attr=5, id=snippet-name-1, attr2=6]")) {
            matches == true;
            language == "console";
            name == "snippet-name-1";
        }

        with(matchSource("[\"source\",\"console\",id=\"snippet-name-1\"]")) {
            matches == true;
            language == "console";
            name == "snippet-name-1";
        }

        with(matchSource("[source,console,id=\"snippet-name-1\"]")) {
            matches == true;
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

    def snippetMustHaveLanguage() {
        given:
        def snippet = snippet()
        when:
        def task = ProjectBuilder.builder().build().tasks.create("docSnippetTask", DocSnippetTask)
        task.emit(snippet, "snippet-content", [:], [])
        then:
        def e = thrown(InvalidUserDataException)
        e.message.contains("Snippet missing a language.")
    }

    def testEmit() {
        given:
        def snippet = snippet() {
            language = "console"
        }
        when:
        def task = ProjectBuilder.builder().build().tasks.create("docSnippetTask", DocSnippetTask)
        task.emit(snippet, "snippet-content", [:], [])
        then:
        snippet.contents == "snippet-content"
    }

    def testSnippetsWithCurl() {
        given:
        def snippet = snippet() {
            language = "sh"
            name = "snippet-name-1"
        }
        when:
        def task = ProjectBuilder.builder().build().tasks.create("docSnippetTask", DocSnippetTask)
        task.emit(snippet, "curl substDefault subst", [:], [:].entrySet())
        then:
        snippet.curl == true
    }

    def testSnippetsWithNoCurlNoConsole() {
        given:
        def snippet = snippet() {
            console = false
            language = "shell"
        }
        when:
        def task = ProjectBuilder.builder().build().tasks.create("docSnippetTask", DocSnippetTask)
        task.emit(snippet, "hello substDefault subst", [:], [:].entrySet())
        then:
        def e = thrown(InvalidUserDataException)
        e.message.contains("No need for NOTCONSOLE if snippet doesn't contain `curl`")
    }

    @Unroll
    def "checks for valid json for #languageParam"() {
        given:
        def task = ProjectBuilder.builder().build().tasks.create("docSnippetTask", DocSnippetTask)
        def snippet = snippet() {
            language = languageParam
            testResponse = true
        }
        def json = """{
    "name": "John Doe",
    "age": 30,
    "isMarried": true,
    "address": {
        "street": "123 Main Street",
        "city": "Springfield",
        "state": "IL",
        "zip": "62701"
    },
    "hobbies": ["Reading", "Cooking", "Traveling"]
}"""
        when:
        def result = task.emit(snippet, json, [:], [:].entrySet())
        then:
        result != null

        when:
        task.emit(snippet, "some no valid json", [:], [:].entrySet())
        then:
        def e = thrown(InvalidUserDataException)
        e.message.contains("Invalid json in")

        when:
        snippet.skip = "true"
        result = task.emit(snippet, "some no valid json", [:], [:].entrySet())
        then:
        result != null

        where:
        languageParam << ["js", "console-result"]
    }

    def testEmitSubstitutes() {
        given:
        def task = ProjectBuilder.builder().build().tasks.create("docSnippetTask", DocSnippetTask)
        def snippet = snippet() {
            language = "console"
        }
        when:
        task.emit(snippet, "snippet-content substDefault subst", [substDefault: "\$body"], [subst: 'substValue'].entrySet())
        then:
        snippet.contents == "snippet-content \$body substValue"
    }

    def "handling test parsing"() {
        given:
        def project = ProjectBuilder.builder().build()
        def task = project.tasks.create("docSnippetTask", DocSnippetTask)
        when:
        def substitutions = []
        def snippets = task.parseDocFile(
            tempDir, docFile(
            """
[source,console]
----
POST logs-my_app-default/_rollover/
----
// TEST[s/_explain\\/1/_explain\\/1?error_trace=false/ catch:/painless_explain_error/]
"""
        ), substitutions)
        then:
        snippets*.test == [true]
        snippets*.catchPart == ["/painless_explain_error/"]
        substitutions.size() == 1
        substitutions[0].key == "_explain\\/1"
        substitutions[0].value == "_explain\\/1?error_trace=false"

        when:
        substitutions = []
        snippets = task.parseDocFile(
            tempDir, docFile(
            """

[source,console]
----
PUT _snapshot/my_hdfs_repository
{
  "type": "hdfs",
  "settings": {
    "uri": "hdfs://namenode:8020/",
    "path": "elasticsearch/repositories/my_hdfs_repository",
    "conf.dfs.client.read.shortcircuit": "true"
  }
}
----
// TEST[skip:we don't have hdfs set up while testing this]
"""
        ), substitutions)
        then:
        snippets*.test == [true]
        snippets*.skip == ["we don't have hdfs set up while testing this"]
    }

    def "handling testresponse parsing"() {
        given:
        def project = ProjectBuilder.builder().build()
        def task = project.tasks.create("docSnippetTask", DocSnippetTask)
        when:
        def substitutions = []
        def snippets = task.parseDocFile(
            tempDir, docFile(
            """
[source,console]
----
POST logs-my_app-default/_rollover/
----
// TESTRESPONSE[s/"root_cause": \\.\\.\\./"root_cause": \$body.error.root_cause/]
"""
        ), substitutions)
        then:
        snippets*.test == [false]
        snippets*.testResponse == [true]
        substitutions.size() == 1
        substitutions[0].key == "\"root_cause\": \\.\\.\\."
        substitutions[0].value == "\"root_cause\": \$body.error.root_cause"

        when:
        snippets = task.parseDocFile(
            tempDir, docFile(
            """
[source,console]
----
POST logs-my_app-default/_rollover/
----
// TESTRESPONSE[skip:no setup made for this example yet]
"""
        ), [])
        then:
        snippets*.test == [false]
        snippets*.testResponse == [true]
        snippets*.skip == ["no setup made for this example yet"]

        when:
        substitutions = []
        snippets = task.parseDocFile(
            tempDir, docFile(
            """
[source,txt]
---------------------------------------------------------------------------
my-index-000001 0 p RELOCATING 3014 31.1mb 192.168.56.10 H5dfFeA -> -> 192.168.56.30 bGG90GE
---------------------------------------------------------------------------
// TESTRESPONSE[non_json]
"""
        ), substitutions)
        then:
        snippets*.test == [false]
        snippets*.testResponse == [true]
        substitutions.size() == 4
    }


    def "handling console parsing"() {
        given:
        def project = ProjectBuilder.builder().build()
        def task = project.tasks.create("docSnippetTask", DocSnippetTask)
        when:
        def snippets = task.parseDocFile(
            tempDir, docFile(
            """
[source,console]
----

// $firstToken
----
"""
        ),[])
        then:
        snippets*.console == [firstToken.equals("CONSOLE")]


        when:
        task.parseDocFile(
            tempDir, docFile(
            """
[source,console]
----
// $firstToken
// $secondToken
----
"""
        ), [])
        then:
        def e = thrown(InvalidUserDataException)
        e.message == "mapping-charfilter.asciidoc:4: Can't be both CONSOLE and NOTCONSOLE"

        when:
        task.parseDocFile(
            tempDir, docFile(
            """
// $firstToken
// $secondToken
"""
        ), []
        )
        then:
        e = thrown(InvalidUserDataException)
        e.message == "mapping-charfilter.asciidoc:1: $firstToken not paired with a snippet"

        where:
        firstToken << ["CONSOLE", "NOTCONSOLE"]
        secondToken << ["NOTCONSOLE", "CONSOLE"]
    }

    def "test parsing snippet from doc"() {
        given:
        def project = ProjectBuilder.builder().build()
        def task = project.tasks.create("docSnippetTask", DocSnippetTask)
        def doc = docFile(
            """
[source,console]
----
GET /_analyze
{
  "tokenizer": "keyword",
  "char_filter": [
    {
      "type": "mapping",
      "mappings": [
        "٠ => 0",
        "١ => 1",
        "٢ => 2"
      ]
    }
  ],
  "text": "My license plate is ٢٥٠١٥"
}
----
"""
        )
        def snippets = task.parseDocFile(tempDir, doc, [])
        expect:
        snippets*.start == [2]
        snippets*.language == ["console"]
        snippets*.contents == ["""GET /_analyze
{
  "tokenizer": "keyword",
  "char_filter": [
    {
      "type": "mapping",
      "mappings": [
        "٠ => 0",
        "١ => 1",
        "٢ => 2"
      ]
    }
  ],
  "text": "My license plate is ٢٥٠١٥"
}
"""]
    }


    File docFile(String docContent) {
        def file = tempDir.toPath().resolve("mapping-charfilter.asciidoc").toFile()
        file.text = docContent
        return file
    }

    Snippet snippet(Closure<DocSnippetTask> configClosure = {}) {
        def snippet = new Snippet(new File("SomePath").toPath(), 0, "snippet-name-1")
        configClosure.delegate = snippet
        configClosure()
        return snippet
    }
}
