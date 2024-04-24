/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc;

import spock.lang.Specification
import spock.lang.Unroll

import org.gradle.api.InvalidUserDataException

import static org.elasticsearch.gradle.internal.doc.AsciidocSnippetParser.finalizeSnippet;
import static org.elasticsearch.gradle.internal.doc.AsciidocSnippetParser.matchSource;

class AsciidocParserSpec extends Specification {

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

    @Unroll
    def "checks for valid json for #languageParam"() {
        given:
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
        def result = finalizeSnippet(snippet, json, [:], [:].entrySet())
        then:
        result != null

        when:
        finalizeSnippet(snippet, "some no valid json", [:], [:].entrySet())
        then:
        def e = thrown(InvalidUserDataException)
        e.message.contains("Invalid json in")

        when:
        snippet.skip = "true"
        result = finalizeSnippet(snippet, "some no valid json", [:], [:].entrySet())
        then:
        result != null

        where:
        languageParam << ["js", "console-result"]
    }

    def "test finalized snippet handles substitutions"() {
        given:
        def snippet = snippet() {
            language = "console"
        }
        when:
        finalizeSnippet(snippet, "snippet-content substDefault subst", [substDefault: "\$body"], [subst: 'substValue'].entrySet())
        then:
        snippet.contents == "snippet-content \$body substValue"
    }

    def snippetMustHaveLanguage() {
        given:
        def snippet = snippet()
        when:
        finalizeSnippet(snippet, "snippet-content", [:], [])
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
        finalizeSnippet(snippet, "snippet-content", [:], [])
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
        finalizeSnippet(snippet, "curl substDefault subst", [:], [:].entrySet())
        then:
        snippet.curl == true
    }

    def "test snippets with no curl no console"() {
        given:
        def snippet = snippet() {
            console = false
            language = "shell"
        }
        when:
        finalizeSnippet(snippet, "hello substDefault subst", [:], [:].entrySet())
        then:
        def e = thrown(InvalidUserDataException)
        e.message.contains("No need for NOTCONSOLE if snippet doesn't contain `curl`")
    }

    Snippet snippet(Closure<DocSnippetTask> configClosure = {}) {
        def snippet = new Snippet(new File("SomePath").toPath(), 0, "snippet-name-1")
        configClosure.delegate = snippet
        configClosure()
        return snippet
    }
}
