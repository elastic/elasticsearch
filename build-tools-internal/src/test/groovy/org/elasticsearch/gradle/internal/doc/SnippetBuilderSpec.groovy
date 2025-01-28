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
import spock.lang.Unroll

import org.gradle.api.InvalidUserDataException

class SnippetBuilderSpec extends Specification {

    @Unroll
    def "checks for valid json for #languageParam"() {
        when:
        def snippet1 = snippetBuilder().withLanguage(languageParam).withTestResponse(true).withConsole(true)
            .withContent(
                """{
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
            ).build()
        then:
        snippet1 != null


        when:
        snippetBuilder().withLanguage(languageParam).withTestResponse(true).withConsole(true)
            .withContent(
                "some no valid json"
            ).build()

        then:
        def e = thrown(InvalidUserDataException)
        e.message.contains("Invalid json in")

        when:
        def snippet2 = snippetBuilder().withLanguage(languageParam).withTestResponse(true).withConsole(true)
            .withSkip("skipping")
            .withContent(
                "some no valid json"
            ).build()

        then:
        snippet2 != null

        where:
        languageParam << ["js", "console-result"]
    }

    def "language must be defined"() {
        when:
        snippetBuilder().withContent("snippet-content").build()
        then:
        def e = thrown(InvalidUserDataException)
        e.message.contains("Snippet missing a language.")
    }

    def "handles snippets with curl"() {
        expect:
        snippetBuilder().withLanguage("sh")
            .withName("snippet-name-1")
            .withContent("curl substDefault subst")
            .build()
            .curl() == true

    }

    def "snippet builder handles substitutions"() {
        when:
        def snippet = snippetBuilder().withLanguage("console").withContent("snippet-content substDefault subst")
            .withSubstitutions([substDefault: "\$body", subst: 'substValue']).build()

        then:
        snippet.contents == "snippet-content \$body substValue"
    }

    def "test snippets with no curl no console"() {
        when:
        snippetBuilder()
            .withConsole(false)
            .withLanguage("shell")
            .withContent("hello substDefault subst")
            .build()
        then:
        def e = thrown(InvalidUserDataException)
        e.message.contains("No need for NOTCONSOLE if snippet doesn't contain `curl`")
    }

    SnippetBuilder snippetBuilder() {
        return new SnippetBuilder()
    }

}
