/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc

import spock.lang.Specification

import java.nio.file.Path

abstract class AbstractParserSpec extends Specification {
    abstract SnippetParser parser()

    def "can parse snippet with test setup"() {
        given:
        def snippetParser = parser()
        List<Snippet> snippets = []

        when:
        docSnippetWithTestSetup().eachLine { line, index ->
            snippetParser.parseLine(snippets, Path.of("some-path.mdx"), index, line)
        }

        then:
        snippets*.test() == [true]
        snippets*.setup() == ["seats"]
    }

    protected abstract String docSnippetWithTestSetup();
}
