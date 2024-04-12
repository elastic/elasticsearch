/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc

import spock.lang.Specification

import static org.elasticsearch.gradle.internal.doc.AsciidocSnippetParser.matchSource

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

}
