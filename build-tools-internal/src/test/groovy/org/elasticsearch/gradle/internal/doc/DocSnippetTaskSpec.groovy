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
import spock.lang.TempDir
import spock.lang.Unroll

import org.gradle.testfixtures.ProjectBuilder

import static org.elasticsearch.gradle.internal.test.TestUtils.normalizeString

class DocSnippetTaskSpec extends Specification {

    @TempDir
    File tempDir

    @Unroll
    def "handling test parsing multiple snippets per #fileType file"() {
        when:
        def snippets = parseFile("example-1.$fileType")

        then:
        snippets*.test == [false, false, false, false, false, false, false]
        snippets*.catchPart == [null, null, null, null, null, null, null]
        snippets*.setup == [null, null, null, null, null, null, null]
        snippets*.teardown == [null, null, null, null, null, null, null]
        snippets*.testResponse == [false, false, false, false, false, false, false]
        snippets*.skip == [null, null, null, null, null, null, null]
        snippets*.continued == [false, false, false, false, false, false, false]
        snippets*.language == ["console", "js", "js", "console", "console", "console", "console"]
        snippets*.contents*.empty == [false, false, false, false, false, false, false]
        snippets*.start == expectedSnippetStarts
        snippets*.end == expectedSnippetEnds

        // test two snippet explicitly for content.
        // More coverage on actual parsing is done in unit tests
        normalizeString(snippets[0].contents) == """PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_field": {
        "type": "annotated_text"
      }
    }
  }
}"""

        normalizeString(snippets[1].contents) == """GET my-index-000001/_analyze
{
  "field": "my_field",
  "text":"Investors in [Apple](Apple+Inc.) rejoiced."
}"""

        where:
        fileType << ["asciidoc", "mdx"]
        expectedSnippetStarts << [[10, 24, 36, 59, 86, 108, 135], [9, 22, 33, 55, 80, 101, 127]]
        expectedSnippetEnds << [[21, 30, 55, 75, 105, 132, 158], [20, 28, 52, 71, 99, 125, 150]]
    }

    List<Snippet> parseFile(String fileName) {
        def task = ProjectBuilder.builder().build().tasks.register("docSnippetTask", DocSnippetTask).get()
        def docFileToParse = docFile(fileName, DocTestUtils.SAMPLE_TEST_DOCS[fileName])
        return task.parseDocFile(
            tempDir, docFileToParse
        )
    }

    File docFile(String filename, String docContent) {
        def file = tempDir.toPath().resolve(filename).toFile()
        file.text = docContent
        return file
    }
}
