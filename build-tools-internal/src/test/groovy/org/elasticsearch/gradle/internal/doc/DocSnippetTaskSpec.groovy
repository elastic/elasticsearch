/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc

import spock.lang.Specification
import spock.lang.TempDir

import org.gradle.api.InvalidUserDataException
import org.gradle.testfixtures.ProjectBuilder

import static DocTestUtils.SAMPLE_TEST_DOCS
import static org.elasticsearch.gradle.internal.test.TestUtils.normalizeString

class DocSnippetTaskSpec extends Specification {

    @TempDir
    File tempDir

    def "handling test parsing multiple snippets per file"() {
        given:
        def project = ProjectBuilder.builder().build()
        def task = project.tasks.register("docSnippetTask", DocSnippetTask).get()
        when:
        def snippets = task.parseDocFile(
            tempDir, docFile(
            "mapping-charfilter.asciidoc",
                DocTestUtils.SAMPLE_TEST_DOCS["mapping-charfilter.asciidoc"]
            )
        )
        then:
        snippets*.test == [false, false, false, false, false, false, false]
        snippets*.catchPart == [null, null, null, null, null, null, null]
    }

    def "handling console parsing"() {
        when:
        def snippets = task().parseDocFile(
            tempDir, docFile(
            "mapping-charfilter.asciidoc",
            """
[source,console]
----

// $firstToken
----
"""
        ))
        then:
        snippets*.console == [firstToken.equals("CONSOLE")]


        when:
        task().parseDocFile(
            tempDir, docFile(
            "mapping-charfilter.asciidoc",
            """
[source,console]
----
// $firstToken
// $secondToken
----
"""
        ))
        then:
        def e = thrown(InvalidUserDataException)
        e.message == "mapping-charfilter.asciidoc:4: Can't be both CONSOLE and NOTCONSOLE"

        when:
        task().parseDocFile(
            tempDir, docFile(
            "mapping-charfilter.asciidoc",
            """
// $firstToken
// $secondToken
"""
        ))
        then:
        e = thrown(InvalidUserDataException)
        e.message == "mapping-charfilter.asciidoc:1: $firstToken not paired with a snippet"

        where:
        firstToken << ["CONSOLE", "NOTCONSOLE"]
        secondToken << ["NOTCONSOLE", "CONSOLE"]
    }

    def "test parsing snippet from doc"() {
        def doc = docFile(
            "mapping-charfilter.asciidoc",
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
        "e => 0",
        "m => 1",
        "p => 2",
        "t => 3",
        "y => 4"
      ]
    }
  ],
  "text": "My license plate is empty"
}
----
"""
        )
        def snippets = task().parseDocFile(tempDir, doc)
        expect:
        snippets[0].start == 3
        snippets[0].language == "console"
        normalizeString(snippets[0].contents, tempDir) == """GET /_analyze
{
  "tokenizer": "keyword",
  "char_filter": [
    {
      "type": "mapping",
      "mappings": [
        "e => 0",
        "m => 1",
        "p => 2",
        "t => 3",
        "y => 4"
      ]
    }
  ],
  "text": "My license plate is empty"
}"""
    }

    def "produces same snippet from mdx and asciidoc"() {
        def mdx = docFile(
            "painless-field-context.mdx", SAMPLE_TEST_DOCS["painless-field-context.mdx"]
        )
        def asciiDoc = docFile(
            "painless-field-context.asciidoc", SAMPLE_TEST_DOCS["painless-field-context.asciidoc"]
        )
        def asciidocSnippets = task().parseDocFile(tempDir, asciiDoc)

        expect:
        asciidocSnippets.size() == 4
        asciidocSnippets[0].start == 45
        asciidocSnippets[0].language == "Painless"
        normalizeString(asciidocSnippets[0].contents, tempDir) ==
            "doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)"

        when:
        def mdxSnippets = task().parseDocFile(tempDir, mdx)
        then:
        mdxSnippets.size() == 4
        mdxSnippets[0].start == 49
        mdxSnippets[0].language == "Painless"
        normalizeString(mdxSnippets[0].contents, tempDir) ==
            "doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)"

        assertSnippetsEqual(normalizeSnippets(asciidocSnippets), normalizeSnippets(mdxSnippets))
    }

    File docFile(String filename, String docContent) {
        def file = tempDir.toPath().resolve(filename).toFile()
        file.text = docContent
        return file
    }


    private DocSnippetTask task() {
        ProjectBuilder.builder().build().tasks.register("docSnippetTask", DocSnippetTask).get()
    }

    boolean assertSnippetsEqual(List<Snippet> snippets1, List<Snippet> snippets2) {
        assert snippets1.size() == snippets2.size()
        assert snippets1 == snippets2
        true
    }

    List<Snippet> normalizeSnippets(List<Snippet> snippets) {
        return snippets.collect { Snippet s ->
            def orgPath = s.path()
            def fileName = orgPath.fileName.toString() - ".asciidoc" - ".mdx" + ".xdoc";
            def normalizedPath = orgPath.parent == null ? new File(fileName).toPath() : orgPath.parent.resolve(fileName)
            new Snippet(
                normalizedPath,
                0,
                10,
                s.contents,
                s.console,
                s.test,
                s.testResponse,
                s.testSetup,
                s.testTearDown,
                s.skip,
                s.continued,
                s.language,
                s.catchPart,
                s.setup,
                s.teardown,
                s.curl,
                s.warnings,
                s.skipShardsFailures,
                s.name() ? s.name() - "asciidoc" : null
            )
        }
    }
}
