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

import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.testfixtures.ProjectBuilder

import static org.elasticsearch.gradle.internal.doc.DocTestUtils.SAMPLE_TEST_DOCS
import static org.elasticsearch.gradle.internal.doc.RestTestsFromDocSnippetTask.replaceBlockQuote
import static org.elasticsearch.gradle.internal.doc.RestTestsFromDocSnippetTask.shouldAddShardFailureCheck
import static org.elasticsearch.gradle.internal.test.TestUtils.normalizeString

class RestTestsFromDocSnippetTaskSpec extends Specification {

    @TempDir
    File tempDir;

    def "test simple block quote"() {
        expect:
        replaceBlockQuote("\"foo\": \"\"\"bort baz\"\"\"") == "\"foo\": \"bort baz\""
    }

    def "test multiple block quotes"() {
        expect:
        replaceBlockQuote("\"foo\": \"\"\"bort baz\"\"\", \"bar\": \"\"\"other\"\"\"") == "\"foo\": \"bort baz\", \"bar\": \"other\""
    }

    def "test escaping in block quote"() {
        expect:
        replaceBlockQuote("\"foo\": \"\"\"bort\" baz\"\"\"") == "\"foo\": \"bort\\\" baz\""
        replaceBlockQuote("\"foo\": \"\"\"bort\n baz\"\"\"") == "\"foo\": \"bort\\n baz\""
    }

    def "test invalid block quotes"() {
        given:
        String input = "\"foo\": \"\"\"bar\"";
        when:
        RestTestsFromDocSnippetTask.replaceBlockQuote(input);
        then:
        def e = thrown(InvalidUserDataException)
        e.message == "Invalid block quote starting at 7 in:\n" + input
    }

    def "test is doc write request"() {
        expect:
        shouldAddShardFailureCheck("doc-index/_search") == true
        shouldAddShardFailureCheck("_cat") == false
        shouldAddShardFailureCheck("_ml/datafeeds/datafeed-id/_preview") == false
    }

    def "can generate tests files from asciidoc and mdx"() {
        given:
        def build = ProjectBuilder.builder().build()
        def task = build.tasks.register("restTestFromSnippet", RestTestsFromDocSnippetTask).get()
        task.expectedUnconvertedCandidates = []
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));
        docFile('docs/example-2-asciidoc.asciidoc', SAMPLE_TEST_DOCS['example-2.asciidoc'])
        docFile('docs/example-2-mdx.mdx', SAMPLE_TEST_DOCS['example-2.mdx'])
        task.getSetups().put(
            "seats", """
'''
  - do:
        indices.create:
          index: seats
          body:
            settings:
              number_of_shards: 1
              number_of_replicas: 0
            mappings:
              properties:
                theatre:
                  type: keyword
"""
        )
        when:
        task.getActions().forEach { it.execute(task) }

        then:
        def restSpecFromAsciidoc = new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/example-2-asciidoc.yml")
        def restSpecFromMdx = new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/example-2-mdx.yml")
        normalizeRestSpec(restSpecFromAsciidoc.text) == normalizeRestSpec(restSpecFromMdx.text)
    }

    def "task fails on same doc source file with supported different extension"() {
        given:
        def build = ProjectBuilder.builder().build()
        def task = build.tasks.register("restTestFromSnippet", RestTestsFromDocSnippetTask).get()
        task.expectedUnconvertedCandidates = []
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));
        docFile('docs/example-2.asciidoc', SAMPLE_TEST_DOCS['example-2.asciidoc'])
        docFile('docs/example-2.mdx', SAMPLE_TEST_DOCS['example-2.mdx'])
        task.getSetups().put(
            "seats", """
'''
  - do:
        indices.create:
          index: seats
          body:
            settings:
              number_of_shards: 1
              number_of_replicas: 0
            mappings:
              properties:
                theatre:
                  type: keyword
"""
        )
        when:
        task.getActions().forEach { it.execute(task) }

        then:
        def e = thrown(GradleException)
        e.message == "Found multiple files with the same name 'example-2' but different extensions: [asciidoc, mdx]"
    }

    def "can run in migration mode to compare same doc source file with supported different extension"() {
        given:
        def build = ProjectBuilder.builder().build()
        def task = build.tasks.register("restTestFromSnippet", RestTestsFromDocSnippetTask).get()
        task.expectedUnconvertedCandidates = []
        task.migrationMode = true
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));
        docFile('docs/example-2.asciidoc', SAMPLE_TEST_DOCS['example-2.asciidoc'])
        docFile('docs/example-2.mdx', SAMPLE_TEST_DOCS['example-2.mdx'])
        task.getSetups().put(
            "seats", """
'''
  - do:
        indices.create:
          index: seats
          body:
            settings:
              number_of_shards: 1
              number_of_replicas: 0
            mappings:
              properties:
                theatre:
                  type: keyword
"""
        )
        when:
        task.getActions().forEach { it.execute(task) }

        then:
        new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/example-2.asciidoc.yml").exists()
        new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/example-2.mdx.yml").exists()
    }

    def "fails in migration mode for same doc source file with different extension generates different spec"() {
        given:
        def build = ProjectBuilder.builder().build()
        def task = build.tasks.register("restTestFromSnippet", RestTestsFromDocSnippetTask).get()
        task.getMigrationMode().set(true)
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));
        docFile('docs/example-2.asciidoc', SAMPLE_TEST_DOCS['example-2.asciidoc'])
        docFile('docs/example-2.mdx', SAMPLE_TEST_DOCS['example-2-different.mdx'])
        task.getSetups().put(
            "seats", """
'''
  - do:
        indices.create:
          index: seats
          body:
            settings:
              number_of_shards: 1
              number_of_replicas: 0
            mappings:
              properties:
                theatre:
                  type: keyword
"""
        )
        when:
        task.getActions().forEach { it.execute(task) }

        then:
        new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/example-2.asciidoc.yml").exists()
        new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/example-2.mdx.yml").exists()
    }

    File docFile(String fileName, String docContent) {
        def file = tempDir.toPath().resolve(fileName).toFile()
        file.parentFile.mkdirs()
        file.text = docContent
        return file
    }

    String normalizeRestSpec(String inputString) {
        def withNormalizedLines = inputString.replaceAll(/"line_\d+":/, "\"line_0\":")
        return withNormalizedLines
    }
}
