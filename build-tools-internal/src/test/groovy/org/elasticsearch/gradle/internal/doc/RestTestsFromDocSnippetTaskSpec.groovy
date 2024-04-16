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

    def "can create rest tests from asciidoc docs"() {
        def build = ProjectBuilder.builder().build()
        given:
        def task = build.tasks.create("restTestFromSnippet", RestTestsFromDocSnippetTask)
        task.expectedUnconvertedCandidates = ["ml-update-snapshot.asciidoc", "reference/security/authorization/run-as-privilege.asciidoc"]
        sampleDocs()
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));

        when:
        task.getActions().forEach { it.execute(task) }
        def restSpec = new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-debugging.yml")

        then:
        restSpec.exists()
        normalizeString(restSpec.text, tempDir) == """---
"line_22":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: PUT
        path: "hockey/_doc/1"
        refresh: ""
        body: |
          {"first":"johnny","last":"gaudreau","goals":[9,27,1],"assists":[17,46,0],"gp":[26,82,1]}
  - is_false: _shards.failures
  - do:
      catch: /painless_explain_error/
      raw:
        method: POST
        path: "hockey/_explain/1"
        error_trace: "false"
        body: |
          {
            "query": {
              "script": {
                "script": "Debug.explain(doc.goals)"
              }
            }
          }
  - is_false: _shards.failures
  - match:
      \$body:
        {
           "error": {
              "type": "script_exception",
              "to_string": "[1, 9, 27]",
              "painless_class": "org.elasticsearch.index.fielddata.ScriptDocValues.Longs",
              "java_class": "org.elasticsearch.index.fielddata.ScriptDocValues\$Longs",
              "script_stack": \$body.error.script_stack, "script": \$body.error.script, "lang": \$body.error.lang, "position": \$body.error.position, "caused_by": \$body.error.caused_by, "root_cause": \$body.error.root_cause, "reason": \$body.error.reason
           },
           "status": 400
        }
  - do:
      catch: /painless_explain_error/
      raw:
        method: POST
        path: "hockey/_update/1"
        error_trace: "false"
        body: |
          {
            "script": "Debug.explain(ctx._source)"
          }
  - is_false: _shards.failures
  - match:
      \$body:
        {
          "error" : {
            "root_cause": \$body.error.root_cause,
            "type": "illegal_argument_exception",
            "reason": "failed to execute script",
            "caused_by": {
              "type": "script_exception",
              "to_string": \$body.error.caused_by.to_string,
              "painless_class": "java.util.LinkedHashMap",
              "java_class": "java.util.LinkedHashMap",
              "script_stack": \$body.error.caused_by.script_stack, "script": \$body.error.caused_by.script, "lang": \$body.error.caused_by.lang, "position": \$body.error.caused_by.position, "caused_by": \$body.error.caused_by.caused_by, "reason": \$body.error.caused_by.reason
            }
          },
          "status": 400
        }"""
        def restSpec2 = new File(task.testRoot.get().getAsFile(), "rest-api-spec/test/ml-update-snapshot.yml")
        restSpec2.exists()
        normalizeString(restSpec2.text, tempDir) == """---
"line_50":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
        - always_skip
      reason: todo
  - do:
      raw:
        method: POST
        path: "_ml/anomaly_detectors/it_ops_new_logs/model_snapshots/1491852978/_update"
        body: |
          {
            "description": "Snapshot 1",
            "retain": true
          }
  - is_false: _shards.failures"""
        def restSpec3 = new File(task.testRoot.get().getAsFile(), "rest-api-spec/test/reference/sql/getting-started.yml")
        restSpec3.exists()
        normalizeString(restSpec3.text, tempDir) == """---
"line_10":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: PUT
        path: "library/_bulk"
        refresh: ""
        body: |
          {"index":{"_id": "Leviathan Wakes"}}
          {"name": "Leviathan Wakes", "author": "James S.A. Corey", "release_date": "2011-06-02", "page_count": 561}
          {"index":{"_id": "Hyperion"}}
          {"name": "Hyperion", "author": "Dan Simmons", "release_date": "1989-05-26", "page_count": 482}
          {"index":{"_id": "Dune"}}
          {"name": "Dune", "author": "Frank Herbert", "release_date": "1965-06-01", "page_count": 604}
  - is_false: _shards.failures
  - do:
      raw:
        method: POST
        path: "_sql"
        format: "txt"
        body: |
          {
            "query": "SELECT * FROM library WHERE release_date < '2000-01-01'"
          }
  - is_false: _shards.failures
  - match:
      \$body:
        /    /s+author     /s+/|     /s+name      /s+/|  /s+page_count   /s+/| /s+release_date/s*
         ---------------/+---------------/+---------------/+------------------------/s*
         Dan /s+Simmons    /s+/|Hyperion       /s+/|482            /s+/|1989-05-26T00:00:00.000Z/s*
         Frank /s+Herbert  /s+/|Dune           /s+/|604            /s+/|1965-06-01T00:00:00.000Z/s*/"""
        def restSpec4 = new File(
            task.testRoot.get().getAsFile(),
            "rest-api-spec/test/reference/security/authorization/run-as-privilege.yml"
        )
        restSpec4.exists()
        normalizeString(restSpec4.text, tempDir) == """---
"line_51":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/role/my_director"
        refresh: "true"
        body: |
          {
            "cluster": ["manage"],
            "indices": [
              {
                "names": [ "index1", "index2" ],
                "privileges": [ "manage" ]
              }
            ],
            "run_as": [ "jacknich", "rdeniro" ],
            "metadata" : {
              "version" : 1
            }
          }
  - is_false: _shards.failures
---
"line_114":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/role/my_admin_role"
        refresh: "true"
        body: |
          {
            "cluster": ["manage"],
            "indices": [
              {
                "names": [ "index1", "index2" ],
                "privileges": [ "manage" ]
              }
            ],
            "applications": [
              {
                "application": "myapp",
                "privileges": [ "admin", "read" ],
                "resources": [ "*" ]
              }
            ],
            "run_as": [ "analyst_user" ],
            "metadata" : {
              "version" : 1
            }
          }
  - is_false: _shards.failures
---
"line_143":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/role/my_analyst_role"
        refresh: "true"
        body: |
          {
            "cluster": [ "monitor"],
            "indices": [
              {
                "names": [ "index1", "index2" ],
                "privileges": ["manage"]
              }
            ],
            "applications": [
              {
                "application": "myapp",
                "privileges": [ "read" ],
                "resources": [ "*" ]
              }
            ],
            "metadata" : {
              "version" : 1
            }
          }
  - is_false: _shards.failures
---
"line_170":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/user/admin_user"
        refresh: "true"
        body: |
          {
            "password": "l0ng-r4nd0m-p@ssw0rd",
            "roles": [ "my_admin_role" ],
            "full_name": "Eirian Zola",
            "metadata": { "intelligence" : 7}
          }
  - is_false: _shards.failures
---
"line_184":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/user/analyst_user"
        refresh: "true"
        body: |
          {
            "password": "l0nger-r4nd0mer-p@ssw0rd",
            "roles": [ "my_analyst_role" ],
            "full_name": "Monday Jaffe",
            "metadata": { "innovation" : 8}
          }
  - is_false: _shards.failures"""
    }

    def "can generate tests files from asciidoc and mdx"() {
        given:
        def build = ProjectBuilder.builder().build()
        def task = build.tasks.register("restTestFromSnippet", RestTestsFromDocSnippetTask).get()
        task.expectedUnconvertedCandidates = []
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));
        docFile('docs/painless-field-context-asciidoc.asciidoc', SAMPLE_TEST_DOCS['painless-field-context.asciidoc'])
        docFile('docs/painless-field-context-mdx.mdx', SAMPLE_TEST_DOCS['painless-field-context.mdx'])
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
        def restSpecFromAsciidoc = new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-field-context-asciidoc.yml")
        def restSpecFromMdx = new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-field-context-mdx.yml")
        normalizeRestSpec(restSpecFromAsciidoc.text) == normalizeRestSpec(restSpecFromMdx.text)
    }

    def "task fails on same doc source file with supported different extension"() {
        given:
        def build = ProjectBuilder.builder().build()
        def task = build.tasks.register("restTestFromSnippet", RestTestsFromDocSnippetTask).get()
        task.expectedUnconvertedCandidates = []
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));
        docFile('docs/painless-field-context.asciidoc', SAMPLE_TEST_DOCS['painless-field-context.asciidoc'])
        docFile('docs/painless-field-context.mdx', SAMPLE_TEST_DOCS['painless-field-context.mdx'])
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
        def restSpecFromAsciidoc = new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-field-context-asciidoc.yml")
        def restSpecFromMdx = new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-field-context-mdx.yml")
        def e = thrown(GradleException)
        e.message == "Found multiple files with the same name 'painless-field-context' but different extensions: [asciidoc, mdx]"
        //normalizeRestSpec(restSpecFromAsciidoc.text) == normalizeRestSpec(restSpecFromMdx.text)
    }

    def "can run in migration mode to compare same doc source file with supported different extension"() {
        given:
        def build = ProjectBuilder.builder().build()
        def task = build.tasks.register("restTestFromSnippet", RestTestsFromDocSnippetTask).get()
        task.expectedUnconvertedCandidates = []
        task.migrationMode = true
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));
        docFile('docs/painless-field-context.asciidoc', SAMPLE_TEST_DOCS['painless-field-context.asciidoc'])
        docFile('docs/painless-field-context.mdx', SAMPLE_TEST_DOCS['painless-field-context.mdx'])
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
        new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-field-context.asciidoc.yml").exists()
        new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-field-context.mdx.yml").exists()
    }

    def "fails in migration mode for same doc source file with different extension generates different spec"() {
        given:
        def build = ProjectBuilder.builder().build()
        def task = build.tasks.register("restTestFromSnippet", RestTestsFromDocSnippetTask).get()
        task.expectedUnconvertedCandidates = []
        task.migrationMode = true
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));
        docFile('docs/painless-field-context.asciidoc', SAMPLE_TEST_DOCS['painless-field-context.asciidoc'])
        docFile('docs/painless-field-context.mdx', SAMPLE_TEST_DOCS['painless-field-context-different.mdx'])
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
        new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-field-context.asciidoc.yml").exists()
        new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-field-context.mdx.yml").exists()
    }


    File docFile(String fileName, String docContent) {
        def file = tempDir.toPath().resolve(fileName).toFile()
        file.parentFile.mkdirs()
        file.text = docContent
        return file
    }


    void sampleDocs() {
        docFile("docs/reference/sql/getting-started.asciidoc",  SAMPLE_TEST_DOCS["docs/reference/sql/getting-started.asciidoc"])
        docFile("docs/ml-update-snapshot.asciidoc",  SAMPLE_TEST_DOCS["docs/ml-update-snapshot.asciidoc"])
        docFile("docs/painless-debugging.asciidoc",  SAMPLE_TEST_DOCS["docs/painless-debugging.asciidoc"])
        docFile("docs/reference/security/authorization/run-as-privilege.asciidoc",  SAMPLE_TEST_DOCS["docs/reference/security/authorization/run-as-privilege.asciidoc"])
    }

    String normalizeRestSpec(String inputString) {
        def withNormalizedLines = inputString.replaceAll(/"line_\d+":/, "\"line_0\":")
        return withNormalizedLines
    }
}
