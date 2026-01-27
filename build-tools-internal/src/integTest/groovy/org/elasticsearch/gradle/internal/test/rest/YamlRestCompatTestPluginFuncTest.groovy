/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SequenceWriter
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import org.elasticsearch.gradle.fixtures.AbstractRestResourcesFuncTest
import org.gradle.testkit.runner.TaskOutcome

class YamlRestCompatTestPluginFuncTest extends AbstractRestResourcesFuncTest {

    def specIntermediateDir = "restResources/compat/yamlSpecs"
    def testIntermediateDir = "restResources/compat/yamlTests"
    def transformTask = ":yamlRestCompatTestTransform"
    def YAML_FACTORY = new YAMLFactory()
    def MAPPER = new ObjectMapper(YAML_FACTORY)
    def READER = MAPPER.readerFor(ObjectNode.class)
    def WRITER = MAPPER.writerFor(ObjectNode.class)

//    def setup() {
//        // not cc compatible due to:
//        // 1. TestClustersPlugin not cc compatible due to listener registration
//        // 2. RestIntegTestTask not cc compatible due to
//        configurationCacheCompatible = true
//        buildApiRestrictionsDisabled = false
//    }

    def "yamlRestCompatTest does nothing when there are no tests"() {
        given:
        internalBuild()
        setupServerAndCore()
        setupBwcProject()

        applyPlugin()

        when:
        def result = gradleRunner("yamlRestCompatTest", '--stacktrace').build()

        then:
        verifyAll(result) {
            task(":yamlRestCompatTest").outcome == TaskOutcome.NO_SOURCE
            task(':copyRestCompatApiTask').outcome == TaskOutcome.NO_SOURCE
            task(':copyRestCompatTestTask').outcome == TaskOutcome.NO_SOURCE
            task(transformTask).outcome == TaskOutcome.NO_SOURCE
        }
    }

    def "yamlRestCompatTest executes and copies api and transforms tests from unreleased previous major"() {
        given:
        internalBuild()
        setupServerAndCore()
        setupBwcProject()

        applyPlugin()
        configureTestExecution()

        String wrongApi = "wrong_version.json"
        String wrongTest = "wrong_version.yml"
        String additionalTest = "additional_test.yml"
        setupRestResources([wrongApi], [wrongTest]) //setups up resources for current version, which should not be used for this test
        String sourceSetName = "yamlRestCompatTest"
        addRestTestsToProject([additionalTest], sourceSetName)
        //intentionally adding to yamlRestTest source set since the .classes are copied from there
        file("src/yamlRestTest/java/MockIT.java") << "import org.junit.Test;class MockIT { @Test public void doNothing() { }}"

        String api = "foo.json"
        String test = "10_basic.yml"
        //add the compatible test and api files, these are the prior version's normal yaml rest tests
        file("distribution/bwc/major1/checkoutDir/rest-api-spec/src/main/resources/rest-api-spec/api/" + api) << ""
        file("distribution/bwc/major1/checkoutDir/src/yamlRestTest/resources/rest-api-spec/test/" + test) << ""

        when:
        def result = gradleRunner("yamlRestCompatTest").build()

        then:
        verifyAll(result) {
            task(":yamlRestCompatTest").outcome == TaskOutcome.SKIPPED
            task(':copyRestCompatApiTask').outcome == TaskOutcome.SUCCESS
            task(':copyRestCompatTestTask').outcome == TaskOutcome.SUCCESS
            task(transformTask).outcome == TaskOutcome.SUCCESS
        }

        fileExists("/build/${specIntermediateDir}/rest-api-spec/api/" + api)
        fileExists("/build/${testIntermediateDir}/original/rest-api-spec/test/" + test)
        fileExists("/build/${testIntermediateDir}/transformed/rest-api-spec/test/" + test)
        file("/build/${testIntermediateDir}/transformed/rest-api-spec/test/" + test).text.contains("headers") //transformation adds this
        fileExists("/build/resources/${sourceSetName}/rest-api-spec/test/" + additionalTest)

        //additionalTest is not copied from the prior version, and thus not in the intermediate directory, nor transformed
        !fileExists("/build/resources/${sourceSetName}/" + testIntermediateDir + "/rest-api-spec/test/" + additionalTest)
        !file("/build/resources/${sourceSetName}/rest-api-spec/test/" + additionalTest).text.contains("headers")

        fileExists("/build/classes/java/yamlRestTest/MockIT.class") //The "standard" runner is used to execute the compat test

        !fileExists("/build/resources/${sourceSetName}/rest-api-spec/api/" + wrongApi)
        !fileExists("/build/resources/${sourceSetName}/" + testIntermediateDir + "/rest-api-spec/test/" + wrongTest)
        !fileExists("/build/resources/${sourceSetName}/rest-api-spec/test/" + wrongTest)

        when:
        result = gradleRunner("yamlRestCompatTest").build()

        then:
        verifyAll(result) {
            task(":yamlRestCompatTest").outcome == TaskOutcome.SKIPPED
            task(':copyRestCompatApiTask').outcome == TaskOutcome.UP_TO_DATE
            task(':copyRestCompatTestTask').outcome == TaskOutcome.UP_TO_DATE
            task(transformTask).outcome == TaskOutcome.UP_TO_DATE
        }
    }

    def "yamlRestCompatTest executes and copies api and transforms tests from released previous major"() {
        given:
        setupServerAndCore()
        createVersionFile()
        createBranchesJson()

        buildFile << """
            plugins {
              id 'elasticsearch.global-build-info'
            }

            // Configure branches file location
            project.ext.set("org.elasticsearch.build.branches-file-location", file("branches.json").toURI().toString())
        """

        applyPlugin()

        buildFile << """
            restResources {
              restApi {
                include '*'
              }
              restTests {
                includeCore '*'
              }
            }

            repositories {
                maven { url "file://${testProjectDir.root.absolutePath}/repo" }
                mavenCentral()
            }
        """

        configureTestExecution()

        String api = "foo.json"
        String test = "10_basic.yml"

        createRepoArtifacts(api, test)

        when:
        def result = gradleRunner("yamlRestCompatTest").build()

        then:
        verifyAll(result) {
            task(":yamlRestCompatTest").outcome == TaskOutcome.SKIPPED
            task(':copyRestCompatApiTask').outcome == TaskOutcome.SUCCESS
            task(':copyRestCompatTestTask').outcome == TaskOutcome.SUCCESS
            task(transformTask).outcome == TaskOutcome.SUCCESS
        }

        fileExists("/build/${specIntermediateDir}/rest-api-spec/api/" + api)
        fileExists("/build/${testIntermediateDir}/original/rest-api-spec/test/" + test)
        fileExists("/build/${testIntermediateDir}/transformed/rest-api-spec/test/" + test)
    }

    def "yamlRestCompatTest is wired into check and checkRestCompat"() {
        given:
        internalBuild()
        withVersionCatalogue()
        setupServerAndCore()
        setupBwcProject()

        applyPlugin()

        when:
        def result = gradleRunner("check").build()

        then:
        verifyAll(result) {
            task(':check').outcome == TaskOutcome.UP_TO_DATE
            task(':checkRestCompat').outcome == TaskOutcome.UP_TO_DATE
            task(":yamlRestCompatTest").outcome == TaskOutcome.NO_SOURCE
            task(':copyRestCompatApiTask').outcome == TaskOutcome.NO_SOURCE
            task(':copyRestCompatTestTask').outcome == TaskOutcome.NO_SOURCE
            task(transformTask).outcome == TaskOutcome.NO_SOURCE
        }

        when:
        buildFile << """
         ext.bwc_tests_enabled = false
        """
        result = gradleRunner("check").build()

        then:
        verifyAll(result) {
            task(':check').outcome == TaskOutcome.UP_TO_DATE
            task(':checkRestCompat').outcome == TaskOutcome.UP_TO_DATE
            task(":yamlRestCompatTest").outcome == TaskOutcome.SKIPPED
            task(':copyRestCompatApiTask').outcome == TaskOutcome.SKIPPED
            task(':copyRestCompatTestTask').outcome == TaskOutcome.SKIPPED
            task(transformTask).outcome == TaskOutcome.SKIPPED
        }
    }

    def "transform task executes and works as configured"() {
        given:
        internalBuild()
        setupServerAndCore()
        setupBwcProject()

        applyPlugin()
        configureTestExecution()

        buildFile << """
            tasks.named("yamlRestCompatTestTransform").configure({ task ->
              task.skipTest("test/test/two", "This is a test to skip test two")
              task.replaceValueInMatch("_type", "_doc")
              task.replaceValueInMatch("_source.values", ["z", "x", "y"], "one")
              task.removeMatch("_source.blah")
              task.removeMatch("_source.junk", "two")
              task.addMatch("_source.added", [name: 'jake', likes: 'cheese'], "one")
              task.addWarning("one", "warning1", "warning2")
              task.addWarningRegex("two", "regex warning here .* [a-z]")
              task.addAllowedWarning("added allowed warning")
              task.addAllowedWarningRegex("added allowed warning regex .* [0-9]")
              task.removeWarning("one", "warning to remove")
              task.replaceIsTrue("value_to_replace", "replaced_value")
              task.replaceIsFalse("value_to_replace", "replaced_value")
              task.replaceKeyInDo("do_.some.key_to_replace", "do_.some.key_that_was_replaced")
              task.replaceKeyInDo("do_.some.key_to_replace_in_two", "do_.some.key_that_was_replaced_in_two", "two")
              task.replaceKeyInMatch("match_.some.key_to_replace", "match_.some.key_that_was_replaced")
              task.replaceKeyInLength("key.in_length_to_replace", "key.in_length_that_was_replaced")
              task.replaceValueInLength("value_to_replace", 99, "one")
              task.replaceValueTextByKeyValue("keyvalue", "toreplace", "replacedkeyvalue")
              task.replaceValueTextByKeyValue("index", "test", "test2", "two")
            })
        """

        setupRestResources([], [])

        createTestSourceFile()

        when:
        def result = gradleRunner("yamlRestCompatTest").build()

        then:
        result.task(transformTask).outcome == TaskOutcome.SUCCESS
        verifyTransformedFile()

        when:
        result = gradleRunner(transformTask).build()

        then:
        result.task(transformTask).outcome == TaskOutcome.UP_TO_DATE

        when:
        buildFile.write(buildFile.text.replace("blah", "baz"))
        result = gradleRunner(transformTask).build()

        then:
        result.task(transformTask).outcome == TaskOutcome.SUCCESS
    }

    def "plugin applies InternalYamlRestTestPlugin as base"() {
        given:
        internalBuild()
        setupServerAndCore()
        setupBwcProject()

        applyPlugin()

        buildFile << """
            import org.elasticsearch.gradle.internal.test.rest.InternalYamlRestTestPlugin

            def hasYamlRestTestPlugin = project.plugins.hasPlugin(InternalYamlRestTestPlugin)
            println "Has InternalYamlRestTestPlugin: " + hasYamlRestTestPlugin
            if (!hasYamlRestTestPlugin) {
                throw new GradleException("Expected InternalYamlRestTestPlugin to be applied")
            }
        """

        when:
        def result = gradleRunner("tasks").build()

        then:
        result.output.contains("Has InternalYamlRestTestPlugin: true")
    }

    def "plugin creates yamlRestCompatTest source set"() {
        given:
        internalBuild()
        setupServerAndCore()
        setupBwcProject()

        applyPlugin()

        buildFile << """
            def sourceSets = project.extensions.getByType(SourceSetContainer)
            def compatSourceSet = sourceSets.findByName('yamlRestCompatTest')
            println "Has yamlRestCompatTest source set: " + (compatSourceSet != null)
            if (compatSourceSet == null) {
                throw new GradleException("Expected yamlRestCompatTest source set to exist")
            }
        """

        when:
        def result = gradleRunner("tasks").build()

        then:
        result.output.contains("Has yamlRestCompatTest source set: true")
    }

    // Helper methods

    void setupServerAndCore() {
        subProject(":libs:elasticsearch-core") << "apply plugin: 'elasticsearch.java'"
        subProject(":server") << """
        apply plugin: 'elasticsearch.java'

        import org.gradle.api.artifacts.type.ArtifactTypeDefinition

        dependencies {
            api project(':libs:elasticsearch-core')
        }

        // Create features metadata configuration and artifact
        def featuresFile = file('build/cluster-features.json')
        featuresFile.parentFile.mkdirs()
        featuresFile.text = '[]'

        configurations {
            featuresMetadata {
                canBeResolved = false
                canBeConsumed = true
                attributes {
                    attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, 'features-metadata-json')
                }
            }
        }
        artifacts {
            featuresMetadata(featuresFile)
        }
        """
    }

    void setupBwcProject() {
        subProject(":distribution:bwc:major1") << """
        configurations { checkout }
        artifacts {
            checkout(new File(projectDir, "checkoutDir"))
        }
        """
    }

    void applyPlugin() {
        buildFile << """
            apply plugin: 'elasticsearch.yaml-rest-compat-test'
        """
    }

    void configureTestExecution() {
        buildFile << """
            // avoids a dependency problem in this test, the distribution in use here is inconsequential to the test
            import org.elasticsearch.gradle.testclusters.TestDistribution;

            dependencies {
               yamlRestTestImplementation "junit:junit:4.12"
            }

            // can't actually spin up test cluster from this test
           tasks.withType(Test).configureEach{ enabled = false }
        """
    }

    void createVersionFile() {
        File versionFile = file("server/src/main/java/org/elasticsearch/Version.java")
        versionFile.parentFile.mkdirs()
        versionFile.text = """
            package org.elasticsearch;
            import org.elasticsearch.gradle.Version;
            public class Version {
                public static final Version V_8_100_0 = new Version(8, 100, 0);
                public static final Version V_9_1_0 = new Version(9, 1, 0);
                public static final Version CURRENT = V_9_1_0;
            }
        """
    }

    void createBranchesJson() {
        File branchesFile = file("branches.json")
        branchesFile.text = """
            [
              {
                "name": "main",
                "version": "9.1.0",
                "creation_date": "2021-01-01"
              }
            ]
        """
    }

    void createRepoArtifacts(String api, String test) {
        File repoDir = file("repo/org/elasticsearch/rest-api-spec/8.100.0")
        repoDir.mkdirs()
        File jarFile = new File(repoDir, "rest-api-spec-8.100.0.jar")
        File pomFile = new File(repoDir, "rest-api-spec-8.100.0.pom")

        java.util.zip.ZipOutputStream zos = new java.util.zip.ZipOutputStream(new FileOutputStream(jarFile))
        zos.putNextEntry(new java.util.zip.ZipEntry("rest-api-spec/api/" + api))
        zos.write("{ \"foo\": {} }".getBytes())
        zos.closeEntry()
        zos.putNextEntry(new java.util.zip.ZipEntry("rest-api-spec/test/" + test))
        zos.write("\"test\":\n  - do:\n      foo: {}".getBytes())
        zos.closeEntry()
        zos.close()

        pomFile.text = """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>org.elasticsearch</groupId>
              <artifactId>rest-api-spec</artifactId>
              <version>8.100.0</version>
            </project>
        """
    }

    void createTestSourceFile() {
        file("distribution/bwc/major1/checkoutDir/src/yamlRestTest/resources/rest-api-spec/test/test.yml") << """
        "one":
          - do:
              do_.some.key_to_replace:
                index: test
                id: 1
                keyvalue : toreplace
              do_.some.key_to_replace_in_two:
                no_change_here: "because it's not in test 'two'"
              warnings:
                - "warning to remove"
          - match: { _source.values: ["foo"] }
          - match: { _type: "_foo" }
          - match: { _source.blah: 1234 }
          - match: { _source.junk: true }
          - match: { match_.some.key_to_replace: true }
          - is_true: "value_to_replace"
          - is_false: "value_to_replace"
          - is_true: "value_not_to_replace"
          - is_false: "value_not_to_replace"
          - length: { key.in_length_to_replace: 1 }
          - length: { value_to_replace: 1 }
        ---
        "two":
          - do:
              get:
                index: test
                id: 1
              do_.some.key_to_replace_in_two:
                changed_here: "because it is in test 'two'"
          - match: { _source.values: ["foo"] }
          - match: { _type: "_foo" }
          - match: { _source.blah: 1234 }
          - match: { _source.junk: true }
          - is_true: "value_to_replace"
          - is_false: "value_to_replace"
          - is_true: "value_not_to_replace"
          - is_false: "value_not_to_replace"
          - length: { value_not_to_replace: 1 }
        ---
        "use cat with no header":
          - do:
              cat.indices:
                {}
          - match: {}
        """.stripIndent()
    }

    void verifyTransformedFile() {
        assert fileExists("/build/${testIntermediateDir}/transformed/rest-api-spec/test/test.yml")
        List<ObjectNode> actual = READER.readValues(file("/build/${testIntermediateDir}/transformed/rest-api-spec/test/test.yml")).readAll()
        List<ObjectNode> expectedAll = READER.readValues(getExpectedTransformedYaml()).readAll()

        if (actual != expectedAll) {
            println("\nTransformed Test:")
            SequenceWriter sequenceWriter = WRITER.writeValues(System.out)
            for (ObjectNode transformedTest : actual) {
                sequenceWriter.write(transformedTest)
            }
            sequenceWriter.close()
        }
        assert actual == expectedAll
    }

    String getExpectedTransformedYaml() {
        return """
---
setup:
- skip:
    features:
    - "headers"
    - "warnings"
    - "warnings_regex"
    - "allowed_warnings"
    - "allowed_warnings_regex"
---
one:
- do:
    do_.some.key_to_replace_in_two:
      no_change_here: "because it's not in test 'two'"
    warnings:
    - "warning1"
    - "warning2"
    headers:
      Content-Type: "application/vnd.elasticsearch+json;compatible-with=8"
      Accept: "application/vnd.elasticsearch+json;compatible-with=8"
    allowed_warnings:
    - "added allowed warning"
    allowed_warnings_regex:
    - "added allowed warning regex .* [0-9]"
    do_.some.key_that_was_replaced:
      index: "test"
      id: 1
      keyvalue : "replacedkeyvalue"
- match:
    _source.values:
    - "z"
    - "x"
    - "y"
- match:
    _type: "_doc"
- match: {}
- match:
    _source.junk: true
- match:
    match_.some.key_that_was_replaced: true
- is_true: "replaced_value"
- is_false: "replaced_value"
- is_true: "value_not_to_replace"
- is_false: "value_not_to_replace"
- length:
    key.in_length_that_was_replaced: 1
- length:
    value_to_replace: 99
- match:
    _source.added:
      name: "jake"
      likes: "cheese"
---
two:
- skip:
    awaits_fix: "This is a test to skip test two"
- do:
    get:
      index: "test2"
      id: 1
    headers:
      Content-Type: "application/vnd.elasticsearch+json;compatible-with=8"
      Accept: "application/vnd.elasticsearch+json;compatible-with=8"
    warnings_regex:
    - "regex warning here .* [a-z]"
    allowed_warnings:
    - "added allowed warning"
    allowed_warnings_regex:
    - "added allowed warning regex .* [0-9]"
    do_.some.key_that_was_replaced_in_two:
      changed_here: "because it is in test 'two'"
- match:
    _source.values:
    - "foo"
- match:
    _type: "_doc"
- match: {}
- match: {}
- is_true: "replaced_value"
- is_false: "replaced_value"
- is_true: "value_not_to_replace"
- is_false: "value_not_to_replace"
- length:
    value_not_to_replace: 1
---
"use cat with no header":
  - do:
      cat.indices: {}
      allowed_warnings:
        - "added allowed warning"
      allowed_warnings_regex:
        - "added allowed warning regex .* [0-9]"
  - match: {}
""".stripIndent()
    }


    boolean fileExists(String path) {
        file(path).exists()
    }

    boolean fileDoesNotExist(String path) {
        !file(path).exists()
    }
}
