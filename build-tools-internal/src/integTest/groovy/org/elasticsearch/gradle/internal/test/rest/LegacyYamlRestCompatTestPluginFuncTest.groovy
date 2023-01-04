/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SequenceWriter
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.fixtures.AbstractRestResourcesFuncTest
import org.elasticsearch.gradle.VersionProperties
import org.gradle.testkit.runner.TaskOutcome

class LegacyYamlRestCompatTestPluginFuncTest extends AbstractRestResourcesFuncTest {

    def compatibleVersion = Version.fromString(VersionProperties.getVersions().get("elasticsearch")).getMajor() - 1
    def specIntermediateDir = "restResources/v${compatibleVersion}/yamlSpecs"
    def testIntermediateDir = "restResources/v${compatibleVersion}/yamlTests"
    def transformTask  = ":yamlRestTestV${compatibleVersion}CompatTransform"
    def YAML_FACTORY = new YAMLFactory()
    def MAPPER = new ObjectMapper(YAML_FACTORY)
    def READER = MAPPER.readerFor(ObjectNode.class)
    def WRITER = MAPPER.writerFor(ObjectNode.class)

    def setup() {
        // not cc compatible due to:
        // 1. TestClustersPlugin not cc compatible due to listener registration
        // 2. RestIntegTestTask not cc compatible due to
        configurationCacheCompatible = false
    }
    def "yamlRestTestVxCompatTest does nothing when there are no tests"() {
        given:
        subProject(":distribution:bwc:maintenance") << """
        configurations { checkout }
        artifacts {
            checkout(new File(projectDir, "checkoutDir"))
        }
        """

        buildFile << """
        plugins {
          id 'elasticsearch.legacy-yaml-rest-compat-test'
        }
        """

        when:
        def result = gradleRunner("yamlRestTestV${compatibleVersion}CompatTest", '--stacktrace').build()

        then:
        result.task(":yamlRestTestV${compatibleVersion}CompatTest").outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestCompatApiTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestCompatTestTask').outcome == TaskOutcome.NO_SOURCE
        result.task(transformTask).outcome == TaskOutcome.NO_SOURCE
    }

    def "yamlRestTestVxCompatTest executes and copies api and transforms tests from :bwc:maintenance"() {
        given:
        internalBuild()

        subProject(":distribution:bwc:maintenance") << """
        configurations { checkout }
        artifacts {
            checkout(new File(projectDir, "checkoutDir"))
        }
        """

        buildFile << """
            apply plugin: 'elasticsearch.legacy-yaml-rest-compat-test'

            // avoids a dependency problem in this test, the distribution in use here is inconsequential to the test
            import org.elasticsearch.gradle.testclusters.TestDistribution;

            dependencies {
               yamlRestTestImplementation "junit:junit:4.12"
            }

            // can't actually spin up test cluster from this test
           tasks.withType(Test).configureEach{ enabled = false }
        """

        String wrongApi = "wrong_version.json"
        String wrongTest = "wrong_version.yml"
        String additionalTest = "additional_test.yml"
        setupRestResources([wrongApi], [wrongTest]) //setups up resources for current version, which should not be used for this test
        String sourceSetName = "yamlRestTestV" + compatibleVersion + "Compat"
        addRestTestsToProject([additionalTest], sourceSetName)
        //intentionally adding to yamlRestTest source set since the .classes are copied from there
        file("src/yamlRestTest/java/MockIT.java") << "import org.junit.Test;class MockIT { @Test public void doNothing() { }}"

        String api = "foo.json"
        String test = "10_basic.yml"
        //add the compatible test and api files, these are the prior version's normal yaml rest tests
        file("distribution/bwc/maintenance/checkoutDir/rest-api-spec/src/main/resources/rest-api-spec/api/" + api) << ""
        file("distribution/bwc/maintenance/checkoutDir/src/yamlRestTest/resources/rest-api-spec/test/" + test) << ""

        when:
        def result = gradleRunner("yamlRestTestV${compatibleVersion}CompatTest").build()

        then:
        result.task(":yamlRestTestV${compatibleVersion}CompatTest").outcome == TaskOutcome.SKIPPED
        result.task(':copyRestCompatApiTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyRestCompatTestTask').outcome == TaskOutcome.SUCCESS
        result.task(transformTask).outcome == TaskOutcome.SUCCESS

        file("/build/${specIntermediateDir}/rest-api-spec/api/" + api).exists()
        file("/build/${testIntermediateDir}/original/rest-api-spec/test/" + test).exists()
        file("/build/${testIntermediateDir}/transformed/rest-api-spec/test/" + test).exists()
        file("/build/${testIntermediateDir}/original/rest-api-spec/test/" + test).exists()
        file("/build/${testIntermediateDir}/transformed/rest-api-spec/test/" + test).exists()
        file("/build/${testIntermediateDir}/transformed/rest-api-spec/test/" + test).text.contains("headers") //transformation adds this
        file("/build/resources/${sourceSetName}/rest-api-spec/test/" + additionalTest).exists()

        //additionalTest is not copied from the prior version, and thus not in the intermediate directory, nor transformed
        file("/build/resources/${sourceSetName}/" + testIntermediateDir + "/rest-api-spec/test/" + additionalTest).exists() == false
        file("/build/resources/${sourceSetName}/rest-api-spec/test/" + additionalTest).text.contains("headers") == false

        file("/build/classes/java/yamlRestTest/MockIT.class").exists() //The "standard" runner is used to execute the compat test

        file("/build/resources/${sourceSetName}/rest-api-spec/api/" + wrongApi).exists() == false
        file("/build/resources/${sourceSetName}/" + testIntermediateDir + "/rest-api-spec/test/" + wrongTest).exists() == false
        file("/build/resources/${sourceSetName}/rest-api-spec/test/" + wrongTest).exists() == false

        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE

        when:
        result = gradleRunner("yamlRestTestV${compatibleVersion}CompatTest").build()

        then:
        result.task(":yamlRestTestV${compatibleVersion}CompatTest").outcome == TaskOutcome.SKIPPED
        result.task(':copyRestCompatApiTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':copyRestCompatTestTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(transformTask).outcome == TaskOutcome.UP_TO_DATE
    }

    def "yamlRestTestVxCompatTest is wired into check and checkRestCompat"() {
        given:
        withVersionCatalogue()
        subProject(":distribution:bwc:maintenance") << """
        configurations { checkout }
        artifacts {
            checkout(new File(projectDir, "checkoutDir"))
        }
        """

        buildFile << """
        plugins {
          id 'elasticsearch.legacy-yaml-rest-compat-test'
        }

        """

        when:
        def result = gradleRunner("check").build()

        then:
        result.task(':check').outcome == TaskOutcome.UP_TO_DATE
        result.task(':checkRestCompat').outcome == TaskOutcome.UP_TO_DATE
        result.task(":yamlRestTestV${compatibleVersion}CompatTest").outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestCompatApiTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestCompatTestTask').outcome == TaskOutcome.NO_SOURCE
        result.task(transformTask).outcome == TaskOutcome.NO_SOURCE

        when:
        buildFile << """
         ext.bwc_tests_enabled = false
        """
        result = gradleRunner("check").build()

        then:
        result.task(':check').outcome == TaskOutcome.UP_TO_DATE
        result.task(':checkRestCompat').outcome == TaskOutcome.UP_TO_DATE
        result.task(":yamlRestTestV${compatibleVersion}CompatTest").outcome == TaskOutcome.SKIPPED
        result.task(':copyRestCompatApiTask').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestCompatTestTask').outcome == TaskOutcome.SKIPPED
        result.task(transformTask).outcome == TaskOutcome.SKIPPED
    }

    def "transform task executes and works as configured"() {
        given:
        internalBuild()

        subProject(":distribution:bwc:maintenance") << """
        configurations { checkout }
        artifacts {
            checkout(new File(projectDir, "checkoutDir"))
        }
        """

        buildFile << """
            apply plugin: 'elasticsearch.legacy-yaml-rest-compat-test'

            // avoids a dependency problem in this test, the distribution in use here is inconsequential to the test
            import org.elasticsearch.gradle.testclusters.TestDistribution;

            dependencies {
               yamlRestTestImplementation "junit:junit:4.12"
            }
            tasks.named("yamlRestTestV${compatibleVersion}CompatTransform").configure({ task ->
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
            // can't actually spin up test cluster from this test
           tasks.withType(Test).configureEach{ enabled = false }
        """

        setupRestResources([], [])

        file("distribution/bwc/maintenance/checkoutDir/src/yamlRestTest/resources/rest-api-spec/test/test.yml" ) << """
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
        when:
        def result = gradleRunner("yamlRestTestV${compatibleVersion}CompatTest").build()

        then:

        result.task(transformTask).outcome == TaskOutcome.SUCCESS


        file("/build/${testIntermediateDir}/transformed/rest-api-spec/test/test.yml" ).exists()
        List<ObjectNode> actual = READER.readValues(file("/build/${testIntermediateDir}/transformed/rest-api-spec/test/test.yml")).readAll()
        List<ObjectNode> expectedAll = READER.readValues(
        """
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
            do_.some.key_that_was_replaced:
              index: "test"
              id: 1
              keyvalue : replacedkeyvalue
            do_.some.key_to_replace_in_two:
              no_change_here: "because it's not in test 'two'"
            warnings:
            - "warning1"
            - "warning2"
            headers:
              Content-Type: "application/vnd.elasticsearch+json;compatible-with=7"
              Accept: "application/vnd.elasticsearch+json;compatible-with=7"
            allowed_warnings:
            - "added allowed warning"
            allowed_warnings_regex:
            - "added allowed warning regex .* [0-9]"
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
        - length: { key.in_length_that_was_replaced: 1 }
        - length: { value_to_replace: 99 }
        - match:
            _source.added:
              name: "jake"
              likes: "cheese"

        ---
        two:
        - skip:
            version: "all"
            reason: "This is a test to skip test two"
        - do:
            get:
              index: "test2"
              id: 1
            do_.some.key_that_was_replaced_in_two:
                changed_here: "because it is in test 'two'"
            headers:
              Content-Type: "application/vnd.elasticsearch+json;compatible-with=7"
              Accept: "application/vnd.elasticsearch+json;compatible-with=7"
            warnings_regex:
            - "regex warning here .* [a-z]"
            allowed_warnings:
            - "added allowed warning"
            allowed_warnings_regex:
            - "added allowed warning regex .* [0-9]"
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
        - length: { value_not_to_replace: 1 }
        ---
        "use cat with no header":
          - do:
              cat.indices:
                {}
              allowed_warnings:
                - "added allowed warning"
              allowed_warnings_regex:
                - "added allowed warning regex .* [0-9]"
          - match: {}
        """.stripIndent()).readAll()

        expectedAll.eachWithIndex{ ObjectNode expected, int i ->
            if(expected != actual.get(i)) {
                println("\nTransformed Test:")
                SequenceWriter sequenceWriter = WRITER.writeValues(System.out)
                for (ObjectNode transformedTest : actual) {
                    sequenceWriter.write(transformedTest)
                }
                sequenceWriter.close()
            }
           assert expected == actual.get(i)
        }

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

}
