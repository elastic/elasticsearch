/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest

import org.elasticsearch.gradle.fixtures.AbstractRestResourcesFuncTest
import org.gradle.testkit.runner.TaskOutcome

class YamlRestTestPluginFuncTest extends AbstractRestResourcesFuncTest {

    def "yamlRestTest does nothing when there are no tests"() {
        given:
        buildFile << """
        plugins {
          id 'elasticsearch.yaml-rest-test'
        }
        """

        when:
        def result = gradleRunner("yamlRestTest").build()

        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.NO_SOURCE
    }

    def "yamlRestTest executes and copies api and tests to correct source set"() {
        given:
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.yaml-rest-test'

            dependencies {
               yamlRestTestImplementation "junit:junit:4.12"
            }

            // can't actually spin up test cluster from this test
           tasks.withType(Test).configureEach{ enabled = false }

           tasks.register("printYamlRestTestClasspath").configure {
               doLast {
                   println sourceSets.yamlRestTest.runtimeClasspath.asPath
               }
           }
        """
        String api = "foo.json"
        setupRestResources([api])
        addRestTestsToProject(["10_basic.yml"], "yamlRestTest")
        file("src/yamlRestTest/java/MockIT.java") << "import org.junit.Test;class MockIT { @Test public void doNothing() { }}"

        when:
        def result = gradleRunner("yamlRestTest", "printYamlRestTestClasspath").build()

        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE

        file("/build/restResources/yamlSpecs/rest-api-spec/api/" + api).exists()
        file("/build/resources/yamlRestTest/rest-api-spec/test/10_basic.yml").exists()
        file("/build/classes/java/yamlRestTest/MockIT.class").exists()

        // check that our copied specs and tests are on the yamlRestTest classpath
        normalized(result.output).contains("./build/restResources/yamlSpecs")
        normalized(result.output).contains("./build/restResources/yamlTests")

        when:
        result = gradleRunner("yamlRestTest").build()

        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
    }
}
