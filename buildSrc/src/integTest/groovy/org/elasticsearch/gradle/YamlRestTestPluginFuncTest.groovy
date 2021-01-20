/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle

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
        """
        String api = "foo.json"
        setupRestResources([api])
        addRestTestsToProject(["10_basic.yml"], "yamlRestTest")
        file("src/yamlRestTest/java/MockIT.java") << "import org.junit.Test;class MockIT { @Test public void doNothing() { }}"

        when:
        def result = gradleRunner("yamlRestTest").build()

        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
        File resourceDir = new File(testProjectDir.root, "build/resources/yamlRestTest/rest-api-spec")
        File classDir = new File(testProjectDir.root, "build/classes/java/yamlRestTest")
        new File(resourceDir, "/api/" + api).exists()
        new File(resourceDir, "/test/10_basic.yml").exists()
        new File(classDir, "/MockIT.class").exists()

        File wrongResourceDir = new File(testProjectDir.root, "build/resources/test/rest-api-spec")
        File wrongClassDir = new File(testProjectDir.root, "build/classes/java/test")
        new File(wrongResourceDir, "/api/" + api).exists() == false
        new File(wrongResourceDir, "/test/10_basic.yml").exists() == false
        new File(wrongClassDir, "/MockIT.class").exists() == false

        when:
        result = gradleRunner("yamlRestTest").build()

        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE
    }
}
