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

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class YamlRestTestPluginFuncTest extends AbstractGradleFuncTest {

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

    def "yamlRestTest executes and copies api and tests from :rest-api-spec"() {
        given:
        String api = "foo.json"
        String test = "foo/10_basic.yml"
        setupInternalRestResources(api, test)

        buildFile << """
        apply plugin: 'elasticsearch.yaml-rest-test'

        restResources {
          restTests {
            includeCore '*'
          }
        }
        """
        when:
        def result = gradleRunner("yamlRestTest").build()

        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.NO_SOURCE //no Java classes in source set
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.SUCCESS

        File resourceDir = new File(testProjectDir.root, "build/resources/yamlRestTest/rest-api-spec")
        assert new File(resourceDir, "/test/" + test).exists()
        assert new File(resourceDir, "/api/" + api).exists()

        when:
        //add the mock java test and dependency to run
        file("src/yamlRestTest/java/MockIT.java") << "import org.junit.Test;class MockIT { @Test public void doNothing() { }}"

        buildFile << """
           dependencies {
           yamlRestTestImplementation "junit:junit:4.12"
        }
        """

        result = gradleRunner("yamlRestTest", '-i').buildAndFail() //expect to fail since we don't actually spin up a cluster

        then:
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':yamlRestTest').outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, "Starting `node{::yamlRestTest-0}`")
        assertOutputContains(result.output, "Expected configuration ':es_distro_extracted_testclusters--yamlRestTest-")
        assertOutputContains(result.output, "to contain exactly one file, however, it contains no files.")
    }

}
