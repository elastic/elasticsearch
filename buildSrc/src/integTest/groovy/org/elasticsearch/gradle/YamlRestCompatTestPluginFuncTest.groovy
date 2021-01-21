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

class YamlRestCompatTestPluginFuncTest extends AbstractRestResourcesFuncTest {

    def "yamlRestCompatTest does nothing when there are no tests"() {
        given:

        addSubProject(":distribution:bwc:minor") << """
        configurations { checkout }
        artifacts {
            checkout(new File(projectDir, "checkoutDir"))
        }
        """

        buildFile << """
        plugins {
          id 'elasticsearch.yaml-rest-compat-test'
        }
        """

        when:
        def result = gradleRunner("yamlRestCompatTest").build()

        then:
        result.task(':yamlRestCompatTest').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestApiCompatSpecsTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestApiCompatTestTask').outcome == TaskOutcome.NO_SOURCE
    }

    def "yamlRestCompatTest executes and copies api and tests from :bwc:minor"() {
        given:
        internalBuild()

        addSubProject(":distribution:bwc:minor") << """
        configurations { checkout }
        artifacts {
            checkout(new File(projectDir, "checkoutDir"))
        }
        """

        buildFile << """
            apply plugin: 'elasticsearch.yaml-rest-compat-test'

            // avoids a dependency problem in this test, the distribution in use here is inconsequential to the test
            import org.elasticsearch.gradle.testclusters.TestDistribution;
            testClusters {
              yamlRestCompatTest.setTestDistribution(TestDistribution.INTEG_TEST)
            }

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
        addRestTestsToProject([additionalTest], "yamlRestCompatTest")
        //intentionally adding to yamlRestTest source set since the .classes are copied from there
        file("src/yamlRestTest/java/MockIT.java") << "import org.junit.Test;class MockIT { @Test public void doNothing() { }}"

        String api = "foo.json"
        String test = "10_basic.yml"
        //add the compatible test and api files, these are the prior version's normal yaml rest tests
        file("distribution/bwc/minor/checkoutDir/rest-api-spec/src/main/resources/rest-api-spec/api/" + api) << ""
        file("distribution/bwc/minor/checkoutDir/src/yamlRestTest/resources/rest-api-spec/test/" + test) << ""

        when:
        def result = gradleRunner("yamlRestCompatTest").build()

        then:
        result.task(':yamlRestCompatTest').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiCompatSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyRestApiCompatTestTask').outcome == TaskOutcome.SUCCESS

        file("/build/resources/yamlRestCompatTest/rest-api-spec/api/" + api).exists()
        file("/build/resources/yamlRestCompatTest/rest-api-spec/test/" + test).exists()
        file("/build/resources/yamlRestCompatTest/rest-api-spec/test/" + additionalTest).exists()

        file("/build/classes/java/yamlRestTest/MockIT.class").exists() //The "standard" runner is used to execute the compat test

        file("/build/resources/yamlRestCompatTest/rest-api-spec/api/" + wrongApi).exists() == false
        file("/build/resources/yamlRestCompatTest/rest-api-spec/test/" + wrongTest).exists() == false

        result.task(':copyRestApiSpecsTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyYamlTestsTask').outcome == TaskOutcome.NO_SOURCE

        when:
        result = gradleRunner("yamlRestCompatTest").build()

        then:
        result.task(':yamlRestCompatTest').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiCompatSpecsTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':copyRestApiCompatTestTask').outcome == TaskOutcome.UP_TO_DATE
    }
}
