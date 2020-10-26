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
import spock.lang.IgnoreIf

class YamlRestCompatTestPluginFuncTest extends AbstractGradleFuncTest {

    def "yamlRestCompatTest does nothing when there are no tests"() {
        given:

        addSubProject(":distribution:bwc:minor") <<  """
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
        def result = gradleRunner("yamlRestCompatTest", '-i').build()

        then:
        result.task(':yamlRestCompatTest').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestApiCompatSpecsTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestApiCompatTestTask').outcome == TaskOutcome.NO_SOURCE
    }

    def "yamlRestCompatTest executes and copies api and tests from :bwc:minor"() {
        given:
        String api = "wrongversion.json"
        String test = "wrongversion.yml"
        setupInternalRestResources(api, test)

        //don't use default distro from here since it requires resolving OS specific project and don't want to create the mock projects
        System.setProperty("tests.rest.compat.use.default.distro", "false");

        addSubProject(":distribution:bwc:minor") <<  """
        configurations { checkout }
        artifacts {
            checkout(new File(projectDir, "checkoutDir"))
        }
        """

        //add the compatible test and api files
        file("distribution/bwc/minor/checkoutDir/src/yamlRestTest/resources/rest-api-spec/test/foo/10_basic.yml") << ""
        file("distribution/bwc/minor/checkoutDir/rest-api-spec/src/main/resources/rest-api-spec/api/foo.json") << ""

        buildFile << " apply plugin: 'elasticsearch.yaml-rest-compat-test'"

        when:
        def result = gradleRunner("yamlRestCompatTest", '-i').build()

        then:
        result.task(':yamlRestCompatTest').outcome == TaskOutcome.NO_SOURCE //no Java classes in source set
        result.task(':copyRestApiCompatSpecsTask').outcome == TaskOutcome.SUCCESS
        result.task(':copyRestApiCompatTestTask').outcome == TaskOutcome.SUCCESS

        File resourceDir = new File(testProjectDir.root, "build/resources/yamlRestCompatTest/rest-api-spec")
        // ensure the compatible resources from bwc:minor are in the resource dir
        assert new File(resourceDir, "test/foo/10_basic.yml").exists()
        assert new File(resourceDir, "api/foo.json").exists()
        // we don't copy the "normal" rest resources to the compat resource dir
        assert new File(resourceDir, "/test/" + test).exists() == false
        assert new File(resourceDir, "/api/" + api).exists() == false

        when:
        //add the mock java test and dependency to run, the runner from the yamlRestTest sourceSet is used
        file("src/yamlRestTest/java/MockIT.java") << "import org.junit.Test;class MockIT { @Test public void doNothing() { }}"

        buildFile << """
           dependencies {
           yamlRestTestImplementation "junit:junit:4.12"
        }
        """

        result = gradleRunner("yamlRestCompatTest", '-i').buildAndFail() //expect failure - we don't actually spin up a cluster

        then:
        result.task(':copyRestApiCompatSpecsTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':copyRestApiCompatTestTask').outcome == TaskOutcome.UP_TO_DATE
        result.task(':yamlRestCompatTest').outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, "Starting `node{::yamlRestCompatTest-0}`")
        assertOutputContains(result.output, "Expected configuration ':es_distro_extracted_testclusters--yamlRestCompatTest-")
        assertOutputContains(result.output, "to contain exactly one file, however, it contains no files.")
    }
}
