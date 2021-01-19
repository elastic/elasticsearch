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

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.IgnoreIf

class YamlRestCompatTestPluginFuncTest  extends AbstractGradleFuncTest {

    def "yamlRestCompatTest is wired into check and checkRestCompat"() {
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
        def result = gradleRunner("check").build()

        then:
        result.task(':check').outcome == TaskOutcome.UP_TO_DATE
        result.task(':checkRestCompat').outcome == TaskOutcome.UP_TO_DATE
        result.task(':yamlRestCompatTest').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestApiCompatSpecsTask').outcome == TaskOutcome.NO_SOURCE
        result.task(':copyRestApiCompatTestTask').outcome == TaskOutcome.NO_SOURCE

        when:
        buildFile << """
         ext.bwc_tests_enabled = false
        """
        result = gradleRunner("check").build()

        then:
        result.task(':check').outcome == TaskOutcome.UP_TO_DATE
        result.task(':checkRestCompat').outcome == TaskOutcome.SKIPPED
        result.task(':yamlRestCompatTest').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiCompatSpecsTask').outcome == TaskOutcome.SKIPPED
        result.task(':copyRestApiCompatTestTask').outcome == TaskOutcome.SKIPPED
    }
}
