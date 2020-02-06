/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.Before;

public class TestingConventionsTasksIT extends GradleIntegrationTestCase {

    @Before
    public void setUp() {}

    public void testInnerClasses() {
        GradleRunner runner = getGradleRunner("testingConventions").withArguments(
            "clean",
            ":no_tests_in_inner_classes:testingConventions",
            "-i",
            "-s"
        );
        BuildResult result = runner.buildAndFail();
        assertOutputContains(
            result.getOutput(),
            "Test classes implemented by inner classes will not run:",
            "  * org.elasticsearch.gradle.testkit.NastyInnerClasses$LooksLikeATestWithoutNamingConvention1",
            "  * org.elasticsearch.gradle.testkit.NastyInnerClasses$LooksLikeATestWithoutNamingConvention2",
            "  * org.elasticsearch.gradle.testkit.NastyInnerClasses$LooksLikeATestWithoutNamingConvention3",
            "  * org.elasticsearch.gradle.testkit.NastyInnerClasses$NamingConventionIT",
            "  * org.elasticsearch.gradle.testkit.NastyInnerClasses$NamingConventionTests"
        );
    }

    public void testNamingConvention() {
        GradleRunner runner = getGradleRunner("testingConventions").withArguments(
            "clean",
            ":incorrect_naming_conventions:testingConventions",
            "-i",
            "-s"
        );
        BuildResult result = runner.buildAndFail();
        assertOutputContains(
            result.getOutput(),
            "Seem like test classes but don't match naming convention:",
            "  * org.elasticsearch.gradle.testkit.LooksLikeATestWithoutNamingConvention1",
            "  * org.elasticsearch.gradle.testkit.LooksLikeATestWithoutNamingConvention2",
            "  * org.elasticsearch.gradle.testkit.LooksLikeATestWithoutNamingConvention3"
        );
        assertOutputDoesNotContain(result.getOutput(), "LooksLikeTestsButAbstract");
    }

    public void testNoEmptyTasks() {
        GradleRunner runner = getGradleRunner("testingConventions").withArguments(
            "clean",
            ":empty_test_task:testingConventions",
            "-i",
            "-s"
        );
        BuildResult result = runner.buildAndFail();
        assertOutputContains(
            result.getOutput(),
            "Expected at least one test class included in task :empty_test_task:emptyTest, but found none.",
            "Expected at least one test class included in task :empty_test_task:test, but found none."
        );
    }

    public void testAllTestTasksIncluded() {
        GradleRunner runner = getGradleRunner("testingConventions").withArguments(
            "clean",
            ":all_classes_in_tasks:testingConventions",
            "-i",
            "-s"
        );
        BuildResult result = runner.buildAndFail();
        assertOutputContains(
            result.getOutput(),
            "Test classes are not included in any enabled task (:all_classes_in_tasks:test):",
            "  * org.elasticsearch.gradle.testkit.NamingConventionIT"
        );
    }

    public void testTaskNotImplementBaseClass() {
        GradleRunner runner = getGradleRunner("testingConventions").withArguments(
            "clean",
            ":not_implementing_base:testingConventions",
            "-i",
            "-s"
        );
        BuildResult result = runner.buildAndFail();
        assertOutputContains(
            result.getOutput(),
            "Tests classes with suffix `IT` should extend org.elasticsearch.gradle.testkit.Integration but the following classes do not:",
            "  * org.elasticsearch.gradle.testkit.NamingConventionIT",
            "  * org.elasticsearch.gradle.testkit.NamingConventionMissmatchIT",
            "Tests classes with suffix `Tests` should extend org.elasticsearch.gradle.testkit.Unit but the following classes do not:",
            "  * org.elasticsearch.gradle.testkit.NamingConventionMissmatchTests",
            "  * org.elasticsearch.gradle.testkit.NamingConventionTests"
        );
    }

    public void testValidSetupWithoutBaseClass() {
        GradleRunner runner = getGradleRunner("testingConventions").withArguments(
            "clean",
            ":valid_setup_no_base:testingConventions",
            "-i",
            "-s"
        );
        BuildResult result = runner.build();
        assertTaskSuccessful(result, ":valid_setup_no_base:testingConventions");
    }

    public void testValidSetupWithBaseClass() {
        GradleRunner runner = getGradleRunner("testingConventions").withArguments(
            "clean",
            ":valid_setup_with_base:testingConventions",
            "-i",
            "-s"
        );
        BuildResult result = runner.build();
        assertTaskSuccessful(result, ":valid_setup_with_base:testingConventions");
    }

    public void testTestsInMain() {
        GradleRunner runner = getGradleRunner("testingConventions").withArguments("clean", ":tests_in_main:testingConventions", "-i", "-s");
        BuildResult result = runner.buildAndFail();
        assertOutputContains(
            result.getOutput(),
            "Classes matching the test naming convention should be in test not main:",
            "  * NamingConventionIT",
            "  * NamingConventionTests"
        );
    }

}
