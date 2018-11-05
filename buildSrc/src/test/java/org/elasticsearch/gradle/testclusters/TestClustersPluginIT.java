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
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;

import java.util.Arrays;

public class TestClustersPluginIT extends GradleIntegrationTestCase {

    public void testListClusters() {
        BuildResult result = getTestClustersRunner("listTestClusters").build();

        assertTaskSuccessful(result, ":listTestClusters");
        assertOutputContains(
            result.getOutput(),
            "   * myTestCluster:"
        );
    }

    public void testUseClusterByOne() {
        BuildResult result = getTestClustersRunner("user1").build();
        assertTaskSuccessful(result, ":user1");
        assertStartedAndStoppedOnce(result);
    }

    public void testUseClusterByOneWithDryRun() {
        BuildResult result = getTestClustersRunner("--dry-run", "user1").build();
        assertNull(result.task(":user1"));
        assertNotStarted(result);
    }

    public void testUseClusterByTwo() {
        BuildResult result = getTestClustersRunner("user1", "user2").build();
        assertTaskSuccessful(result, ":user1", ":user2");
        assertStartedAndStoppedOnce(result);
    }

    public void testUseClusterByUpToDateTask() {
        BuildResult result = getTestClustersRunner("upToDate1", "upToDate2").build();
        assertTaskUpToDate(result, ":upToDate1", ":upToDate2");
        assertNotStarted(result);
    }

    public void testUseClusterBySkippedTask() {
        BuildResult result = getTestClustersRunner("skipped1", "skipped2").build();
        assertTaskSkipped(result, ":skipped1", ":skipped2");
        assertNotStarted(result);
    }

    public void testUseClusterBySkippedAndWorkingTask() {
        BuildResult result = getTestClustersRunner("skipped1", "user1").build();
        assertTaskSkipped(result, ":skipped1");
        assertTaskSuccessful(result, ":user1");
        assertOutputContains(
            result.getOutput(),
            "> Task :user1",
            "Starting `ElasticsearchNode{name='myTestCluster'}`",
            "Stopping `ElasticsearchNode{name='myTestCluster'}`"
        );
    }

    public void testMultiProject() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters_multiproject"))
            .withArguments("user1", "user2", "-s", "-i", "--parallel")
            .withPluginClasspath()
            .build();
        assertTaskSuccessful(result, ":user1", ":user2");

        assertStartedAndStoppedOnce(result);
    }

    public void testUseClusterByFailingOne() {
        BuildResult result = getTestClustersRunner("itAlwaysFails").buildAndFail();
        assertTaskFailed(result, ":itAlwaysFails");
        assertStartedAndStoppedOnce(result);
        assertOutputContains(
            result.getOutput(),
            "Stopping `ElasticsearchNode{name='myTestCluster'}`, tailLogs: true",
            "Execution failed for task ':itAlwaysFails'."
        );
    }

    public void testUseClusterByFailingDependency() {
        BuildResult result = getTestClustersRunner("dependsOnFailed").buildAndFail();
        assertTaskFailed(result, ":itAlwaysFails");
        assertNull(result.task(":dependsOnFailed"));
        assertStartedAndStoppedOnce(result);
        assertOutputContains(
            result.getOutput(),
            "Stopping `ElasticsearchNode{name='myTestCluster'}`, tailLogs: true",
            "Execution failed for task ':itAlwaysFails'."
        );
    }

    public void testConfigurationLocked() {
        BuildResult result = getTestClustersRunner("illegalConfigAlter").buildAndFail();
        assertTaskFailed(result, ":illegalConfigAlter");
        assertOutputContains(
            result.getOutput(),
            "Configuration can not be altered, already locked"
        );
    }

    private void assertNotStarted(BuildResult result) {
        assertOutputDoesNotContain(
            result.getOutput(),
            "Starting ",
            "Stopping "
        );
    }

    private GradleRunner getTestClustersRunner(String... tasks) {
        String[] arguments = Arrays.copyOf(tasks, tasks.length + 2);
        arguments[tasks.length] = "-s";
        arguments[tasks.length + 1] = "-i";
        return GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments(arguments)
            .withPluginClasspath();
    }


    private void assertStartedAndStoppedOnce(BuildResult result) {
        assertOutputOnlyOnce(
            result.getOutput(),
            "Starting `ElasticsearchNode{name='myTestCluster'}`",
            "Stopping `ElasticsearchNode{name='myTestCluster'}`"
        );
    }
}
