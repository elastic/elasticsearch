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
import org.junit.Before;
import org.junit.Ignore;


import java.util.Arrays;

@Ignore("https://github.com/elastic/elasticsearch/issues/42453")
public class TestClustersPluginIT extends GradleIntegrationTestCase {

    private GradleRunner runner;

    @Before
    public void setUp() throws Exception {
        runner = getGradleRunner("testclusters");
    }

    public void testListClusters() {
        BuildResult result = getTestClustersRunner("listTestClusters").build();

        assertTaskSuccessful(result, ":listTestClusters");
        assertOutputContains(
            result.getOutput(),
            "   * myTestCluster:"
        );
    }

    public void testUseClusterByOne() {
        BuildResult result = getTestClustersRunner(":user1").build();
        assertTaskSuccessful(result, ":user1");
        assertStartedAndStoppedOnce(result);
    }

    public void testUseClusterByOneWithDryRun() {
        BuildResult result = getTestClustersRunner("--dry-run", ":user1").build();
        assertNull(result.task(":user1"));
        assertNotStarted(result);
    }

    public void testUseClusterByTwo() {
        BuildResult result = getTestClustersRunner(":user1", ":user2").build();
        assertTaskSuccessful(result, ":user1", ":user2");
        assertStartedAndStoppedOnce(result);
    }

    public void testUseClusterByUpToDateTask() {
        // Run it once, ignoring the result and again to make sure it's considered up to date.
        // Gradle randomly considers tasks without inputs and outputs as as up-to-date or success on the first run
        getTestClustersRunner(":upToDate1").build();
        BuildResult result = getTestClustersRunner(":upToDate1").build();
        assertTaskUpToDate(result, ":upToDate1");
        assertNotStarted(result);
    }

    public void testUseClusterBySkippedTask() {
        BuildResult result = getTestClustersRunner(":skipped1", ":skipped2").build();
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
            "Starting `node{::myTestCluster-0}`",
            "Stopping `node{::myTestCluster-0}`"
        );
    }

    @Ignore // https://github.com/elastic/elasticsearch/issues/41256
    public void testMultiProject() {
        BuildResult result = getTestClustersRunner(
            "user1", "user2", "-s", "-i", "--parallel", "-Dlocal.repo.path=" + getLocalTestRepoPath()
        ).build();

        assertTaskSuccessful(
            result,
            ":user1", ":user2", ":alpha:user1", ":alpha:user2", ":bravo:user1", ":bravo:user2"
        );
        assertStartedAndStoppedOnce(result);
        assertOutputOnlyOnce(
            result.getOutput(),
            "Starting `node{:alpha:myTestCluster-0}`",
            "Stopping `node{::myTestCluster-0}`"
        );
        assertOutputOnlyOnce(
            result.getOutput(),
            "Starting `node{::myTestCluster-0}`",
            "Stopping `node{:bravo:myTestCluster-0}`"
        );
    }

    public void testReleased() {
        BuildResult result = getTestClustersRunner("testReleased").build();
        assertTaskSuccessful(result, ":testReleased");
        assertStartedAndStoppedOnce(result, "releasedVersionDefault-0");
        assertStartedAndStoppedOnce(result, "releasedVersionOSS-0");
        assertStartedAndStoppedOnce(result, "releasedVersionIntegTest-0");
    }

    public void testIncremental() {
        BuildResult result = getTestClustersRunner("clean", ":user1").build();
        assertTaskSuccessful(result, ":user1");
        assertStartedAndStoppedOnce(result);

        result = getTestClustersRunner(":user1").build();
        assertTaskSuccessful(result, ":user1");
        assertStartedAndStoppedOnce(result);

        result = getTestClustersRunner("clean", ":user1").build();
        assertTaskSuccessful(result, ":user1");
        assertStartedAndStoppedOnce(result);
        assertStartedAndStoppedOnce(result);
    }

    public void testUseClusterByFailingOne() {
        BuildResult result = getTestClustersRunner(":itAlwaysFails").buildAndFail();
        assertTaskFailed(result, ":itAlwaysFails");
        assertStartedAndStoppedOnce(result);
        assertOutputContains(
            result.getOutput(),
            "Stopping `node{::myTestCluster-0}`, tailLogs: true",
            "Execution failed for task ':itAlwaysFails'."
        );
    }

    public void testUseClusterByFailingDependency() {
        BuildResult result = getTestClustersRunner(":dependsOnFailed").buildAndFail();
        assertTaskFailed(result, ":itAlwaysFails");
        assertNull(result.task(":dependsOnFailed"));
        assertStartedAndStoppedOnce(result);
        assertOutputContains(
            result.getOutput(),
            "Stopping `node{::myTestCluster-0}`, tailLogs: true",
            "Execution failed for task ':itAlwaysFails'."
        );
    }

    public void testConfigurationLocked() {
        BuildResult result = getTestClustersRunner(":illegalConfigAlter").buildAndFail();
        assertTaskFailed(result, ":illegalConfigAlter");
        assertOutputContains(
            result.getOutput(),
            "Configuration for node{::myTestCluster-0} can not be altered, already locked"
        );
    }

    @Ignore // https://github.com/elastic/elasticsearch/issues/41256
    public void testMultiNode() {
        BuildResult result = getTestClustersRunner(":multiNode").build();
        assertTaskSuccessful(result, ":multiNode");
        assertStartedAndStoppedOnce(result, "multiNode-0");
        assertStartedAndStoppedOnce(result, "multiNode-1");
        assertStartedAndStoppedOnce(result, "multiNode-2");
    }

    public void testPluginInstalled() {
        BuildResult result = getTestClustersRunner(":printLog").build();
        assertTaskSuccessful(result, ":printLog");
        assertStartedAndStoppedOnce(result);
        assertOutputContains(result.getOutput(), "-> Installed dummy");
        assertOutputContains(result.getOutput(), "loaded plugin [dummy]");
    }

    private void assertNotStarted(BuildResult result) {
        assertOutputDoesNotContain(
            result.getOutput(),
            "Starting ",
            "Stopping "
        );
    }

    private GradleRunner getTestClustersRunner(String... tasks) {
        String[] arguments = Arrays.copyOf(tasks, tasks.length + 3);
        arguments[tasks.length] = "-s";
        arguments[tasks.length + 1] = "-i";
        arguments[tasks.length + 2] = "-Dlocal.repo.path=" + getLocalTestRepoPath();
        return runner.withArguments(arguments);
    }

    private void assertStartedAndStoppedOnce(BuildResult result, String nodeName) {
        assertOutputOnlyOnce(
            result.getOutput(),
            "Starting `node{::" + nodeName + "}`",
            "Stopping `node{::" + nodeName + "}`"
        );
    }

    private void assertStartedAndStoppedOnce(BuildResult result) {
        assertStartedAndStoppedOnce(result, "myTestCluster-0");
    }


}
