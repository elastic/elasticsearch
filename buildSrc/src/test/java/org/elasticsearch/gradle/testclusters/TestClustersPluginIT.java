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
import org.gradle.testkit.runner.TaskOutcome;

public class TestClustersPluginIT extends GradleIntegrationTestCase {

    public void testListClusters() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments("listElasticSearchClusters", "-s")
            .withPluginClasspath()
            .build();

        assertEquals(TaskOutcome.SUCCESS, result.task(":listElasticSearchClusters").getOutcome());
        assertOutputContains(
            result.getOutput(),
                "   * myTestCluster:"
        );
    }

    public void testUseClusterByOne() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments("user1", "-s")
            .withPluginClasspath()
            .build();

        assertEquals(TaskOutcome.SUCCESS, result.task(":user1").getOutcome());
        assertOutputContains(
            result.getOutput(),
                "Starting `ElasticsearchNode{name='myTestCluster', noOfClaims=1, started=true}`",
                "Stopping `ElasticsearchNode{name='myTestCluster', noOfClaims=0, started=false}`, number of claims is 0"
        );
    }


    public void testUseClusterByOneWithDryRun() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments("user1", "-s", "--dry-run")
            .withPluginClasspath()
            .build();

        assertNull(result.task(":user1"));
        assertOutputDoesNotContain(
            result.getOutput(),
            "Starting `myTestCluster`",
            "Stopping `myTestCluster`, number of claims is 0"
        );
    }

    public void testUseClusterByTwo() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments("user1", "user2", "-s")
            .withPluginClasspath()
            .build();

        assertEquals(TaskOutcome.SUCCESS, result.task(":user1").getOutcome());
        assertEquals(TaskOutcome.SUCCESS, result.task(":user2").getOutcome());
        assertOutputContains(
            result.getOutput(),
            "Starting `ElasticsearchNode{name='myTestCluster', noOfClaims=2, started=true}`",
            "Not stopping `ElasticsearchNode{name='myTestCluster', noOfClaims=1, started=true}`, since node still has 1 claim(s)",
            "Stopping `ElasticsearchNode{name='myTestCluster', noOfClaims=0, started=false}`, number of claims is 0"
        );
    }

    public void testUseClusterByUpToDateTask() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments("upToDate1", "upToDate2", "-s")
            .withPluginClasspath()
            .build();

        assertEquals(TaskOutcome.UP_TO_DATE, result.task(":upToDate1").getOutcome());
        assertEquals(TaskOutcome.UP_TO_DATE, result.task(":upToDate2").getOutcome());
        assertOutputContains(
            result.getOutput(),
            "Not stopping `ElasticsearchNode{name='myTestCluster', noOfClaims=1, started=false}`, since node still has 1 claim(s)",
            "`ElasticsearchNode{name='myTestCluster', noOfClaims=0, started=false}` was not running, no need to stop"
        );
        assertOutputDoesNotContain(result.getOutput(), "Starting `myTestCluster`");
    }

    public void testUseClusterBySkippedTask() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments("skipped1", "skipped2", "-s")
            .withPluginClasspath()
            .build();

        assertEquals(TaskOutcome.SKIPPED, result.task(":skipped1").getOutcome());
        assertEquals(TaskOutcome.SKIPPED, result.task(":skipped2").getOutcome());
        assertOutputContains(
            result.getOutput(),
            "Not stopping `ElasticsearchNode{name='myTestCluster', noOfClaims=1, started=false}`, since node still has 1 claim(s)",
            "`ElasticsearchNode{name='myTestCluster', noOfClaims=0, started=false}` was not running, no need to stop"
        );
        assertOutputDoesNotContain(result.getOutput(), "Starting cluster: myTestCluster");
    }

    public void tetUseClusterBySkippedAndWorkingTask() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments("skipped1", "user1", "-s")
            .withPluginClasspath()
            .build();

        assertEquals(TaskOutcome.SKIPPED, result.task(":skipped1").getOutcome());
        assertEquals(TaskOutcome.SUCCESS, result.task(":user1").getOutcome());
        assertOutputContains(
            result.getOutput(),
            "> Task :user1",
            "Starting `myTestCluster`",
            "Stopping `myTestCluster`, number of claims is 0"
        );
    }

    public void testMultiProject() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters_multiproject"))
            .withArguments("user1", "user2", "-s", "--parallel")
            .withPluginClasspath()
            .build();

        assertEquals(TaskOutcome.SUCCESS, result.task(":user1").getOutcome());
        assertEquals(TaskOutcome.SUCCESS, result.task(":user2").getOutcome());
        String output = result.getOutput();
        assertOutputContains(
            output,
            "Starting `ElasticsearchNode{name='myTestCluster', noOfClaims=2, started=true}`",
            "Not stopping `ElasticsearchNode{name='myTestCluster', noOfClaims=1, started=true}`, since node still has 1 claim(s)",
            "Stopping `ElasticsearchNode{name='myTestCluster', noOfClaims=0, started=false}`, number of claims is 0"
        );
        assertOutputOnlyOnce(output, "Task :synctestclustersArtifacts");
    }

    public void testUseClusterByFailingOne() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments("itAlwaysFails", "-s")
            .withPluginClasspath()
            .buildAndFail();

        assertEquals(TaskOutcome.FAILED, result.task(":itAlwaysFails").getOutcome());
        assertOutputContains(
            result.getOutput(),
            "Starting `ElasticsearchNode{name='myTestCluster', noOfClaims=1, started=true}`",
            "Forcefully stopping `ElasticsearchNode{name='myTestCluster', noOfClaims=1, started=false}`, number of claims is 1",
            "Execution failed for task ':itAlwaysFails'."
        );
    }

    public void testUseClusterByFailingDependency() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("testclusters"))
            .withArguments("dependsOnFailed", "-s")
            .withPluginClasspath()
            .buildAndFail();

        assertEquals(TaskOutcome.FAILED, result.task(":itAlwaysFails").getOutcome());
        assertNull(result.task(":dependsOnFailed"));
        assertOutputContains(
            result.getOutput(),
            "Starting `ElasticsearchNode{name='myTestCluster', noOfClaims=2, started=true}`",
            "Forcefully stopping `ElasticsearchNode{name='myTestCluster', noOfClaims=2, started=false}`, number of claims is 2"
        );
    }

}
