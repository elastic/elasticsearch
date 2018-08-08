package org.elasticsearch.gradle;

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

import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.BuildTask;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;

import java.nio.file.Files;
import java.nio.file.Path;

public class ExportElasticsearchBuildResourcesTaskIT extends GradleIntegrationTestCase {

    public static final String PROJECT_NAME = "elasticsearch-build-resources";

    public void testUpToDateWithSourcesConfigured() {
        GradleRunner.create()
            .withProjectDir(getProjectDir(PROJECT_NAME))
            .withArguments("clean", "-s")
            .withPluginClasspath()
            .build();

        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir(PROJECT_NAME))
            .withArguments("exportBuildResources", "-s", "-i")
            .withPluginClasspath()
            .build();
        assertTaskSuccessFull(result, ":exportBuildResources");
        assertBuildFileExists(result, PROJECT_NAME, "build-tools-exported/checkstyle.xml");


        result = GradleRunner.create()
            .withProjectDir(getProjectDir(PROJECT_NAME))
            .withArguments("exportBuildResources", "-s", "-i")
            .withPluginClasspath()
            .build();
        assertEquals(TaskOutcome.UP_TO_DATE, result.task(":exportBuildResources").getOutcome());
        assertBuildFileExists(result, PROJECT_NAME, "build-tools-exported/checkstyle.xml");
    }

    public void testImplicitTaskDependencyCopy() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir(PROJECT_NAME))
            .withArguments("clean", "sampleCopyAll", "-s", "-i")
            .withPluginClasspath()
            .build();
        assertTaskSuccessFull(result, ":exportBuildResources");
        assertTaskSuccessFull(result, ":sampleCopyAll");
        assertBuildFileExists(result, PROJECT_NAME, "sampleCopyAll/checkstyle.xml");
    }

    public void testImplicitTaskDependencyInputFileOfOther() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir(PROJECT_NAME))
            .withArguments("clean", "sample", "-s", "-i")
            .withPluginClasspath()
            .build();

        assertBuildFileExists(result, PROJECT_NAME, "build-tools-exported/checkstyle.xml");
    }

    private void assertTaskSuccessFull(BuildResult result, String taskName) {
        BuildTask task = result.task(taskName);
        if (task == null) {
            fail("Expected task `" + taskName + "` to be successful, but it did not run");
        }
        assertEquals(TaskOutcome.SUCCESS, task.getOutcome());
    }

    private void assertBuildFileExists(BuildResult result, String projectName, String path) {
        Path absPath = getBuildDir(projectName).toPath().resolve(path);
        assertTrue(
            result.getOutput() + "\n\nExpected `" + absPath + "` to exists but it did not",
            Files.exists(absPath)
        );
    }

}
