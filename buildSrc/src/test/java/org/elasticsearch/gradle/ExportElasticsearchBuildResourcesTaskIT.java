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
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;

import java.io.File;

public class ExportElasticsearchBuildResourcesTaskIT extends GradleIntegrationTestCase {

    public static final String PROJECT_NAME = "elasticsearch-build-resources";

    public void testUpToDateWithSourcesConfigured() {
        GradleRunner.create()
            .withProjectDir(getProjectDir(PROJECT_NAME))
            .withArguments("clean", "-s")
            .withPluginClasspath()
            .build();

        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("elasticsearch-build-resources"))
            .withArguments("exportBuildResources", "-s", "-i")
            .withPluginClasspath()
            .build();
        assertEquals(TaskOutcome.SUCCESS, result.task(":exportBuildResources").getOutcome());
        assertTrue(new File(getBuildDir(PROJECT_NAME), "checkstyle-1.xml").exists());
        assertTrue(new File(getBuildDir(PROJECT_NAME), "checkstyle-2.xml").exists());
        assertTrue(new File(getBuildDir(PROJECT_NAME), "checkstyle-3.xml").exists());

        result = GradleRunner.create()
            .withProjectDir(getProjectDir("elasticsearch-build-resources"))
            .withArguments("exportBuildResources", "-s", "-i")
            .withPluginClasspath()
            .build();
        assertEquals(TaskOutcome.UP_TO_DATE, result.task(":exportBuildResources").getOutcome());
        assertTrue(new File(getBuildDir(PROJECT_NAME), "checkstyle-1.xml").exists());
        assertTrue(new File(getBuildDir(PROJECT_NAME), "checkstyle-2.xml").exists());
        assertTrue(new File(getBuildDir(PROJECT_NAME), "checkstyle-3.xml").exists());


    }

}
