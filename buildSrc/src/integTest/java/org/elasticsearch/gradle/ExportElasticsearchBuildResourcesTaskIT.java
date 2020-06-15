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

public class ExportElasticsearchBuildResourcesTaskIT extends GradleIntegrationTestCase {

    public static final String PROJECT_NAME = "elasticsearch-build-resources";

    public void testUpToDateWithSourcesConfigured() {
        getGradleRunner(PROJECT_NAME).withArguments("clean", "-s").build();

        BuildResult result = getGradleRunner(PROJECT_NAME).withArguments("buildResources", "-s", "-i").build();
        assertTaskSuccessful(result, ":buildResources");
        assertBuildFileExists(result, PROJECT_NAME, "build-tools-exported/checkstyle.xml");
        assertBuildFileExists(result, PROJECT_NAME, "build-tools-exported/checkstyle_suppressions.xml");

        result = getGradleRunner(PROJECT_NAME).withArguments("buildResources", "-s", "-i").build();
        assertTaskUpToDate(result, ":buildResources");
        assertBuildFileExists(result, PROJECT_NAME, "build-tools-exported/checkstyle.xml");
        assertBuildFileExists(result, PROJECT_NAME, "build-tools-exported/checkstyle_suppressions.xml");
    }

    public void testOutputAsInput() {
        BuildResult result = getGradleRunner(PROJECT_NAME).withArguments("clean", "sampleCopy", "-s", "-i").build();

        assertTaskSuccessful(result, ":sampleCopy");
        assertBuildFileExists(result, PROJECT_NAME, "sampleCopy/checkstyle.xml");
        assertBuildFileExists(result, PROJECT_NAME, "sampleCopy/checkstyle_suppressions.xml");
    }

    public void testIncorrectUsage() {
        assertOutputContains(
            getGradleRunner(PROJECT_NAME).withArguments("noConfigAfterExecution", "-s", "-i").buildAndFail().getOutput(),
            "buildResources can't be configured after the task ran"
        );
    }
}
