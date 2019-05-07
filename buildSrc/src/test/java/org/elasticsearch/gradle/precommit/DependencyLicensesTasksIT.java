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
import org.gradle.testkit.runner.GradleRunner;

public class DependencyLicensesTasksIT extends GradleIntegrationTestCase {

    public void testNoDependenciesWithNoFolder() {
        String project = "no_dependencies_and_no_folder";
        GradleRunner runner = getRunner(project);

        assertTaskSuccessful(runner.build(), ":" + project + ":dependencyLicenses");
    }

    public void testDependenciesOk() {
        String project = "dependencies_ok";
        GradleRunner runner = getRunner(project);

        assertTaskSuccessful(runner.build(), ":" + project + ":dependencyLicenses");
    }

    public void testNoDependenciesWithFolder() {
        String project = "no_dependencies_with_folder";
        GradleRunner runner = getRunner(project);

        String expectedOutcome = "Licenses dir " + getProjectName(runner, project) + "licenses exists, but there are no dependencies";
        assertOutputContains(runner.buildAndFail().getOutput(), expectedOutcome);
    }

    public void testShaMissing() {
        String project = "sha_missing";
        GradleRunner runner = getRunner(project);

        String expectedOutcome = "Missing SHA for icu4j-62.1.jar. Run \"gradle updateSHAs\" to create them";
        assertOutputContains(runner.buildAndFail().getOutput(), expectedOutcome);
    }

    public void testLicensesMissing() {
        String project = "license_missing";
        GradleRunner runner = getRunner(project);

        String expectedOutcome = "Missing LICENSE for icu4j-62.1.jar, expected in icu4j-LICENSE.txt";
        assertOutputContains(runner.buildAndFail().getOutput(), expectedOutcome);
    }

    public void testTooManyDependencies() {
        String project = "dependencies_too_much";
        GradleRunner runner = getRunner(project);

        String expectedOutcome = "Unused license super-csv-LICENSE.txt";
        assertOutputContains(runner.buildAndFail().getOutput(), expectedOutcome);
    }

    private GradleRunner getRunner(String subProject) {
        String task = ":" + subProject + ":dependencyLicenses";

        return getGradleRunner("licenses")
            .withArguments("clean", task, "-Dlocal.repo.path=" + getLocalTestRepoPath())
            .withPluginClasspath();
    }

    private String getProjectName(GradleRunner runner, String subProject) {
        return runner.getProjectDir().toString() + "/" + subProject + "/";
    }
}
