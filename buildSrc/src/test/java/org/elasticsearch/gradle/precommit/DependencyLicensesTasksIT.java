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
        GradleRunner runner = getGradleRunner("dependencyLicenses")
            .withArguments("clean", ":no_dependencies_and_no_folder:dependencyLicenses");

        assertTaskSuccessful(runner.build(), ":no_dependencies_and_no_folder:dependencyLicenses");
    }

    public void testDependenciesOk() {
        GradleRunner runner = getGradleRunner("dependencyLicenses")
            .withArguments("clean", ":dependencies_ok:dependencyLicenses");

        assertTaskSuccessful(runner.build(), ":dependencies_ok:dependencyLicenses");
    }

    public void testNoDependenciesWithFolder() {
        GradleRunner runner = getGradleRunner("dependencyLicenses")
            .withArguments("clean", ":no_dependencies_with_folder:dependencyLicenses");

        assertTaskFailed(runner.build(), ":no_dependencies_and_no_folder:dependencyLicenses");
    }

    public void testDependenciesMissing() {
        GradleRunner runner = getGradleRunner("dependencyLicenses")
            .withArguments("clean", ":dependencies_missing:dependencyLicenses");

        assertTaskFailed(runner.build(), ":dependencies_missing:dependencyLicenses");
    }

    public void testTooManyDependencies() {
        GradleRunner runner = getGradleRunner("dependencyLicenses")
            .withArguments("clean", ":no_dependencies_with_folder:dependencyLicenses");

        assertTaskFailed(runner.build(), ":dependencies_too_much:dependencyLicenses");
    }
}
