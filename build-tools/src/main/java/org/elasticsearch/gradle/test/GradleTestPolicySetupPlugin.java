/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.tasks.testing.Test;

public class GradleTestPolicySetupPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        Gradle gradle = project.getGradle();
        project.getTasks().withType(Test.class).configureEach(test -> {
            test.systemProperty("tests.gradle", true);
            test.systemProperty("tests.task", test.getPath());

            SystemPropertyCommandLineArgumentProvider nonInputProperties = new SystemPropertyCommandLineArgumentProvider();
            // don't track these as inputs since they contain absolute paths and break cache relocatability
            nonInputProperties.systemProperty("gradle.dist.lib", gradle.getGradleHomeDir().getAbsolutePath() + "/lib");
            nonInputProperties.systemProperty(
                "gradle.worker.jar",
                gradle.getGradleUserHomeDir().getAbsolutePath() + "/caches/" + gradle.getGradleVersion() + "/workerMain/gradle-worker.jar"
            );
            test.getJvmArgumentProviders().add(nonInputProperties);
        });
    }
}
