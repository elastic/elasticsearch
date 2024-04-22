/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.testing.Test;

import java.io.File;

public class MutedTestPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        File infoPath = new File(Util.locateElasticsearchWorkspace(project.getGradle()), "build-tools-internal");
        Provider<MutedTestsBuildService> mutedTestsProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent("mutedTests", MutedTestsBuildService.class, spec -> {
                spec.getParameters().getInfoPath().set(infoPath);
            });

        project.getTasks().withType(Test.class).configureEach(test -> {
            test.filter(filter -> {
                for (String exclude : mutedTestsProvider.get().getExcludePatterns()) {
                    filter.excludeTestsMatching(exclude);
                }

                // Don't fail when all tests are ignored when running in CI
                filter.setFailOnNoMatchingTests(BuildParams.isCi() == false);
            });
        });
    }
}
