/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.info.BuildParams;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFile;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.testing.Test;

import java.util.Arrays;
import java.util.List;

public class MutedTestPlugin implements Plugin<Project> {
    private static final String ADDITIONAL_FILES_PROPERTY = "org.elasticsearch.additional.muted.tests";

    @Override
    public void apply(Project project) {
        String additionalFilePaths = project.hasProperty(ADDITIONAL_FILES_PROPERTY)
            ? project.property(ADDITIONAL_FILES_PROPERTY).toString()
            : "";
        List<RegularFile> additionalFiles = Arrays.stream(additionalFilePaths.split(","))
            .filter(p -> p.isEmpty() == false)
            .map(p -> project.getRootProject().getLayout().getProjectDirectory().file(p))
            .toList();

        Provider<MutedTestsBuildService> mutedTestsProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent("mutedTests", MutedTestsBuildService.class, spec -> {
                spec.getParameters().getInfoPath().set(project.getRootProject().getProjectDir());
                spec.getParameters().getAdditionalFiles().set(additionalFiles);
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
