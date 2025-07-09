/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFile;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

public class MutedTestPlugin implements Plugin<Project> {
    private static final String ADDITIONAL_FILES_PROPERTY = "org.elasticsearch.additional.muted.tests";

    @Override
    public void apply(Project project) {
        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        var buildParams = loadBuildParams(project).get();

        File settingsRoot = project.getLayout().getSettingsDirectory().getAsFile();
        String additionalFilePaths = project.hasProperty(ADDITIONAL_FILES_PROPERTY)
            ? project.property(ADDITIONAL_FILES_PROPERTY).toString()
            : "";
        List<RegularFile> additionalFiles = Arrays.stream(additionalFilePaths.split(","))
            .filter(p -> p.isEmpty() == false)
            .map(p -> project.getLayout().getSettingsDirectory().file(p))
            .toList();

        Provider<MutedTestsBuildService> mutedTestsProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent("mutedTests", MutedTestsBuildService.class, spec -> {
                spec.getParameters().getInfoPath().set(settingsRoot);
                spec.getParameters().getAdditionalFiles().set(additionalFiles);
            });

        project.getTasks().withType(Test.class).configureEach(test -> {
            test.filter(filter -> {
                for (String exclude : mutedTestsProvider.get().getExcludePatterns()) {
                    filter.excludeTestsMatching(exclude);
                }

                // Don't fail when all tests are ignored when running in CI
                filter.setFailOnNoMatchingTests(buildParams.getCi() == false);
            });
        });
    }
}
