/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit;

import org.gradle.api.Plugin;
import org.gradle.api.initialization.ProjectDescriptor;
import org.gradle.api.initialization.Settings;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Registers a shared build service mapping project build directories to project paths.
 *
 * <p>The service is seeded from the settings {@link ProjectDescriptor} tree to avoid scanning
 * all live {@code Project} instances during project configuration.
 */
public class ProjectBuildDirsSettingsPlugin implements Plugin<Settings> {

    @Override
    public void apply(Settings settings) {
        settings.getGradle().settingsEvaluated(ignored -> {
            Map<File, String> projectBuildDirs = collectProjectBuildDirs(settings.getRootProject());
            settings.getGradle().getSharedServices().registerIfAbsent(ProjectBuildDirsService.NAME, ProjectBuildDirsService.class, spec -> {
                spec.getParameters().getProjectBuildDirs().set(projectBuildDirs);
            });
        });
    }

    static Map<File, String> collectProjectBuildDirs(ProjectDescriptor root) {
        Map<File, String> projectBuildDirs = new LinkedHashMap<>();
        collectProjectBuildDirs(root, projectBuildDirs);
        return projectBuildDirs;
    }

    private static void collectProjectBuildDirs(ProjectDescriptor project, Map<File, String> projectBuildDirs) {
        File buildDir = project.getProjectDir().toPath().resolve("build").normalize().toFile();
        projectBuildDirs.put(buildDir, project.getPath());
        for (ProjectDescriptor child : project.getChildren()) {
            collectProjectBuildDirs(child, projectBuildDirs);
        }
    }
}
