/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.ProjectLayout;

import java.io.File;

import javax.inject.Inject;

/**
 * A plugin to handle reaping external services spawned by a build if Gradle dies.
 * <p>
 * The reaper is a per-build {@link ReaperService}, so the plugin can be applied to any project of a build.
 */
public class ReaperPlugin implements Plugin<Project> {

    /**
     * The unique identifier to register the reaper shared service within a gradle build
     * */
    public static final String REAPER_SERVICE_NAME = "reaper";
    private final ProjectLayout projectLayout;

    @Inject
    ReaperPlugin(ProjectLayout projectLayout) {
        this.projectLayout = projectLayout;
    }

    @Override
    public void apply(Project project) {
        registerReaperService(project, projectLayout, false);
    }

    /**
     * Registers the per-build {@link ReaperService} unless it has already been registered.
     * <p>
     * This can be called from any project of a build. The reaper state lives below the settings directory, so no project needs
     * to touch the root project, which Isolated Projects forbids.
     */
    public static void registerReaperService(Project project, ProjectLayout projectLayout, boolean internal) {
        Directory settingsDir = projectLayout.getSettingsDirectory();
        File inputDir = settingsDir.dir(".gradle").dir("reaper").dir("build-" + ProcessHandle.current().pid()).getAsFile();

        project.getGradle().getSharedServices().registerIfAbsent(REAPER_SERVICE_NAME, ReaperService.class, spec -> {
            // Provide some parameters
            spec.getParameters().getInputDir().set(inputDir);
            spec.getParameters().getBuildDir().set(settingsDir.dir("build"));
            spec.getParameters().setInternal(internal);
        });
    }

}
