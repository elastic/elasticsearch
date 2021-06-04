/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.ProjectLayout;

import javax.inject.Inject;
import java.io.File;

/**
 * A plugin to handle reaping external services spawned by a build if Gradle dies.
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

    public static void registerReaperService(Project project, ProjectLayout projectLayout, boolean internal) {
        if (project != project.getRootProject()) {
            throw new IllegalArgumentException("ReaperPlugin can only be applied to the root project of a build");
        }
        File inputDir = projectLayout.getProjectDirectory()
            .dir(".gradle")
            .dir("reaper")
            .dir("build-" + ProcessHandle.current().pid())
            .getAsFile();

        project.getGradle().getSharedServices().registerIfAbsent(REAPER_SERVICE_NAME, ReaperService.class, spec -> {
            // Provide some parameters
            spec.getParameters().getInputDir().set(inputDir);
            spec.getParameters().getBuildDir().set(projectLayout.getBuildDirectory());
            spec.getParameters().setInternal(internal);
        });
    }

}
