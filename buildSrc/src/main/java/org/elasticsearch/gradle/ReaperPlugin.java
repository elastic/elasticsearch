/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import java.nio.file.Path;

/**
 * A plugin to handle reaping external services spawned by a build if Gradle dies.
 */
public class ReaperPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalArgumentException("ReaperPlugin can only be applied to the root project of a build");
        }

        project.getPlugins().apply(GlobalBuildInfoPlugin.class);

        Path inputDir = project.getRootDir()
            .toPath()
            .resolve(".gradle")
            .resolve("reaper")
            .resolve("build-" + ProcessHandle.current().pid());

        var reaperServiceProvider = project.getGradle().getSharedServices().registerIfAbsent("reaper", ReaperService.class, spec -> {
            // Provide some parameters
            spec.getParameters().getInputDir().set(inputDir);
            spec.getParameters().getBuildDir().set(project.getBuildDir().toPath());
            spec.getParameters().getInternal().set(BuildParams.isInternal());
            spec.getParameters().getLogger().set(project.getLogger());
        });

        project.getExtensions().create("reaper", ReaperExtension.class, reaperServiceProvider);
    }

}
