/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class SplitPackagesAuditPrecommitPlugin extends PrecommitPlugin {
    public static final String TASK_NAME = "splitPackagesAudit";

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<SplitPackagesAuditTask> task = project.getTasks().register(TASK_NAME, SplitPackagesAuditTask.class);
        task.configure(t -> {
            t.setProjectBuildDirs(getProjectBuildDirs(project));
            t.setClasspath(project.getConfigurations().getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME));
            SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
            t.getSrcDirs().set(project.provider(() -> mainSourceSet.getAllSource().getSrcDirs()));
        });
        return task;
    }

    private static Map<File, String> getProjectBuildDirs(Project project) {
        // while this is done in every project, it should be cheap to calculate
        Map<File, String> buildDirs = new HashMap<>();
        for (Project p : project.getRootProject().getAllprojects()) {
            buildDirs.put(p.getBuildDir(), p.getPath());
        }
        return buildDirs;
    }
}
