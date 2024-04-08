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

public class JavaModulePrecommitPlugin extends PrecommitPlugin {

    public static final String TASK_NAME = "validateModule";

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<JavaModulePrecommitTask> task = project.getTasks().register(TASK_NAME, JavaModulePrecommitTask.class);
        task.configure(t -> {
            SourceSet mainSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME);
            t.dependsOn(mainSourceSet.getClassesTaskName());
            t.getSrcDirs().set(project.provider(() -> mainSourceSet.getAllSource().getSrcDirs()));
            t.setClasspath(project.getConfigurations().getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME));
            t.setClassesDirs(mainSourceSet.getOutput().getClassesDirs());
            t.setResourcesDirs(mainSourceSet.getOutput().getResourcesDir());
        });
        return task;
    }
}
