/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions.precommit;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.TaskProvider;

/**
 * Base plugin for adding a precommit task.
 */
public abstract class PrecommitPlugin implements Plugin<Project> {

    public static final String PRECOMMIT_TASK_NAME = "precommit";

    @Override
    public final void apply(Project project) {
        project.getPluginManager().apply(PrecommitTaskPlugin.class);
        TaskProvider<? extends Task> task = createTask(project);
        TaskProvider<Task> precommit = project.getTasks().named(PRECOMMIT_TASK_NAME);
        precommit.configure(t -> t.dependsOn(task));
        project.getPluginManager().withPlugin("java", p -> {
            // We want to get any compilation error before running the pre-commit checks.
            project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().all(sourceSet ->
                    task.configure(t -> t.shouldRunAfter(sourceSet.getClassesTaskName()))
            );
        });
    }

    public abstract TaskProvider<? extends Task> createTask(Project project);

}
