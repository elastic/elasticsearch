/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.TaskProvider;

public class DependencyLicensesPrecommitPlugin extends PrecommitPlugin {

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPlugins().apply(CompileOnlyResolvePlugin.class);
        TaskProvider<DependencyLicensesTask> dependencyLicenses = project.getTasks()
            .register("dependencyLicenses", DependencyLicensesTask.class);

        // only require dependency licenses for non-elasticsearch deps
        dependencyLicenses.configure(t -> {
            Configuration runtimeClasspath = project.getConfigurations().getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME);
            Configuration compileOnly = project.getConfigurations()
                .getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME);
            t.setDependencies(
                runtimeClasspath.fileCollection(
                    dependency -> dependency instanceof ProjectDependency == false
                        && dependency.getGroup().startsWith("org.elasticsearch") == false
                ).minus(compileOnly)
            );
        });
        return dependencyLicenses;
    }
}
