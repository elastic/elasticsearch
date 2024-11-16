/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.component.ComponentIdentifier;
import org.gradle.api.artifacts.component.ModuleComponentIdentifier;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.TaskProvider;

public class DependencyLicensesPrecommitPlugin extends PrecommitPlugin {
    private static Spec<ComponentIdentifier> COMPONENT_FILTER = identifier -> (identifier instanceof ModuleComponentIdentifier)
        && ((ModuleComponentIdentifier) identifier).getGroup().startsWith("org.elasticsearch") == false;

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPlugins().apply(CompileOnlyResolvePlugin.class);
        var dependencyLicenses = project.getTasks().register("dependencyLicenses", DependencyLicensesTask.class, t -> {
            var runtimeClasspath = project.getConfigurations().getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME);
            var compileOnly = project.getConfigurations().getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME);
            t.configureDependencies(runtimeClasspath, compileOnly, COMPONENT_FILTER);
        });
        return dependencyLicenses;
    }
}
