/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

import java.util.Map;

public class FindTransportClassesPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<FindTransportClassesTask> transportTestExistTask = project.getTasks()
            .register("findTransportClassesTask", FindTransportClassesTask.class);
        project.getPlugins().withType(JavaPlugin.class, javaPlugin -> {
            transportTestExistTask.configure(t -> {
                FileCollection mainSourceSet = GradleUtils.getJavaSourceSets(project)
                    .getByName(SourceSet.MAIN_SOURCE_SET_NAME)
                    .getOutput()
                    .getClassesDirs();
                t.setMainSources(mainSourceSet);

                Configuration serverDependencyConfig = project.getConfigurations().create("serverDependencyConfig");
                DependencyHandler dependencyHandler = project.getDependencies();
                serverDependencyConfig.defaultDependencies(
                    deps -> deps.add(dependencyHandler.create(dependencyHandler.project(Map.of("path", ":server"))))
                );

                FileCollection compileClassPath = serverDependencyConfig.plus(
                    project.getConfigurations().getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME)
                );
                t.setCompileClasspath(compileClassPath);
            });
        });
        return transportTestExistTask;
    }
}
