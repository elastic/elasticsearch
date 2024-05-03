/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.jarhell;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

public class JarHellPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        Configuration jarHellConfig = project.getConfigurations().create("jarHell");
        DependencyHandler dependencyHandler = project.getDependencies();
        jarHellConfig.defaultDependencies(
            deps -> deps.add(dependencyHandler.create("org.elasticsearch:elasticsearch-core:" + VersionProperties.getElasticsearch()))
        );

        TaskProvider<? extends Task> jarHellTask = createTask(jarHellConfig, project);

        project.getPluginManager()
            .withPlugin(
                "lifecycle-base",
                p -> project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(jarHellTask))
            );
    }

    private TaskProvider<? extends Task> createTask(Configuration jarHellConfig, Project project) {
        TaskProvider<JarHellTask> jarHell = project.getTasks().register("jarHell", JarHellTask.class);

        project.getPluginManager().withPlugin("java", p -> {
            jarHell.configure(t -> {
                SourceSet testSourceSet = GradleUtils.getJavaSourceSets(project).findByName(SourceSet.TEST_SOURCE_SET_NAME);
                t.setClasspath(testSourceSet.getRuntimeClasspath());
            });
        });
        jarHell.configure(t -> { t.setJarHellRuntimeClasspath(jarHellConfig); });

        return jarHell;
    }

}
