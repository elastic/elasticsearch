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

public class TransportTestExistPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {

        try {
            TaskProvider<TransportTestExistTask> transportTestExistTask = project.getTasks()
                .register("transportTestExistCheck", TransportTestExistTask.class);
            project.getPlugins().withType(JavaPlugin.class, javaPlugin -> {
                FileCollection mainSourceSet = GradleUtils.getJavaSourceSets(project)
                    .getByName(SourceSet.MAIN_SOURCE_SET_NAME)
                    .getOutput()
                    .getClassesDirs();
                FileCollection testSourceSet = GradleUtils.getJavaSourceSets(project)
                    .getByName(SourceSet.TEST_SOURCE_SET_NAME)
                    .getOutput()
                    .getClassesDirs();

                Configuration serverDependencyConfig = project.getConfigurations().create("serverDependencyConfig");
                DependencyHandler dependencyHandler = project.getDependencies();
                serverDependencyConfig.defaultDependencies(
                    deps -> deps.add(dependencyHandler.create(dependencyHandler.project(Map.of("path", ":server"))))
                );

                Configuration testFrameworkConfig = project.getConfigurations().create("testFrameworkConfig");
                testFrameworkConfig.defaultDependencies(
                    deps -> deps.add(dependencyHandler.create(dependencyHandler.project(Map.of("path", ":test:framework"))))
                );

                transportTestExistTask.configure(t -> {
                    t.setMainSources(mainSourceSet);
                    t.setTestSources(testSourceSet);
                    FileCollection compileClassPath = serverDependencyConfig.plus(
                        project.getConfigurations().getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME)
                    );
                    t.setCompileClasspath(compileClassPath);
                    FileCollection byName = testFrameworkConfig.plus(
                        project.getConfigurations().getByName(JavaPlugin.TEST_COMPILE_CLASSPATH_CONFIGURATION_NAME)
                    );
                    t.setTestClasspath(byName);
                });
            });
            // somehow this does not help for rest-api-spec project
            // project.getPluginManager().withPlugin("java", p -> {
            // // We want to get any compilation error before running the pre-commit checks.
            // project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().all(sourceSet ->
            // transportTestExistTask.configure(t -> t.shouldRunAfter(sourceSet.getClassesTaskName()))
            // );
            // });
            return transportTestExistTask;

        } catch (Exception e) {
            // System.out.println(project +" failing");
            // not all projects have main source set.
            // :docs, docker etc
            // TODO how to handle this
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}
