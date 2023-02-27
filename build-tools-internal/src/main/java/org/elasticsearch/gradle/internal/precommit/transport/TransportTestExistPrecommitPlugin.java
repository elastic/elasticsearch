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
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

public class TransportTestExistPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<TransportTestExistTask> transportTestExistTask = project.getTasks()
            .register("transportTestExistCheck", TransportTestExistTask.class);
        try {
            FileCollection mainSourceSet = GradleUtils.getJavaSourceSets(project)
                .getByName(SourceSet.MAIN_SOURCE_SET_NAME)
                .getOutput()
                .getClassesDirs();
            FileCollection testSourceSet = GradleUtils.getJavaSourceSets(project)
                .getByName(SourceSet.TEST_SOURCE_SET_NAME)
                .getOutput()
                .getClassesDirs();

            transportTestExistTask.configure(t -> {
                t.setMainSources(mainSourceSet);
                t.setTestSources(testSourceSet);
                t.setCompileClasspath(project.getConfigurations().getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME));
                t.setTestClasspath(project.getConfigurations().getByName(JavaPlugin.TEST_COMPILE_CLASSPATH_CONFIGURATION_NAME));
            });
            // somehow this does not help for rest-api-spec project
            // project.getPluginManager().withPlugin("java", p -> {
            // // We want to get any compilation error before running the pre-commit checks.
            // project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().all(sourceSet ->
            // transportTestExistTask.configure(t -> t.shouldRunAfter(sourceSet.getClassesTaskName()))
            // );
            // });
        } catch (Exception e) {
            // System.out.println(project +" failing");
            // not all projects have main source set.
            // :docs, docker etc
            // TODO how to handle this
        }
        return transportTestExistTask;

    }
}
