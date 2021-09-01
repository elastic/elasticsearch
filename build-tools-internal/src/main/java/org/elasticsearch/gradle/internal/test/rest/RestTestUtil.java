/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.elasticsearch.gradle.internal.test.RestIntegTestTask;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.api.tasks.bundling.Zip;

/**
 * Utility class to configure the necessary tasks and dependencies.
 */
public class RestTestUtil {

    private RestTestUtil() {
    }

    /**
     * Creates a task with the source set name of type {@link RestIntegTestTask}
     */
    public static Provider<RestIntegTestTask> registerTestTask(Project project, SourceSet sourceSet) {
        // lazily create the test task
        return project.getTasks().register(sourceSet.getName(), RestIntegTestTask.class, testTask -> {
            testTask.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            testTask.setDescription("Runs the REST tests against an external cluster");
            project.getPlugins().withType(JavaPlugin.class, t ->
                testTask.mustRunAfter(project.getTasks().named("test"))
            );

            testTask.setTestClassesDirs(sourceSet.getOutput().getClassesDirs());
            testTask.setClasspath(sourceSet.getRuntimeClasspath());
            // if this a module or plugin, it may have an associated zip file with it's contents, add that to the test cluster
            project.getPluginManager().withPlugin("elasticsearch.internal-es-plugin", plugin -> {
                TaskProvider<Zip> bundle = project.getTasks().withType(Zip.class).named("bundlePlugin");
                testTask.dependsOn(bundle);
                if (GradleUtils.isModuleProject(project.getPath())) {
                    testTask.getClusters().forEach(c -> c.module(bundle.flatMap(AbstractArchiveTask::getArchiveFile)));
                } else {
                    testTask.getClusters().forEach(c -> c.plugin(bundle.flatMap(AbstractArchiveTask::getArchiveFile)));
                }
            });
        });
    }

    /**
     * Setup the dependencies needed for the REST tests.
     */
    public static void setupTestDependenciesDefaults(Project project, SourceSet sourceSet) {
        project.getDependencies().add(sourceSet.getImplementationConfigurationName(), project.project(":test:framework"));
    }

}
