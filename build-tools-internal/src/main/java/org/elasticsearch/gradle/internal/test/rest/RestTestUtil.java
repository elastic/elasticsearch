/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.elasticsearch.gradle.internal.test.RestIntegTestTask;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;

/**
 * Utility class to configure the necessary tasks and dependencies.
 */
public class RestTestUtil {

    private RestTestUtil() {}

    /**
     * Creates a {@link RestIntegTestTask} task with the source set of the same name
     */
    public static Provider<RestIntegTestTask> registerTestTask(Project project, SourceSet sourceSet) {
        return registerTestTask(project, sourceSet, sourceSet.getName());
    }

    /**
     * Creates a {@link RestIntegTestTask} task with a custom name for the provided source set
     */
    public static Provider<RestIntegTestTask> registerTestTask(Project project, SourceSet sourceSet, String taskName) {
        // lazily create the test task
        return project.getTasks().register(taskName, RestIntegTestTask.class, testTask -> {
            testTask.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            testTask.setDescription("Runs the REST tests against an external cluster");
            project.getPlugins().withType(JavaPlugin.class, t -> testTask.mustRunAfter(project.getTasks().named("test")));

            testTask.setTestClassesDirs(sourceSet.getOutput().getClassesDirs());
            testTask.setClasspath(sourceSet.getRuntimeClasspath());
        });
    }

    /**
     * Setup the dependencies needed for the YAML REST tests.
     */
    public static void setupYamlRestTestDependenciesDefaults(Project project, SourceSet sourceSet) {
        project.getDependencies().add(sourceSet.getImplementationConfigurationName(), project.project(":test:yaml-rest-runner"));
    }

    /**
     * Setup the dependencies needed for the Java REST tests.
     */
    public static void setupJavaRestTestDependenciesDefaults(Project project, SourceSet sourceSet) {
        // TODO: this should just be test framework, but some cleanup is needed in places incorrectly specifying java vs yaml
        project.getDependencies().add(sourceSet.getImplementationConfigurationName(), project.project(":test:yaml-rest-runner"));
    }
}
