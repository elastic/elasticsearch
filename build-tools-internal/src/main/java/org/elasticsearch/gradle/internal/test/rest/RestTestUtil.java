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
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

/**
 * Utility class to configure the necessary tasks and dependencies.
 */
public class RestTestUtil {

    private RestTestUtil() {}
    //
    // static ElasticsearchCluster createTestCluster(Project project, SourceSet sourceSet) {
    // // eagerly create the testCluster container so it is easily available for configuration
    // @SuppressWarnings("unchecked")
    // NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
    // .getExtensions()
    // .getByName(TestClustersPlugin.EXTENSION_NAME);
    // return testClusters.create(sourceSet.getName());
    // }

    /**
     * Creates a task with the source set name of type {@link RestIntegTestTask}
     */
    public static TaskProvider<RestIntegTestTask> registerTestTask(Project project, SourceSet sourceSet) {
        return registerTestTask(project, sourceSet, sourceSet.getName());
    }

    /**
     * Creates a {@link RestIntegTestTask} task with a custom name for the provided source set
     */
    public static TaskProvider<RestIntegTestTask> registerTestTask(Project project, SourceSet sourceSet, String taskName) {
        return registerTestTask(project, sourceSet, taskName, RestIntegTestTask.class);
    }

    /**
     * Creates a {@link T} task with a custom name for the provided source set
     *
     * @param <T> test task type
     */
    public static <T extends Test> TaskProvider<T> registerTestTask(Project project, SourceSet sourceSet, String taskName, Class<T> clazz) {
        // lazily create the test task
        return project.getTasks().register(taskName, clazz, testTask -> {
            testTask.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            testTask.setDescription("Runs the REST tests against an external cluster");
            project.getPlugins().withType(JavaPlugin.class, t -> testTask.mustRunAfter(project.getTasks().named("test")));

            testTask.setTestClassesDirs(sourceSet.getOutput().getClassesDirs());
            testTask.setClasspath(sourceSet.getRuntimeClasspath());
        });
    }

    /**
     * Setup the dependencies needed for the REST tests.
     */
    public static void setupTestDependenciesDefaults(Project project, SourceSet sourceSet) {
        project.getDependencies().add(sourceSet.getImplementationConfigurationName(), project.project(":test:framework"));
    }

    public static void setupYamlRestTestDependenciesDefaults(Project project, SourceSet sourceSet) {
        setupYamlRestTestDependenciesDefaults(project, sourceSet, false);
    }

    public static void setupYamlRestTestDependenciesDefaults(Project project, SourceSet sourceSet, boolean useNewTestClusters) {
        Project testFramework = project.findProject(":test:framework");
        // we shield the project dependency to make integration tests easier
        if (testFramework != null) {
            project.getDependencies().add(sourceSet.getImplementationConfigurationName(), testFramework);
            if (useNewTestClusters) {
                project.getDependencies().add(sourceSet.getImplementationConfigurationName(), project.project(":test:test-clusters"));
            }
        }
    }

    /**
     * Setup the dependencies needed for the Java REST tests.
     */
    public static void setupJavaRestTestDependenciesDefaults(Project project, SourceSet sourceSet) {
        // TODO: this should just be test framework, but some cleanup is needed in places incorrectly specifying java vs yaml
        // we shield the project dependency to make integration tests easier
        Project testFramework = project.findProject(":test:framework");
        if (testFramework != null) {
            project.getDependencies().add(sourceSet.getImplementationConfigurationName(), testFramework);
        }
    }
}
