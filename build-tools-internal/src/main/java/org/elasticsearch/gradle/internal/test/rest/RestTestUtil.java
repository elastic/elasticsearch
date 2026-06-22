/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.elasticsearch.gradle.internal.test.RestIntegTestTask;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

/**
 * Utility class to configure the necessary tasks and dependencies.
 */
public class RestTestUtil {

    private RestTestUtil() {}

    /**
     * Registers a plain {@link Test} task with the given name, wired to the provided source set, and
     * attaches the {@link RestIntegTestSpec} marker so that {@link RestTestBasePlugin} applies standard
     * REST integ-test configuration (distribution wiring, system properties, caching, etc.).
     * <p>
     * Prefer this factory over the typed {@link #registerTestTask(Project, SourceSet, String, Class)}
     * overload when no Gradle test-cluster functionality is required.
     */
    public static TaskProvider<Test> registerPlainRestTestTask(Project project, SourceSet sourceSet, String taskName) {
        // Enroll BEFORE registering so that RestTestBasePlugin's configureEach (plain
        // withType(Test).configureEach) fires before the register action and therefore
        // before any build-script restTests.tasks.configureEach closures.
        project.getExtensions().getByType(RestIntegTests.class).enroll(taskName);
        return project.getTasks().register(taskName, Test.class, testTask -> {
            testTask.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            testTask.setDescription("Runs the REST tests against an external cluster");
            project.getPlugins().withType(JavaPlugin.class, t -> testTask.mustRunAfter(project.getTasks().named("test")));
            testTask.setTestClassesDirs(sourceSet.getOutput().getClassesDirs());
            testTask.setClasspath(sourceSet.getRuntimeClasspath());
            // Mark so the task is detectable via task.getExtensions().findByType(RestIntegTestSpec.class)
            RestIntegTestSpec.mark(testTask);
            // Preserve cacheability: plain Test tasks are not @CacheableTask, so opt in explicitly
            testTask.getOutputs().cacheIf("REST integ test", t -> true);
        });
    }

    /**
     * Creates a {@link RestIntegTestTask} task with the source set of the same name
     */
    public static Provider<RestIntegTestTask> registerTestTask(Project project, SourceSet sourceSet) {
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
     * Setup the dependencies needed for the YAML REST tests.
     */
    public static void setupYamlRestTestDependenciesDefaults(Project project, SourceSet sourceSet) {
        setupYamlRestTestDependenciesDefaults(project, sourceSet, false);
    }

    /**
     * Setup the dependencies needed for the YAML REST tests.
     */
    public static void setupYamlRestTestDependenciesDefaults(Project project, SourceSet sourceSet, boolean useNewTestClusters) {
        Project yamlTestRunnerProject = project.findProject(":test:yaml-rest-runner");
        // we shield the project dependency to make integration tests easier
        if (yamlTestRunnerProject != null) {
            project.getDependencies().add(sourceSet.getImplementationConfigurationName(), yamlTestRunnerProject);
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
        Project yamlTestRunnerProject = project.findProject(":test:yaml-rest-runner");
        if (yamlTestRunnerProject != null) {
            project.getDependencies().add(sourceSet.getImplementationConfigurationName(), yamlTestRunnerProject);
        }
    }
}
