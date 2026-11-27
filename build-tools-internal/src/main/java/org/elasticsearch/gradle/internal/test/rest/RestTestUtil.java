/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.util.Map;

/**
 * Utility class to configure the necessary tasks and dependencies.
 */
public class RestTestUtil {

    private RestTestUtil() {}

    /**
     * Registers a plain {@link Test} task with the given name, wired to the provided source set, as a REST
     * integ test in the {@code restTests} extension so that {@link RestTestBasePlugin} applies standard
     * REST integ-test configuration (distribution wiring, system properties, caching, etc.).
     */
    public static TaskProvider<Test> registerPlainRestTestTask(Project project, SourceSet sourceSet, String taskName) {
        return project.getExtensions().getByType(RestIntegTestsExtension.class).register(taskName, testTask -> {
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
            project.getDependencies()
                .add(
                    sourceSet.getImplementationConfigurationName(),
                    project.getDependencies().project(Map.of("path", yamlTestRunnerProject.getPath()))
                );
            if (useNewTestClusters) {
                project.getDependencies()
                    .add(
                        sourceSet.getImplementationConfigurationName(),
                        project.getDependencies().project(Map.of("path", ":test:test-clusters"))
                    );
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
            project.getDependencies()
                .add(
                    sourceSet.getImplementationConfigurationName(),
                    project.getDependencies().project(Map.of("path", yamlTestRunnerProject.getPath()))
                );
        }
    }
}
