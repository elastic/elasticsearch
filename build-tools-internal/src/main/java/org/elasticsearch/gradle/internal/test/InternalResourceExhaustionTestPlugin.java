/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.test.rest.RestTestBasePlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import java.util.Map;

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.registerTestTask;

/**
 * Adds a {@code resourceExhaustionTest} source set for tests that require a heap-constrained
 * cluster and must not share a JVM with other test suites.
 *
 * <p>Each test class runs in its own forked JVM ({@code forkEvery = 1}) with only one class
 * running at a time ({@code maxParallelForks = 1}), preventing concurrent resource-constrained
 * clusters from competing for system memory.
 *
 * <p>The {@code :test:resource-exhaustion-framework} and {@code :test:test-clusters} projects
 * are automatically added to the source set's implementation classpath.
 */
public class InternalResourceExhaustionTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "resourceExhaustionTest";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(RestTestBasePlugin.class);

        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet sourceSet = sourceSets.create(SOURCE_SET_NAME);

        if (project.findProject(":test:resource-exhaustion-framework") != null) {
            project.getDependencies()
                .add(
                    sourceSet.getImplementationConfigurationName(),
                    project.getDependencies().project(Map.of("path", ":test:resource-exhaustion-framework"))
                );
        }

        if (project.findProject(":test:test-clusters") != null) {
            project.getDependencies()
                .add(
                    sourceSet.getImplementationConfigurationName(),
                    project.getDependencies().project(Map.of("path", ":test:test-clusters"))
                );
        }

        TaskProvider<RestIntegTestTask> testTask = registerTestTask(project, sourceSet, SOURCE_SET_NAME, RestIntegTestTask.class);

        testTask.configure(task -> {
            // Each class runs in its own JVM so heap-constrained clusters are fully isolated.
            task.setForkEvery(1L);
            // Only one resource-exhaustion cluster at a time to avoid memory pressure in CI.
            task.setMaxParallelForks(1);
        });

        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(testTask));

        GradleUtils.setupIdeForTestSourceSet(project, sourceSet);
    }
}
