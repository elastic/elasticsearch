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

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.registerTestTask;

/**
 * Adds a {@code resourceExhaustionTest} source set for tests that require a heap-constrained
 * cluster and must run in an isolated JVM.
 *
 * <p>Each test class runs in its own forked JVM ({@code forkEvery = 1}) so that heap state
 * from one resource-exhaustion test cannot affect another.
 *
 * <p>Consumers must declare their own compile dependencies (e.g. {@code :test:framework},
 * {@code :test:test-clusters}) on the {@code resourceExhaustionTestImplementation} configuration.
 */
public class InternalResourceExhaustionTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "resourceExhaustionTest";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(RestTestBasePlugin.class);

        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet sourceSet = sourceSets.create(SOURCE_SET_NAME);

        TaskProvider<RestIntegTestTask> testTask = registerTestTask(project, sourceSet, SOURCE_SET_NAME, RestIntegTestTask.class);

        testTask.configure(task -> {
            // Each class runs in its own JVM so heap-constrained clusters are fully isolated.
            task.setForkEvery(1L);
        });

        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(testTask));

        GradleUtils.setupIdeForTestSourceSet(project, sourceSet);
    }
}
