/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask;
import org.gradle.api.Action;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskCollection;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Project extension registered as {@code restTests} by {@link RestTestBasePlugin}.
 * <p>
 * Provides a concise DSL for configuring and registering REST integration test tasks:
 *
 * <pre>{@code
 * // Groovy — configure all rest integ test tasks in this project
 * restTests.tasks.configureEach {
 *     systemProperty 'my.prop', 'value'
 * }
 *
 * // Groovy — register a new plain Test task as a rest integ test
 * restTests.register("bcUpgradeTest") {
 *     usesBwcDistributionFromRef(...)
 * }
 * }</pre>
 *
 * <h3>Which tasks are "rest integ tests"</h3>
 * A {@link Test} task is treated as a REST integ test if it is either a
 * {@link StandaloneRestIntegTestTask} (the Gradle test-cluster framework path) or a plain
 * {@link Test} task registered through {@link #register(String, Action)} (the JUnit-rule
 * based path). The latter carry no distinguishing type, so their names are recorded in
 * {@link #restTestTaskNames}.
 * <p>
 * No task-creation ordering is required: {@link RestTestBasePlugin} registers its
 * {@link #configureEach} handler at plugin-apply time, so it always runs before any
 * build-script {@code restTests.tasks.configureEach} closure and before a task's own
 * {@code register} action (Gradle executes container {@code configureEach} actions in
 * registration order and the {@code register} action last).
 */
public class RestIntegTestsExtension {

    private final Project project;
    private final Set<String> restTestTaskNames = new HashSet<>();

    public RestIntegTestsExtension(Project project) {
        this.project = project;
    }

    /**
     * Applies {@code action} to all current and future REST integ {@link Test} tasks.
     * <p>
     * Uses a plain {@code withType(Test).configureEach} on the task container (not a
     * {@code matching()} sub-collection) so that, when registered by {@link RestTestBasePlugin}
     * at apply time, it runs before the task's own register action and before any build-script
     * {@code restTests.tasks.configureEach} closures.
     */
    public void configureEach(Action<? super Test> action) {
        project.getTasks().withType(Test.class).configureEach(task -> {
            if (isRestIntegTest(task)) {
                action.execute(task);
            }
        });
    }

    /**
     * Returns a live {@link TaskCollection} of all REST integ {@link Test} tasks.
     * <p>
     * Groovy build scripts should use this collection directly for additional
     * {@code configureEach} closures so that Gradle's native collection handling
     * sets the closure delegate to each task:
     * <pre>{@code
     * restTests.tasks.configureEach {
     *     usesDefaultDistribution("reason")
     * }
     * }</pre>
     */
    public TaskCollection<Test> getTasks() {
        return project.getTasks().withType(Test.class).matching(this::isRestIntegTest);
    }

    /**
     * Registers a new plain {@link Test} task as a REST integ test and applies the given action.
     * The task automatically receives the standard REST integ-test configuration from
     * {@link RestTestBasePlugin}.
     */
    public TaskProvider<Test> register(String name, Action<? super Test> action) {
        restTestTaskNames.add(name);
        return project.getTasks().register(name, Test.class, action);
    }

    private boolean isRestIntegTest(Test task) {
        return task instanceof StandaloneRestIntegTestTask || restTestTaskNames.contains(task.getName());
    }
}
