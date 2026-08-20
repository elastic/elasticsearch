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
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Project extension registered as {@code restTests} by {@link RestTestBasePlugin}.
 * <p>
 * Provides a concise DSL for configuring and registering REST integration test tasks:
 *
 * <pre>{@code
 * // Groovy — configure all rest integ test tasks in this project
 * restTests.configureEach {
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
 * build-script {@code restTests.configureEach} closure and before a task's own
 * {@code register} action (Gradle executes container {@code configureEach} actions in
 * registration order and the {@code register} action last).
 */
public class RestIntegTestsExtension {

    private final Project project;
    private final Set<String> restTestTaskNames = new HashSet<>();
    private final List<TaskProvider<Test>> restTestTaskProviders = new ArrayList<>();

    public RestIntegTestsExtension(Project project) {
        this.project = project;
    }

    /**
     * Applies {@code action} to all current and future REST integ {@link Test} tasks.
     * <p>
     * Uses a plain {@code withType(Test).configureEach} on the task container (not a
     * {@code matching()} sub-collection, which would eagerly realize tasks) so that, when
     * registered by {@link RestTestBasePlugin} at apply time, it runs before the task's own
     * register action and before any build-script {@code restTests.configureEach} closures.
     */
    public void configureEach(Action<? super Test> action) {
        project.getTasks().withType(Test.class).configureEach(task -> {
            if (isRestIntegTest(task)) {
                action.execute(task);
            }
        });
    }

    /**
     * Returns the {@link TaskProvider}s of all REST integ {@link Test} tasks registered via
     * {@link #register(String, Action)}, without realizing any task.
     * <p>
     * Suitable for wiring task dependencies lazily:
     * <pre>{@code
     * tasks.named("check").configure {
     *     dependsOn(restTests.tasks)
     * }
     * }</pre>
     * To configure the tasks, use {@link #configureEach(Action)} instead, which also covers
     * {@link StandaloneRestIntegTestTask}-based tasks not registered through this extension.
     */
    public List<TaskProvider<Test>> getTasks() {
        return Collections.unmodifiableList(restTestTaskProviders);
    }

    /**
     * Registers a new plain {@link Test} task as a REST integ test and applies the given action.
     * The task automatically receives the standard REST integ-test configuration from
     * {@link RestTestBasePlugin}.
     */
    public TaskProvider<Test> register(String name, Action<? super Test> action) {
        restTestTaskNames.add(name);
        TaskProvider<Test> provider = project.getTasks().register(name, Test.class, action);
        restTestTaskProviders.add(provider);
        return provider;
    }

    private boolean isRestIntegTest(Test task) {
        // TODO: remove StandaloneRestIntegTestTask instanceof check once all projects use JUnit-rule-based clusters
        return task instanceof StandaloneRestIntegTestTask || restTestTaskNames.contains(task.getName());
    }
}
