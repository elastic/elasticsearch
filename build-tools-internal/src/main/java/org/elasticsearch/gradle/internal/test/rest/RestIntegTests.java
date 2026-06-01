/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.gradle.api.Action;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskCollection;
import org.gradle.api.tasks.testing.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Project extension registered as {@code restTests} by {@link RestTestBasePlugin}.
 * <p>
 * Tracks REST integration test tasks by their name and provides a concise DSL for
 * configuring and registering them:
 *
 * <pre>{@code
 * // Groovy — configure all rest integ test tasks in this project
 * restTests.tasks.configureEach {
 *     systemProperty 'my.prop', 'value'
 * }
 *
 * // Groovy — register a new plain Test task enrolled as a rest integ test
 * restTests.register("bcUpgradeTest") {
 *     usesBwcDistributionFromRef(...)
 * }
 * }</pre>
 *
 * <h3>Ordering guarantee</h3>
 * Tasks are enrolled (via {@link #enroll(String)}) <em>before</em> they are registered
 * with the task container. This means that {@link RestTestBasePlugin}'s
 * {@code configureEach} handler — which wires distributions and registers extension
 * methods such as {@code usesDefaultDistribution} — fires <em>before</em> the task's
 * own register action and therefore before any build-script {@code configureEach}
 * closures that rely on those extension methods.
 */
public class RestIntegTests {

    private final Project project;
    private final Set<String> enrolledNames = new HashSet<>();

    RestIntegTests(Project project) {
        this.project = project;
    }

    /**
     * Enrolls a task name so that subsequent {@link #configureEach} actions apply to it.
     * Must be called <em>before</em> the task is registered with the task container so
     * that the {@link RestTestBasePlugin} configuration fires before the register action.
     */
    public void enroll(String taskName) {
        enrolledNames.add(taskName);
    }

    /**
     * Returns whether the given task name has been enrolled.
     */
    boolean isEnrolled(String taskName) {
        return enrolledNames.contains(taskName);
    }

    /**
     * Applies {@code action} to all current and future enrolled {@link Test} tasks.
     * <p>
     * Internally uses a plain {@code withType(Test).configureEach} on the task container
     * (not a {@code matching()} sub-collection), which guarantees that actions registered
     * here fire <em>before</em> the task's own register action — and therefore before any
     * build-script {@code restTests.tasks.configureEach} closures.
     */
    public void configureEach(Action<? super Test> action) {
        project.getTasks().withType(Test.class).configureEach(task -> {
            if (enrolledNames.contains(task.getName())) {
                action.execute(task);
            }
        });
    }

    /**
     * Returns a live {@link TaskCollection} of all enrolled {@link Test} tasks.
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
        return project.getTasks().withType(Test.class).matching(task -> enrolledNames.contains(task.getName()));
    }

    /**
     * Registers a new plain {@link Test} task, enrolls it, and applies the given action.
     * The task automatically receives the standard REST integ-test configuration from
     * {@link RestTestBasePlugin} because enrollment happens before task registration.
     */
    public void register(String name, Action<? super Test> action) {
        enroll(name);
        project.getTasks().register(name, Test.class, action);
    }
}
