/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.ci;

import java.util.List;

/**
 * Written to {@code .task-status.json} at the end of every build by {@link TaskStatusTrackerPlugin}.
 *
 * @param tasks        every task in the execution graph, sorted by path, with its final outcome
 * @param tests        every individual test method that completed before the build ended,
 *                     sorted by task path then class then method name
 * @param cancelled    {@code true} when the build was explicitly cancelled (Ctrl+C or preemption signal)
 * @param preemptedAt  ISO-8601 timestamp of when GCP preemption was detected, or {@code null} if not preempted
 */
public record TaskStatusReport(List<TaskEntry> tasks, List<TestEntry> tests, boolean cancelled, String preemptedAt) {

    /**
     * @param path    Gradle task path, e.g. {@code :server:test}
     * @param outcome the task's final execution outcome
     */
    public record TaskEntry(String path, String outcome) {}

    /**
     * @param taskPath   Gradle task path that ran this test, e.g. {@code :server:test}
     * @param className  fully-qualified test class, e.g. {@code org.elasticsearch.FooTests}
     * @param methodName test method name, including parameter description for parameterized tests
     * @param result     {@code SUCCESS}, {@code FAILURE}, or {@code SKIPPED}
     */
    public record TestEntry(String taskPath, String className, String methodName, String result) {}
}
