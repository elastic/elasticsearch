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
 * @param tasks     every task in the execution graph, sorted by path, with its final outcome
 * @param cancelled {@code true} when the build was explicitly cancelled (Ctrl+C or preemption signal)
 */
public record TaskStatusReport(List<TaskEntry> tasks, boolean cancelled) {

    /**
     * @param path    Gradle task path, e.g. {@code :server:test}
     * @param outcome the task's final execution outcome
     */
    public record TaskEntry(String path, String outcome) {}
}
