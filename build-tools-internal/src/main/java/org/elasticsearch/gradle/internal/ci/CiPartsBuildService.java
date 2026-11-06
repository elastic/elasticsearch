/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.Task;
import org.gradle.api.provider.Property;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Parses the checked-in CI part assignment once per build instead of once per project, and lets a part's tasks in
 * one project find the part's long-running test tasks in other projects while the task graph is built.
 */
public abstract class CiPartsBuildService implements BuildService<CiPartsBuildService.Params> {

    public interface Params extends BuildServiceParameters {
        /** Path of the assignment file relative to the root of the build, for error messages. */
        Property<String> getAssignmentFile();

        /** Contents of the assignment file; absent when the file does not exist. */
        Property<String> getAssignmentJson();
    }

    private CiPartAssignment assignment;
    private final Map<String, TaskContainer> tasksByProject = new ConcurrentHashMap<>();

    public synchronized CiPartAssignment assignment() {
        if (assignment == null) {
            if (getParameters().getAssignmentJson().isPresent() == false) {
                throw new IllegalStateException("CI part assignment " + getParameters().getAssignmentFile().get() + " is missing");
            }
            assignment = CiPartAssignment.parse(getParameters().getAssignmentJson().get());
        }
        return assignment;
    }

    /** Makes the tasks of {@code projectPath} findable by {@link #findTask}. */
    public void registerTasks(String projectPath, TaskContainer tasks) {
        tasksByProject.put(projectPath, tasks);
    }

    /**
     * The task at {@code taskPath}, if its project registered its tasks and has a task of that name. Only call this
     * once every project is configured, i.e. while the task graph is built.
     */
    public Optional<TaskProvider<Task>> findTask(String taskPath) {
        int colon = taskPath.lastIndexOf(':');
        TaskContainer tasks = tasksByProject.get(colon <= 0 ? ":" : taskPath.substring(0, colon));
        String name = taskPath.substring(colon + 1);
        if (tasks == null || tasks.getNames().contains(name) == false) {
            return Optional.empty();
        }
        return Optional.of(tasks.named(name));
    }
}
