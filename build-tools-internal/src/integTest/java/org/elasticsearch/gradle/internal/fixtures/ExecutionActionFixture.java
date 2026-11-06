/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.fixtures;

import org.gradle.api.Action;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.invocation.Gradle;

/** Representative javac bytecode: configuration Actions are valid; direct execution captures are not. */
public class ExecutionActionFixture {
    public void unsafe(Task task, Project project, Gradle gradle) {
        task.doFirst(t -> t.getLogger().lifecycle(project.getName()));
        task.doLast("named action", t -> t.getLogger().lifecycle(gradle.getGradleVersion()));
        task.onlyIf(t -> project.hasProperty("enabled"));
        task.doLast(new Action<Task>() {
            @Override
            public void execute(Task t) {
                t.getLogger().lifecycle(project.getName());
            }
        });
    }

    public void safe(Project project, Task task) {
        project.getTasks().register("capturedValue", t -> {
            String name = project.getName();
            t.doLast(actionTask -> actionTask.getLogger().lifecycle(name));
        });
        task.doFirst(t -> t.getLogger().lifecycle("no model capture"));
        task.onlyIf(t -> true);
        task.doLast(new ValueAction(project));
        new Unrelated().doLast(t -> t.getLogger().lifecycle(project.getName()));
    }

    private static class ValueAction implements Action<Task> {
        private final String name;

        ValueAction(Project project) {
            name = project.getName();
        }

        @Override
        public void execute(Task task) {
            task.getLogger().lifecycle(name);
        }
    }

    private static class Unrelated {
        void doLast(Action<Task> action) {}
    }
}
