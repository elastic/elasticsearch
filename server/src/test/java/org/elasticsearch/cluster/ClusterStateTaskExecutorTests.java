/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class ClusterStateTaskExecutorTests extends ESTestCase {

    private class TestTask {
        private final String description;

        TestTask(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description == null ? "" : "Task{" + description + "}";
        }
    }

    public void testDescribeTasks() {
        final ClusterStateTaskExecutor<TestTask> executor = (currentState, tasks) -> {
            throw new AssertionError("should not be called");
        };

        assertThat("describes an empty list", executor.describeTasks(Collections.emptyList()), equalTo(""));
        assertThat("describes a singleton list", executor.describeTasks(Collections.singletonList(new TestTask("a task"))),
            equalTo("Task{a task}"));
        assertThat("describes a list of two tasks",
            executor.describeTasks(Arrays.asList(new TestTask("a task"), new TestTask("another task"))),
            equalTo("Task{a task}, Task{another task}"));

        assertThat("skips the only item if it has no description", executor.describeTasks(Collections.singletonList(new TestTask(null))),
            equalTo(""));
        assertThat("skips an item if it has no description",
            executor.describeTasks(Arrays.asList(
                new TestTask("a task"), new TestTask(null), new TestTask("another task"), new TestTask(null))),
            equalTo("Task{a task}, Task{another task}"));
    }
}
