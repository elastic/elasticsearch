/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ClusterStateTaskExecutorTests extends ESTestCase {

    private class TestTask implements ClusterStateTaskListener {
        private final String description;

        TestTask(String description) {
            this.description = description;
        }

        @Override
        public void onFailure(Exception e) {
            throw new AssertionError("Should not fail in test", e);
        }

        @Override
        public String toString() {
            return description == null ? "" : "Task{" + description + "}";
        }
    }

    public void testDescribeTasks() {
        final ClusterStateTaskExecutor<TestTask> executor = batchExecutionContext -> { throw new AssertionError("should not be called"); };

        assertThat("describes an empty list", executor.describeTasks(List.of()), equalTo(""));
        assertThat("describes a singleton list", executor.describeTasks(List.of(new TestTask("a task"))), equalTo("Task{a task}"));
        assertThat(
            "describes a list of two tasks",
            executor.describeTasks(List.of(new TestTask("a task"), new TestTask("another task"))),
            equalTo("Task{a task}, Task{another task}")
        );

        assertThat("skips the only item if it has no description", executor.describeTasks(List.of(new TestTask(null))), equalTo(""));
        assertThat(
            "skips an item if it has no description",
            executor.describeTasks(List.of(new TestTask("a task"), new TestTask(null), new TestTask("another task"), new TestTask(null))),
            equalTo("Task{a task}, Task{another task}")
        );
    }
}
