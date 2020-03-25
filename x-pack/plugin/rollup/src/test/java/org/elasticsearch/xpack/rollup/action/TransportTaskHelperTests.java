/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomRollupJobConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportTaskHelperTests extends ESTestCase {

    public void testProcessRequestOneMatching() {
        RollupJobConfig job = randomRollupJobConfig(random(), "foo");
        TaskManager taskManager = mock(TaskManager.class);
        RollupJobTask task = mock(RollupJobTask.class);
        when(task.getDescription()).thenReturn("rollup_foo");
        when(task.getConfig()).thenReturn(job);

        Map<Long, Task> tasks = getRandomTasks();
        tasks.put(1L, task);
        when(taskManager.getTasks()).thenReturn(tasks);

        Consumer<RollupJobTask> consumer = rollupJobTask -> {
            assertThat(rollupJobTask.getDescription(), equalTo("rollup_foo"));
            assertThat(rollupJobTask.getConfig().getId(), equalTo(job.getId()));
        };
        TransportTaskHelper.doProcessTasks("foo", consumer, taskManager);
    }

    public void testProcessRequestNoMatching() {
        TaskManager taskManager = mock(TaskManager.class);
        Map<Long, Task> tasks = getRandomTasks();
        when(taskManager.getTasks()).thenReturn(tasks);

        Consumer<RollupJobTask> consumer = rollupJobTask -> {
            fail("Should not have reached consumer");
        };
        TransportTaskHelper.doProcessTasks("foo", consumer, taskManager);
    }

    public void testProcessRequestMultipleMatching() {
        TaskManager taskManager = mock(TaskManager.class);
        Map<Long, Task> tasks = getRandomTasks();
        when(taskManager.getTasks()).thenReturn(tasks);

        RollupJobConfig job = randomRollupJobConfig(random(), "foo");
        RollupJobTask task = mock(RollupJobTask.class);
        when(task.getDescription()).thenReturn("rollup_foo");
        when(task.getConfig()).thenReturn(job);
        tasks.put(1L, task);

        RollupJobConfig job2 = randomRollupJobConfig(random(), "foo");
        RollupJobTask task2 = mock(RollupJobTask.class);
        when(task2.getDescription()).thenReturn("rollup_foo");
        when(task2.getConfig()).thenReturn(job2);
        tasks.put(2L, task2);

        Consumer<RollupJobTask> consumer = rollupJobTask -> {
            fail("Should not have reached consumer");
        };
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> TransportTaskHelper.doProcessTasks("foo", consumer, taskManager));
        assertThat(e.getMessage(), equalTo("Found more than one matching task for rollup job [foo] when " +
                "there should only be one."));
    }

    private Map<Long, Task> getRandomTasks() {
        int num = randomIntBetween(1,10);
        Map<Long, Task> tasks = new HashMap<>(num);
        for (int i = 0; i < num; i++) {
            Long taskId = randomLongBetween(10, Long.MAX_VALUE);
            tasks.put(taskId, new TestTask(taskId, randomAlphaOfLength(10), "test_action", "test_description",
                    new TaskId("node:123"), Collections.emptyMap()));
        }
        return tasks;
    }

    private static class TestTask extends Task {
        TestTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
            super(id, type, action, description, parentTask, headers);
        }
    }
}
