/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomRollupJobConfig;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
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

        final var rollupJobTasks = TransportTaskHelper.doProcessTasks("foo", taskManager);
        assertThat(rollupJobTasks, hasSize(1));
        for (final var rollupJobTask : rollupJobTasks) {
            assertThat(rollupJobTask.getDescription(), equalTo("rollup_foo"));
            assertThat(rollupJobTask.getConfig().getId(), equalTo(job.getId()));
        }
    }

    public void testProcessRequestNoMatching() {
        TaskManager taskManager = mock(TaskManager.class);
        Map<Long, Task> tasks = getRandomTasks();
        when(taskManager.getTasks()).thenReturn(tasks);

        assertThat(TransportTaskHelper.doProcessTasks("foo", taskManager), empty());
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

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportTaskHelper.doProcessTasks("foo", taskManager)
        );
        assertThat(e.getMessage(), equalTo("Found more than one matching task for rollup job [foo] when there should only be one."));
    }

    private Map<Long, Task> getRandomTasks() {
        int num = randomIntBetween(1, 10);
        Map<Long, Task> tasks = Maps.newMapWithExpectedSize(num);
        for (int i = 0; i < num; i++) {
            Long taskId = randomLongBetween(10, Long.MAX_VALUE);
            tasks.put(
                taskId,
                new TestTask(
                    taskId,
                    randomAlphaOfLength(10),
                    "test_action",
                    "test_description",
                    new TaskId("node:123"),
                    Collections.emptyMap()
                )
            );
        }
        return tasks;
    }

    private static class TestTask extends Task {
        TestTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
            super(id, type, action, description, parentTask, headers);
        }
    }
}
