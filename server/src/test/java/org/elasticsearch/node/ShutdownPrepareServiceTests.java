/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShutdownPrepareServiceTests extends ESTestCase {

    public void testAwaitTasksComplete_timesOut() {
        String actionName = "test:action/name";
        TaskManager taskManager = mock();
        long taskId = randomNonNegativeLong();
        String nodeId = randomAlphaOfLength(10);
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        when(taskManager.getTasks()).thenReturn(Map.of(taskId, task));

        boolean completed = ShutdownPrepareService.awaitTasksComplete(
            timeValueMillis(ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL.millis() * 3),
            mock(),
            actionName,
            taskManager,
            null
        );
        assertThat(completed, is(false));
    }

    public void testAwaitTasksComplete_completes() {
        String actionName = "test:action/name";
        TaskManager taskManager = mock();
        long taskId = randomNonNegativeLong();
        String nodeId = randomAlphaOfLength(10);
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        when(taskManager.getTasks())
            // First call: non-empty
            .thenReturn(Map.of(taskId, task))
            // Second call: non-empty
            .thenReturn(Map.of(taskId, task))
            // Third call: empty
            .thenReturn(Map.of());

        boolean completed = ShutdownPrepareService.awaitTasksComplete(
            timeValueMillis(ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL.millis() * 3),
            mock(),
            actionName,
            taskManager,
            null
        );
        assertThat(completed, is(true));
    }

    public void testAwaitTasksComplete_invokesNotifierExactlyOnceForEachTask() {
        String actionName = "test:action/name";
        TaskManager taskManager = mock();
        String nodeId = randomAlphaOfLength(10);
        long taskId1 = randomNonNegativeLong();
        long taskId2 = randomValueOtherThan(taskId1, ESTestCase::randomNonNegativeLong);
        long taskId3 = randomValueOtherThanMany(Set.of(taskId1, taskId2)::contains, ESTestCase::randomNonNegativeLong);
        BulkByPaginatedSearchTask task1 = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId1),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        BulkByPaginatedSearchTask task2 = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId2),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        BulkByPaginatedSearchTask task3 = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId3),
            "transport",
            actionName,
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        when(taskManager.getTasks())
            // First call: tasks 1 and 2
            .thenReturn(Map.of(taskId1, task1, taskId2, task2))
            // Second call: tasks 2 and 3
            .thenReturn(Map.of(taskId2, task2, taskId3, task3))
            // Third call: task 2 only
            .thenReturn(Map.of(taskId2, task2));

        List<Task> notified = new ArrayList<>();
        ShutdownPrepareService.awaitTasksComplete(
            timeValueMillis(ShutdownPrepareService.AWAIT_TASKS_POLL_INTERVAL.millis() * 3),
            mock(),
            actionName,
            taskManager,
            notified::add
        );
        assertThat(notified, containsInAnyOrder(task1, task2, task3)); // should notify each task exactly once
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_nonRelocatableLeader() {
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            "transport",
            "test:action/name",
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        task.setWorkerCount(randomIntBetween(1, 20), Float.POSITIVE_INFINITY);
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        assertThat(task.isRelocationRequested(), is(false));
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_relocatableLeader() {
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            "transport",
            "test:action/name",
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            true,
            randomOrigin()
        );
        task.setWorkerCount(randomIntBetween(1, 20), Float.POSITIVE_INFINITY);
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_nonRelocatableWorker() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomNonNegativeLong();
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId),
            "transport",
            "test:action/name",
            "description",
            new TaskId(nodeId, randomValueOtherThan(taskId, ESTestCase::randomNonNegativeLong)),
            Map.of(),
            false,
            randomOrigin()
        );
        task.setWorker(randomFloat(), randomInt());
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        assertThat(task.isRelocationRequested(), is(false));
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_relocatableWorker() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomNonNegativeLong();
        BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(nodeId, taskId),
            "transport",
            "test:action/name",
            "description",
            new TaskId(nodeId, randomValueOtherThan(taskId, ESTestCase::randomNonNegativeLong)),
            Map.of(),
            true,
            randomOrigin()
        );
        task.setWorker(randomFloat(), randomInt());
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByPaginatedSearch_notBulkByPaginatedSearch() {
        Task task = new Task(randomInt(), "transport", "test:action/name", "description", new TaskId("localNode", randomLong()), Map.of());
        ShutdownPrepareService.maybeRequestRelocationForBulkByPaginatedSearch(task, mock());
        // No assertion, just check it doesn't blow up
    }

    private static ResumeInfo.RelocationOrigin randomOrigin() {
        return randomBoolean()
            ? null
            : new ResumeInfo.RelocationOrigin(new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()), randomNonNegativeLong());
    }
}
