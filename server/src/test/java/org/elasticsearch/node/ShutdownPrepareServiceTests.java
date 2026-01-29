/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestRequest;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class ShutdownPrepareServiceTests extends ESTestCase {

    public void testMarkBulkByScrollTasksAsRequiringRelocation() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
            BulkByScrollTask nonRelocatableLeader = registerLeaderBulkByScrollTask(false, taskManager);
            BulkByScrollTask nonRelocatableWorker = registerWorkerBulkByScrollTask(nonRelocatableLeader, false, taskManager);
            BulkByScrollTask relocatableLeader = registerLeaderBulkByScrollTask(true, taskManager);
            BulkByScrollTask relocatableWorker = registerWorkerBulkByScrollTask(nonRelocatableLeader, true, taskManager);
            Task vanillaTask = registerVanillaTask(taskManager);

            ShutdownPrepareService.markBulkByScrollTasksAsRequiringRelocation(taskManager);

            assertThat(
                taskManager.getTasks().values(),
                containsInAnyOrder(nonRelocatableLeader, nonRelocatableWorker, relocatableLeader, relocatableWorker, vanillaTask)
            );
            assertThat(nonRelocatableLeader.isRelocationRequested(), is(false));
            assertThat(nonRelocatableWorker.isRelocationRequested(), is(false));
            assertThat(relocatableLeader.isRelocationRequested(), is(true));
            assertThat(relocatableWorker.isRelocationRequested(), is(true));
        } finally {
            threadPool.shutdown();
        }
    }

    private static BulkByScrollTask registerLeaderBulkByScrollTask(boolean eligibleForRelocationOnShutdown, TaskManager taskManager) {
        TaskAwareRequest request = new FakeBulkByScrollRequest(eligibleForRelocationOnShutdown);
        BulkByScrollTask task = (BulkByScrollTask) taskManager.register("transport", "test:action/name", request);
        task.setWorkerCount(randomIntBetween(1, 20));
        return task;
    }

    private static BulkByScrollTask registerWorkerBulkByScrollTask(
        BulkByScrollTask parentTask,
        boolean eligibleForRelocationOnShutdown,
        TaskManager taskManager
    ) {
        TaskAwareRequest request = new FakeBulkByScrollRequest(eligibleForRelocationOnShutdown);
        request.setParentTask(new TaskId("localNode", parentTask.getId()));
        BulkByScrollTask task = (BulkByScrollTask) taskManager.register("transport", "test:action/name", request);
        task.setWorker(randomFloat(), randomIntBetween(0, 9));
        return task;
    }

    private Task registerVanillaTask(TaskManager taskManager) {
        TaskAwareRequest request = new TestRequest(randomAlphaOfLength(10));
        return taskManager.register("transport", "test:action/name", request);
    }

    private static class FakeBulkByScrollRequest extends TestRequest {

        private final boolean eligibleForRelocationOnShutdown;

        FakeBulkByScrollRequest(boolean eligibleForRelocationOnShutdown) {
            super(ESTestCase.randomAlphaOfLength(10));
            this.eligibleForRelocationOnShutdown = eligibleForRelocationOnShutdown;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new BulkByScrollTask(id, type, action, "description", parentTaskId, headers, eligibleForRelocationOnShutdown);
        }
    }
}
