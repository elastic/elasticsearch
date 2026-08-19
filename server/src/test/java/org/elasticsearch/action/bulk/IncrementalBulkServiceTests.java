/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link IncrementalBulkService#REQUEST_TIMEOUT} scheduling that this service adds on top of
 * the bulk-session cancellation machinery. The downstream effect of cancellation (the next chunk failing
 * with a {@code TaskCancelledException}) is covered by {@code IncrementalBulkIT}; here we only assert that
 * the timeout reliably triggers cancellation and is cancelled itself once the request finishes.
 */
public class IncrementalBulkServiceTests extends ESTestCase {

    /**
     * Builds a service whose requests run on {@code threadPool}'s simulated clock. A null
     * {@code requestTimeout} configures the {@link IncrementalBulkService#REQUEST_TIMEOUT} setting to its
     * disabled default.
     *
     * <p>{@code taskManager} is a mock rather than a real {@link TaskManager} because the timeout's effect
     * is delivered through {@code cancelTaskAndDescendants}, which a bare {@code TaskManager} cannot service
     * without a transport-wired {@code TaskCancellationService}. Mocking it lets us verify the cancellation
     * call directly; {@code register()} is stubbed to return a real {@link CancellableTask} so the handler
     * can be constructed and parented. {@code Client} is mocked because these tests only exercise timeout
     * scheduling and never issue a bulk request.
     */
    private static IncrementalBulkService newService(ThreadPool threadPool, TimeValue requestTimeout, TaskManager taskManager) {
        CancellableTask task = new CancellableTask(
            1L,
            IncrementalBulkService.BULK_SESSION_TASK_TYPE,
            IncrementalBulkService.BULK_SESSION_ACTION,
            "",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        when(taskManager.register(anyString(), anyString(), any())).thenReturn(task);
        when(taskManager.getNodeId()).thenReturn("test-node");

        Settings.Builder settings = Settings.builder();
        if (requestTimeout != null) {
            settings.put(IncrementalBulkService.REQUEST_TIMEOUT.getKey(), requestTimeout);
        }
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings.build());

        return new IncrementalBulkService(
            mock(Client.class),
            new IndexingPressure(Settings.EMPTY),
            MeterRegistry.NOOP,
            taskManager,
            threadPool,
            clusterSettings
        );
    }

    /**
     * The timeout fires on a context that {@link ThreadPool#schedule} preserved from the REST caller, but the
     * {@code internal:admin/tasks/ban} request that cancellation fans out to can never be granted to a user. So
     * {@code cancel} must stash and mark the context as the system context before calling into the
     * {@link TaskManager}, and must restore the caller's context afterwards.
     */
    public void testCancellationRunsInSystemContext() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = taskQueue.getThreadPool();
        ThreadContext threadContext = threadPool.getThreadContext();
        TaskManager taskManager = mock(TaskManager.class);

        AtomicBoolean systemContextWhileCancelling = new AtomicBoolean();
        doAnswer(invocation -> {
            systemContextWhileCancelling.set(threadContext.isSystemContext());
            return null;
        }).when(taskManager).cancelTaskAndDescendants(any(), startsWith("request timed out"), eq(false), any());

        IncrementalBulkService service = newService(threadPool, TimeValue.timeValueSeconds(30), taskManager);
        try (var handler = service.newBulkRequest()) {
            assertFalse("the request itself must not run as the system user", threadContext.isSystemContext());

            taskQueue.runAllTasksInTimeOrder();

            assertTrue("cancellation must run as the system user", systemContextWhileCancelling.get());
            assertFalse("the system context must not leak past cancellation", threadContext.isSystemContext());
        }
    }

    public void testTimeoutTriggersCancellation() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        TaskManager taskManager = mock(TaskManager.class);
        IncrementalBulkService service = newService(taskQueue.getThreadPool(), TimeValue.timeValueSeconds(30), taskManager);

        try (var handler = service.newBulkRequest()) {
            assertTrue("a timeout task should be scheduled", taskQueue.hasDeferredTasks());

            taskQueue.runAllTasksInTimeOrder();

            verify(taskManager).cancelTaskAndDescendants(any(), startsWith("request timed out"), eq(false), any());
        }
    }

    public void testNoTimeoutWhenDisabled() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

        // MINUS_ONE disables the timeout; a sub-millisecond value rounds down to a 0ms delay and must also be
        // treated as disabled rather than scheduling a task that cancels the request immediately.
        for (TimeValue timeout : new TimeValue[] { null, TimeValue.MINUS_ONE, TimeValue.timeValueNanos(500_000) }) {
            IncrementalBulkService service = newService(taskQueue.getThreadPool(), timeout, mock(TaskManager.class));
            try (var handler = service.newBulkRequest()) {
                assertFalse("no timeout task should be scheduled when disabled", taskQueue.hasDeferredTasks());
            }
        }
    }

    public void testCloseCancelsPendingTimeout() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        TaskManager taskManager = mock(TaskManager.class);
        IncrementalBulkService service = newService(taskQueue.getThreadPool(), TimeValue.timeValueSeconds(30), taskManager);

        var handler = service.newBulkRequest();
        assertTrue(taskQueue.hasDeferredTasks());
        handler.close();

        // close() cancelled the scheduled task, so advancing past the deadline must not fire the timeout
        taskQueue.runAllTasksInTimeOrder();
        verify(taskManager, never()).cancelTaskAndDescendants(any(), startsWith("request timed out"), anyBoolean(), any());
    }
}
