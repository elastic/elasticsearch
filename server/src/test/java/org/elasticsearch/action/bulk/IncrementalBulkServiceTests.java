/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
 * the bulk-session cancellation machinery, and the registration lifecycle tied to it: the session task
 * exists only while the timeout is active, and every path that ends a session reclaims it. The downstream
 * effect of cancellation (the next chunk failing with a {@code TaskCancelledException}) is covered by
 * {@code IncrementalBulkIT}.
 */
public class IncrementalBulkServiceTests extends ESTestCase {

    /**
     * Builds a service whose requests run on {@code threadPool}'s simulated clock, backed by a mock
     * {@link TaskManager}. A null {@code requestTimeout} leaves
     * {@link IncrementalBulkService#REQUEST_TIMEOUT} at its disabled default, in which case no session task
     * is registered at all.
     *
     * <p>The mock is for tests that only need to verify which calls were made: the timeout's effect is
     * delivered through {@code cancelTaskAndDescendants}, which a bare {@code TaskManager} cannot service
     * without a transport-wired {@code TaskCancellationService}. {@code register()} is stubbed to return a
     * real {@link CancellableTask} so the handler can be constructed and parented. Tests that need to observe
     * genuine registration state use {@link #buildService} with a real {@code TaskManager} instead.
     * {@code Client} is mocked because these tests never issue a bulk request.
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
        return buildService(threadPool, requestTimeout, taskManager);
    }

    /**
     * Builds a service against whichever {@link TaskManager} is supplied, without stubbing it. Tests that need
     * to observe real registration state pass a real {@link TaskManager}; {@link #newService} layers Mockito
     * stubbing on top of this for the tests that only verify which calls were made.
     */
    private static IncrementalBulkService buildService(ThreadPool threadPool, TimeValue requestTimeout, TaskManager taskManager) {
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
            assertNotNull("an active timeout must register a session task to cancel", handler.getBulkSessionTask());

            taskQueue.runAllTasksInTimeOrder();

            verify(taskManager).cancelTaskAndDescendants(any(), startsWith("request timed out"), eq(false), any());
        }
    }

    /**
     * With no active timeout there is nothing that would ever reclaim a session task: the handler may be
     * abandoned without {@code close()} or {@code lastItems} running, and no scheduled cancellation exists to
     * bound it. So no task is registered at all, which is what keeps an abandoned session from leaking a
     * permanent {@link TaskManager} entry.
     */
    public void testNoTimeoutAndNoSessionTaskWhenDisabled() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

        // MINUS_ONE disables the timeout; a sub-millisecond value rounds down to a 0ms delay and must also be
        // treated as disabled rather than scheduling a task that cancels the request immediately.
        for (TimeValue timeout : new TimeValue[] { null, TimeValue.MINUS_ONE, TimeValue.timeValueNanos(500_000) }) {
            TaskManager taskManager = mock(TaskManager.class);
            IncrementalBulkService service = newService(taskQueue.getThreadPool(), timeout, taskManager);
            try (var handler = service.newBulkRequest()) {
                assertFalse("no timeout task should be scheduled when disabled", taskQueue.hasDeferredTasks());
                assertNull("no session task should be registered when disabled", handler.getBulkSessionTask());
            }
            // The handler was closed by the try-with-resources: with nothing registered there is nothing to
            // cancel or reclaim, so close() must not reach the TaskManager at all.
            verify(taskManager, never()).register(anyString(), anyString(), any());
            verify(taskManager, never()).cancelTaskAndDescendants(any(), anyString(), anyBoolean(), any());
            verify(taskManager, never()).unregister(any());
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

    /**
     * Stub for a cancellation that never completes: {@code cancelTaskAndDescendants} only fires its listener
     * once every child connection has answered its ban request, and those sends carry no timeout, so an
     * unresponsive node stalls it indefinitely. A bare {@link TaskManager} cannot reproduce that without a
     * transport-wired {@code TaskCancellationService}, so the override drops the listener. Registration is
     * left real, so the tests read genuine registry state rather than a mock's.
     */
    private static class StalledCancellationTaskManager extends TaskManager {

        private final AtomicInteger cancellations = new AtomicInteger();
        private final AtomicInteger unregistrations = new AtomicInteger();
        private final AtomicBoolean taskRegisteredWhenCancelled = new AtomicBoolean();

        private final boolean throwOnCancel;

        StalledCancellationTaskManager(ThreadPool threadPool) {
            this(threadPool, false);
        }

        StalledCancellationTaskManager(ThreadPool threadPool, boolean throwOnCancel) {
            super(Settings.EMPTY, threadPool, Set.of());
            this.throwOnCancel = throwOnCancel;
        }

        @Override
        public void cancelTaskAndDescendants(
            CancellableTask task,
            String reason,
            boolean waitForCompletion,
            ActionListener<Void> listener
        ) {
            taskRegisteredWhenCancelled.set(getCancellableTask(task.getId()) != null);
            cancellations.incrementAndGet();
            if (throwOnCancel) {
                // The message the real TaskManager uses when its cancellation service was never set.
                throw new IllegalStateException("TaskCancellationService is not initialized");
            }
        }

        @Override
        public Task unregister(Task task) {
            unregistrations.incrementAndGet();
            return super.unregister(task);
        }

        void assertCancelledThenUnregistered(String what) {
            assertEquals(what + " must propagate the cancellation", 1, cancellations.get());
            assertTrue(
                "cancellation must run before the task is unregistered, otherwise it is silently a no-op",
                taskRegisteredWhenCancelled.get()
            );
            assertTrue(what + " must unregister without waiting for the cancellation to complete", getCancellableTasks().isEmpty());
            assertEquals(what + " must unregister exactly once", 1, unregistrations.get());
        }
    }

    /**
     * The timeout is the terminal bound on a session's lifetime, so it must reclaim the registration itself:
     * an abandoned handler receives no further chunk, and neither {@code close()} nor {@code lastItems} will
     * ever run. Waiting for the cancellation to complete would defeat that, since a stalled ban round-trip
     * never returns.
     */
    public void testTimeoutUnregistersTaskWhenCancellationNeverCompletes() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = taskQueue.getThreadPool();
        StalledCancellationTaskManager taskManager = new StalledCancellationTaskManager(threadPool);
        IncrementalBulkService service = buildService(threadPool, TimeValue.timeValueSeconds(30), taskManager);

        try (var handler = service.newBulkRequest()) {
            assertEquals("the bulk session task should be registered", 1, taskManager.getCancellableTasks().size());

            taskQueue.runAllTasksInTimeOrder();

            taskManager.assertCancelledThenUnregistered("the timeout");
        }
    }

    /**
     * The timeout and the normal teardown path can both run for the same session, but only one may reclaim
     * it, since {@link TaskManager#unregister} is not idempotent.
     */
    public void testSessionTaskReclaimedOnceWhenTimeoutPrecedesClose() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = taskQueue.getThreadPool();
        StalledCancellationTaskManager taskManager = new StalledCancellationTaskManager(threadPool);
        IncrementalBulkService service = buildService(threadPool, TimeValue.timeValueSeconds(30), taskManager);

        var handler = service.newBulkRequest();
        taskQueue.runAllTasksInTimeOrder();
        assertTrue("the timeout should have reclaimed the session", taskManager.getCancellableTasks().isEmpty());

        handler.close();

        assertEquals("close() must not unregister a session the timeout already reclaimed", 1, taskManager.unregistrations.get());
        assertEquals("close() must not re-cancel a session the timeout already reclaimed", 1, taskManager.cancellations.get());
    }

    /**
     * {@code cancelTaskAndDescendants} can throw synchronously -- {@link IllegalStateException} when the
     * cancellation service was never set, or a listener failure rethrown by {@code notifyListeners}. Since
     * reclaiming the registration is the whole point of these paths, a failed cancellation must not strand it.
     */
    public void testSessionTaskUnregisteredWhenCancellationThrows() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = taskQueue.getThreadPool();
        StalledCancellationTaskManager taskManager = new StalledCancellationTaskManager(threadPool, true);
        IncrementalBulkService service = buildService(threadPool, TimeValue.timeValueSeconds(30), taskManager);

        var handler = service.newBulkRequest();
        expectThrows(IllegalStateException.class, handler::close);

        assertTrue("a failed cancellation must not strand the registration", taskManager.getCancellableTasks().isEmpty());
    }

    /**
     * Same contract for the normal teardown path: a stalled cancellation must not hold the registration open.
     */
    public void testCloseUnregistersTaskWhenCancellationNeverCompletes() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = taskQueue.getThreadPool();
        StalledCancellationTaskManager taskManager = new StalledCancellationTaskManager(threadPool);
        IncrementalBulkService service = buildService(threadPool, TimeValue.timeValueSeconds(30), taskManager);

        var handler = service.newBulkRequest();
        assertEquals("the bulk session task should be registered", 1, taskManager.getCancellableTasks().size());

        handler.close();

        taskManager.assertCancelledThenUnregistered("close()");
    }
}
