/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.TransportTasksActionTests;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.tracing.AttributeKeys;
import org.elasticsearch.telemetry.tracing.Span;
import org.elasticsearch.telemetry.tracing.SpanBuilder;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.FakeTcpChannel;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransportChannel;
import org.elasticsearch.transport.TestTransportChannels;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskManagerTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(TransportTasksActionTests.class.getSimpleName());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    /**
     * Makes sure that tasks that attempt to store themselves on completion retry if
     * they don't succeed at first.
     */
    public void testResultsServiceRetryTotalTime() {
        Iterator<TimeValue> times = TaskResultsService.STORE_BACKOFF_POLICY.iterator();
        long total = 0;
        while (times.hasNext()) {
            total += times.next().millis();
        }
        assertEquals(600000L, total);
    }

    public void testTrackingChannelTask() throws Exception {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.noop());
        Set<Task> cancelledTasks = ConcurrentCollections.newConcurrentSet();
        final var transportServiceMock = mock(TransportService.class);
        when(transportServiceMock.getThreadPool()).thenReturn(threadPool);
        taskManager.setTaskCancellationService(new TaskCancellationService(transportServiceMock) {
            @Override
            void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
                assertThat(reason, equalTo("channel was closed"));
                assertFalse(waitForCompletion);
                assertTrue("task [" + task + "] was cancelled already", cancelledTasks.add(task));
            }
        });
        Map<TcpChannel, Set<Task>> pendingTasks = new HashMap<>();
        Set<Task> expectedCancelledTasks = new HashSet<>();
        FakeTcpChannel[] channels = new FakeTcpChannel[randomIntBetween(1, 10)];
        List<Releasable> stopTrackingTasks = new ArrayList<>();
        for (int i = 0; i < channels.length; i++) {
            channels[i] = new SingleThreadedTcpChannel();
        }
        int iterations = randomIntBetween(1, 200);
        for (int i = 0; i < iterations; i++) {
            final List<Releasable> subset = randomSubsetOf(stopTrackingTasks);
            stopTrackingTasks.removeAll(subset);
            Releasables.close(subset);
            final FakeTcpChannel channel = randomFrom(channels);
            final Task task = taskManager.register("transport", "test", new CancellableRequest(Integer.toString(i)));
            if (channel.isOpen() && randomBoolean()) {
                channel.close();
                expectedCancelledTasks.addAll(pendingTasks.getOrDefault(channel, Collections.emptySet()));
            }
            final Releasable stopTracking = taskManager.startTrackingCancellableChannelTask(channel, (CancellableTask) task);
            if (channel.isOpen()) {
                pendingTasks.computeIfAbsent(channel, k -> new HashSet<>()).add(task);
                stopTrackingTasks.add(() -> {
                    stopTracking.close();
                    assertTrue(pendingTasks.get(channel).remove(task));
                    expectedCancelledTasks.remove(task);
                });
            } else {
                expectedCancelledTasks.add(task);
            }
        }
        assertBusy(() -> assertThat(expectedCancelledTasks, everyItem(in(cancelledTasks))), 30, TimeUnit.SECONDS);
        for (FakeTcpChannel channel : channels) {
            channel.close();
        }
        assertThat(taskManager.numberOfChannelPendingTaskTrackers(), equalTo(0));
    }

    public void testTrackingTaskAndCloseChannelConcurrently() throws Exception {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.noop());
        Set<CancellableTask> cancelledTasks = ConcurrentCollections.newConcurrentSet();
        final var transportServiceMock = mock(TransportService.class);
        when(transportServiceMock.getThreadPool()).thenReturn(threadPool);
        taskManager.setTaskCancellationService(new TaskCancellationService(transportServiceMock) {
            @Override
            void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
                assertTrue("task [" + task + "] was cancelled already", cancelledTasks.add(task));
            }
        });
        Set<Task> expectedCancelledTasks = ConcurrentCollections.newConcurrentSet();
        FakeTcpChannel[] channels = new FakeTcpChannel[randomIntBetween(1, 10)];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = new FakeTcpChannel();
        }
        Thread[] threads = new Thread[randomIntBetween(2, 8)];
        Phaser phaser = new Phaser(threads.length);
        for (int t = 0; t < threads.length; t++) {
            String threadName = "thread-" + t;
            threads[t] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                int iterations = randomIntBetween(50, 500);
                for (int i = 0; i < iterations; i++) {
                    final FakeTcpChannel channel = randomFrom(channels);
                    if (randomBoolean()) {
                        final Task task = taskManager.register("transport", "test", new CancellableRequest(threadName + ":" + i));
                        expectedCancelledTasks.add(task);
                        taskManager.startTrackingCancellableChannelTask(channel, (CancellableTask) task);
                        if (randomInt(100) < 5) {
                            randomFrom(channels).close();
                        }
                    } else {
                        final TaskId taskId = new TaskId("node", between(1, 100));
                        final TcpTransportChannel tcpTransportChannel = TestTransportChannels.newFakeTcpTransportChannel(
                            "node-" + i,
                            channel,
                            threadPool,
                            "action-" + i,
                            randomIntBetween(0, 1000),
                            TransportVersion.current()
                        );
                        taskManager.setBan(taskId, "test", tcpTransportChannel);
                    }
                }
            });
            threads[t].start();
        }
        for (FakeTcpChannel channel : channels) {
            channel.close();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertBusy(() -> assertThat(cancelledTasks, equalTo(expectedCancelledTasks)), 1, TimeUnit.MINUTES);
        assertBusy(() -> assertThat(taskManager.getBannedTaskIds(), empty()), 1, TimeUnit.MINUTES);
        assertThat(taskManager.numberOfChannelPendingTaskTrackers(), equalTo(0));
    }

    public void testRemoveBansOnChannelDisconnects() throws Exception {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.noop());
        final var transportServiceMock = mock(TransportService.class);
        when(transportServiceMock.getThreadPool()).thenReturn(threadPool);
        taskManager.setTaskCancellationService(new TaskCancellationService(transportServiceMock) {
            @Override
            void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {}
        });
        Map<TaskId, Set<TcpChannel>> installedBans = new HashMap<>();
        FakeTcpChannel[] channels = new FakeTcpChannel[randomIntBetween(1, 10)];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = new SingleThreadedTcpChannel();
        }
        int iterations = randomIntBetween(1, 200);
        for (int i = 0; i < iterations; i++) {
            final FakeTcpChannel channel = randomFrom(channels);
            if (channel.isOpen() && randomBoolean()) {
                channel.close();
            }
            TaskId taskId = new TaskId("node-" + randomIntBetween(1, 3), randomIntBetween(1, 100));
            installedBans.computeIfAbsent(taskId, t -> new HashSet<>()).add(channel);
            taskManager.setBan(
                taskId,
                "test",
                TestTransportChannels.newFakeTcpTransportChannel(
                    "node",
                    channel,
                    threadPool,
                    "action",
                    randomIntBetween(1, 10000),
                    TransportVersion.current()
                )
            );
        }
        final Set<TaskId> expectedBannedTasks = installedBans.entrySet()
            .stream()
            .filter(e -> e.getValue().stream().anyMatch(CloseableChannel::isOpen))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        assertBusy(() -> assertThat(taskManager.getBannedTaskIds(), equalTo(expectedBannedTasks)), 30, TimeUnit.SECONDS);
        for (FakeTcpChannel channel : channels) {
            channel.close();
        }
        assertBusy(() -> assertThat(taskManager.getBannedTaskIds(), empty()));
        assertThat(taskManager.numberOfChannelPendingTaskTrackers(), equalTo(0));
    }

    public void testTaskAccounting() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());

        final Task task1 = taskManager.register("transport", "test", new CancellableRequest("thread 1"));
        final Task task2 = taskManager.register("transport", "test", new CancellableRequest("thread 2"));

        final MockConnection connection1 = new MockConnection();
        final MockConnection connection2 = new MockConnection();

        Releasable releasableConnection1 = taskManager.registerChildConnection(task1.getId(), connection1);
        Releasable releasableConnection2 = taskManager.registerChildConnection(task2.getId(), connection2);
        Releasable releasableConnection3 = taskManager.registerChildConnection(task1.getId(), connection1);

        assertEquals(2, taskManager.childTasksPerConnection(task1.getId(), connection1).intValue());
        assertEquals(1, taskManager.childTasksPerConnection(task2.getId(), connection2).intValue());

        releasableConnection1.close();
        assertEquals(1, taskManager.childTasksPerConnection(task1.getId(), connection1).intValue());

        releasableConnection2.close();
        assertNull(taskManager.childTasksPerConnection(task2.getId(), connection2));

        releasableConnection3.close();
        assertNull(taskManager.childTasksPerConnection(task1.getId(), connection1));
    }

    /**
     * Listeners hear about every child cancellation, also when no child task is running any more.
     */
    public void testCancelChildLocalNotifiesCancelledChildRequestListeners() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        final List<String> cancelled = new ArrayList<>();
        final CancelledChildRequestListener listener = (parentTaskId, childRequestId) -> cancelled.add(parentTaskId + "/" + childRequestId);
        taskManager.registerCancelledChildRequestListener(listener);
        final TaskId parentTaskId = new TaskId("parent_node", 1);

        final CancellableRequest runningRequest = new CancellableRequest("running");
        runningRequest.setParentTask(parentTaskId);
        runningRequest.setRequestId(2);
        final CancellableTask runningChild = (CancellableTask) taskManager.register("transport", "test", runningRequest);
        taskManager.cancelChildLocal(parentTaskId, 2, "test");
        assertTrue(runningChild.isCancelled());
        // No child task runs for this request any more.
        taskManager.cancelChildLocal(parentTaskId, 3, "test");
        // Not a child request.
        taskManager.cancelChildLocal(parentTaskId, -1, "test");
        assertThat(cancelled, equalTo(List.of("parent_node:1/2", "parent_node:1/3")));

        taskManager.unregisterCancelledChildRequestListener(listener);
        taskManager.cancelChildLocal(parentTaskId, 4, "test");
        assertThat(cancelled, equalTo(List.of("parent_node:1/2", "parent_node:1/3")));
        taskManager.unregister(runningChild);
    }

    /**
     * Check that registering a task also causes tracing to be started on that task, and that unregistering it ends the span.
     */
    public void testRegisterTaskStartsTracingAndUnregisterEndsIt() {
        final RecordingTracer tracer = new RecordingTracer();
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), tracer);

        final boolean hasParentTask = randomBoolean();
        final TaskId parentTask = hasParentTask ? new TaskId("parentNode", 1) : TaskId.EMPTY_TASK_ID;

        try (var ignored = threadPool.getThreadContext().newStoredContextPreservingResponseHeaders()) {
            final Task task = taskManager.register("testType", "testAction", taskRequest(parentTask));

            assertThat(tracer.spans, hasSize(1));
            final RecordedSpan span = tracer.spans.getFirst();
            assertThat(span.name, equalTo("testAction"));
            assertThat(
                span.attributes,
                equalTo(
                    hasParentTask
                        ? Map.of(AttributeKeys.TASK_ID, task.getId(), AttributeKeys.PARENT_TASK_ID, parentTask.toString())
                        : Map.of(AttributeKeys.TASK_ID, task.getId())
                )
            );
            assertThat(span.endCount, equalTo(0));

            taskManager.unregister(task);
            assertThat(span.endCount, equalTo(1));
        }
    }

    public void testTaskSpanIsNotStoredInThreadContext() {
        final RecordingTracer tracer = new RecordingTracer();
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), tracer);

        final Task task = taskManager.register("testType", "testAction", taskRequest(TaskId.EMPTY_TASK_ID));
        taskManager.unregister(task);

        assertThat(tracer.spans, hasSize(1));
        assertThat(tracer.spans.getFirst().storeCount, equalTo(0));
        assertThat(tracer.spans.getFirst().parent, nullValue());
    }

    /**
     * The span of a task is not made current in any thread context, so the span of the child of a task is given the span of its parent
     * task as the parent explicitly.
     */
    public void testSpanOfTheChildOfALocalTaskIsAChildOfTheSpanOfItsParentTask() {
        final RecordingTracer tracer = new RecordingTracer();
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), tracer);
        final Task parent = taskManager.register("testType", "parentAction", taskRequest(TaskId.EMPTY_TASK_ID));
        // other tasks, so that the parent is not just the only one that there is
        final Task other = taskManager.register("testType", "otherAction", taskRequest(TaskId.EMPTY_TASK_ID));

        final Task child = taskManager.register(
            "testType",
            "childAction",
            taskRequest(new TaskId(taskManager.getNodeId(), parent.getId()))
        );

        assertThat(tracer.spans, hasSize(3));
        final RecordedSpan parentSpan = tracer.spans.get(0);
        final RecordedSpan childSpan = tracer.spans.get(2);
        assertThat(parentSpan.name, equalTo("parentAction"));
        assertThat(childSpan.name, equalTo("childAction"));
        assertThat(childSpan.parent, sameInstance(parentSpan));
        assertThat(tracer.spans.get(1).parent, nullValue());

        taskManager.unregister(child);
        taskManager.unregister(other);
        taskManager.unregister(parent);
    }

    /**
     * Same as above, for a task that can be cancelled, which is registered in another way.
     */
    public void testSpanOfTheChildOfALocalCancellableTaskIsAChildOfTheSpanOfItsParentTask() {
        final RecordingTracer tracer = new RecordingTracer();
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), tracer);
        final Task parent = taskManager.register("testType", "parentAction", new CancellableRequest("parent"));
        assertThat(parent, instanceOf(CancellableTask.class));

        final Task child = taskManager.register(
            "testType",
            "childAction",
            taskRequest(new TaskId(taskManager.getNodeId(), parent.getId()))
        );

        assertThat(tracer.spans, hasSize(2));
        assertThat(tracer.spans.get(1).parent, sameInstance(tracer.spans.get(0)));

        taskManager.unregister(child);
        taskManager.unregister(parent);
    }

    /**
     * If the parent task is not registered (any more), the span is not given a parent, so that it is inherited from the thread context.
     */
    public void testSpanOfTheChildOfATaskThatIsNotRegisteredHasNoExplicitParent() {
        final RecordingTracer tracer = new RecordingTracer();
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), tracer);

        final Task child = taskManager.register("testType", "childAction", taskRequest(new TaskId(taskManager.getNodeId(), 12345)));

        assertThat(tracer.spans, hasSize(1));
        assertThat(tracer.spans.getFirst().parent, nullValue());

        taskManager.unregister(child);
    }

    /**
     * Task ids are only unique within a node, so a parent task of another node must not be looked up among the tasks of this one, even
     * if one of them has the same numeric id: that would make the span of the child a child of the span of an unrelated task.
     */
    public void testSpanOfTheChildOfATaskOfAnotherNodeWithTheSameNumericIdHasNoExplicitParent() {
        final RecordingTracer tracer = new RecordingTracer();
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), tracer);
        final Task unrelated = taskManager.register("testType", "unrelatedAction", taskRequest(TaskId.EMPTY_TASK_ID));
        final String otherNodeId = randomValueOtherThan(taskManager.getNodeId(), () -> randomAlphaOfLength(10));

        final Task child = taskManager.register("testType", "childAction", taskRequest(new TaskId(otherNodeId, unrelated.getId())));

        assertThat(tracer.spans, hasSize(2));
        assertThat(tracer.spans.get(1).parent, nullValue());

        taskManager.unregister(child);
        taskManager.unregister(unrelated);
    }

    /**
     * Check that registering a task without tracing it neither starts nor ends any span.
     */
    public void testRegisterTaskWithoutTracingDoesNotStartSpan() {
        final RecordingTracer tracer = new RecordingTracer();
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), tracer);

        final Task task = taskManager.register("testType", "testAction", taskRequest(TaskId.EMPTY_TASK_ID), false);
        taskManager.unregister(task);

        assertThat(tracer.spans, empty());
    }

    public void testRegisterAndExecuteStartsTracing() {
        final RecordingTracer tracer = new RecordingTracer();
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), tracer);

        taskManager.registerAndExecute(
            "testType",
            new TransportAction<ActionRequest, ActionResponse>(
                "actionName",
                ActionFilters.EMPTY,
                taskManager,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            ) {
                @Override
                protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
                    listener.onResponse(new ActionResponse() {
                        @Override
                        public void writeTo(StreamOutput out) {}
                    });
                }
            },
            new ActionRequest() {
                @Override
                public ActionRequestValidationException validate() {
                    return null;
                }

                @Override
                public TaskId getParentTask() {
                    return TaskId.EMPTY_TASK_ID;
                }
            },
            null,
            ActionTestUtils.assertNoFailureListener(r -> {})
        );

        assertThat(tracer.spans, hasSize(1));
        assertThat(tracer.spans.getFirst().name, equalTo("actionName"));
        assertThat(tracer.spans.getFirst().endCount, equalTo(1));
    }

    private static TaskAwareRequest taskRequest(TaskId parentTask) {
        return new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return parentTask;
            }
        };
    }

    /**
     * A {@link Tracer} that records the spans that were started on it, with their attributes and the number of times they were closed.
     */
    private static class RecordingTracer implements Tracer {
        final List<RecordedSpan> spans = new ArrayList<>();

        @Override
        public SpanBuilder spanBuilder(String name) {
            final RecordedSpan span = new RecordedSpan(name);
            return new SpanBuilder() {
                @Override
                public SpanBuilder setParent(Span parentSpan) {
                    span.parent = parentSpan;
                    return this;
                }

                @Override
                public SpanBuilder setNoParent() {
                    return this;
                }

                @Override
                public SpanBuilder setAttribute(String key, boolean value) {
                    span.attributes.put(key, value);
                    return this;
                }

                @Override
                public SpanBuilder setAttribute(String key, double value) {
                    span.attributes.put(key, value);
                    return this;
                }

                @Override
                public SpanBuilder setAttribute(String key, long value) {
                    span.attributes.put(key, value);
                    return this;
                }

                @Override
                public SpanBuilder setAttribute(String key, String value) {
                    span.attributes.put(key, value);
                    return this;
                }

                @Override
                public Span startSpan() {
                    spans.add(span);
                    return span;
                }
            };
        }

        @Override
        public Span currentSpan() {
            return Span.noop();
        }
    }

    private static class RecordedSpan implements Span {
        final String name;
        final Map<String, Object> attributes = new HashMap<>();
        Span parent;
        int endCount;
        int storeCount;

        RecordedSpan(String name) {
            this.name = name;
        }

        @Override
        public Span setAttribute(String key, boolean value) {
            attributes.put(key, value);
            return this;
        }

        @Override
        public Span setAttribute(String key, double value) {
            attributes.put(key, value);
            return this;
        }

        @Override
        public Span setAttribute(String key, long value) {
            attributes.put(key, value);
            return this;
        }

        @Override
        public Span setAttribute(String key, String value) {
            attributes.put(key, value);
            return this;
        }

        @Override
        public Span recordException(Throwable throwable) {
            return this;
        }

        @Override
        public void end() {
            endCount++;
        }

        @Override
        public ThreadContext.StoredContext storeInThreadContext() {
            storeCount++;
            return () -> {};
        }
    }

    public void testRegisterWithEnabledDisabledTracing() {

        final TaskManager taskManager = spy(new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.noop()));

        taskManager.register("type", "action", makeTaskRequest(true, 123), false);
        verify(taskManager, times(0)).startTrace(any());

        taskManager.register("type", "action", makeTaskRequest(false, 234), false);
        verify(taskManager, times(0)).startTrace(any());

        clearInvocations(taskManager);

        taskManager.register("type", "action", makeTaskRequest(true, 345), true);
        verify(taskManager, times(1)).startTrace(any());

        clearInvocations(taskManager);

        taskManager.register("type", "action", makeTaskRequest(false, 456), true);
        verify(taskManager, times(1)).startTrace(any());
    }

    static class CancellableRequest extends AbstractTransportRequest {
        private final String requestId;

        CancellableRequest(String requestId) {
            this.requestId = requestId;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "request-" + requestId, parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return false;
                }

                @Override
                public String toString() {
                    return getDescription();
                }
            };
        }
    }

    static class SingleThreadedTcpChannel extends FakeTcpChannel {
        private boolean registeredListener = false;

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            if (isOpen()) {
                assertFalse("listener was registered already", registeredListener);
                registeredListener = true;
            }
            super.addCloseListener(listener);
        }
    }

    public static final class MockConnection implements Transport.Connection {
        @Override
        public DiscoveryNode getNode() {
            return null;
        }

        @Override
        public TransportVersion getTransportVersion() {
            return TransportVersion.current();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {}

        @Override
        public void addRemovedListener(ActionListener<Void> listener) {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onRemoved() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void incRef() {}

        @Override
        public boolean tryIncRef() {
            return true;
        }

        @Override
        public boolean decRef() {
            assert false : "shouldn't release a mock connection";
            return false;
        }

        @Override
        public boolean hasReferences() {
            return true;
        }
    }

    private TaskAwareRequest makeTaskRequest(boolean cancellable, final int parentTaskNum) {
        return new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return new TaskId("something", parentTaskNum);
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                if (cancellable) {
                    return new CancellableTask(id, type, action, "request-" + id, parentTaskId, headers) {
                        @Override
                        public boolean shouldCancelChildrenOnCancellation() {
                            return false;
                        }

                        @Override
                        public String toString() {
                            return getDescription();
                        }
                    };
                }

                return new Task(id, type, action, "request-" + id, parentTaskId, headers);
            }
        };
    }

    public void testForEachCancellableTaskIteratesOverTasks() throws Exception {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.noop());

        Task task1 = taskManager.register("transport", "action1", new CancellableRequest("1"));
        Task task2 = taskManager.register("transport", "action2", new CancellableRequest("2"));
        Task task3 = taskManager.register("transport", "action3", new CancellableRequest("3"));
        try {
            assertBusy(() -> {
                Set<Long> visitedTaskIds = new HashSet<>();
                taskManager.forEachCancellableTask(1, info -> {
                    visitedTaskIds.add(info.task().getId());
                    return true;
                });
                assertThat(visitedTaskIds, is(Set.of(task1.getId(), task2.getId(), task3.getId())));
            });
        } finally {
            taskManager.unregister(task1);
            taskManager.unregister(task2);
            taskManager.unregister(task3);
        }
    }

    public void testForEachCancellableTaskEarlyTermination() throws Exception {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.noop());

        Task task1 = taskManager.register("transport", "action1", new CancellableRequest("1"));
        Task task2 = taskManager.register("transport", "action2", new CancellableRequest("2"));
        Task task3 = taskManager.register("transport", "action3", new CancellableRequest("3"));
        try {
            assertBusy(() -> {
                List<Long> visitedTaskIds = new ArrayList<>();
                taskManager.forEachCancellableTask(1, info -> {
                    visitedTaskIds.add(info.task().getId());
                    return false;
                });
                assertThat(visitedTaskIds.size(), is(1));
            });
        } finally {
            taskManager.unregister(task1);
            taskManager.unregister(task2);
            taskManager.unregister(task3);
        }
    }

    public void testForEachCancellableTaskSkipsNonPositiveThreshold() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.noop());

        Task task = taskManager.register("transport", "action", new CancellableRequest("1"));
        try {
            List<Long> visitedTaskIds = new ArrayList<>();
            taskManager.forEachCancellableTask(0, info -> {
                visitedTaskIds.add(info.task().getId());
                return true;
            });
            assertThat(visitedTaskIds.isEmpty(), is(true));

            taskManager.forEachCancellableTask(-1, info -> {
                visitedTaskIds.add(info.task().getId());
                return true;
            });
            assertThat(visitedTaskIds.isEmpty(), is(true));
        } finally {
            taskManager.unregister(task);
        }
    }

    public void testForEachCancellableTaskReportsHasOutstandingChildren() throws Exception {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.noop());
        final var transportServiceMock = mock(TransportService.class);
        when(transportServiceMock.getThreadPool()).thenReturn(threadPool);
        taskManager.setTaskCancellationService(new TaskCancellationService(transportServiceMock) {
            @Override
            void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {}
        });

        Task parentTask = taskManager.register("transport", "parent", new CancellableRequest("parent"));
        try {
            CancellableTask cancellableParent = (CancellableTask) parentTask;

            assertBusy(() -> {
                List<TaskManager.CancellableTaskInfo> infos = new ArrayList<>();
                taskManager.forEachCancellableTask(1, info -> {
                    infos.add(info);
                    return true;
                });
                assertThat(infos.size(), is(1));
                assertThat(infos.getFirst().hasOutstandingChildren(), is(false));
            });

            MockConnection connection = new MockConnection();
            taskManager.registerChildConnection(cancellableParent.getId(), connection);

            List<TaskManager.CancellableTaskInfo> infos = new ArrayList<>();
            taskManager.forEachCancellableTask(1, info -> {
                infos.add(info);
                return true;
            });
            assertThat(infos.size(), is(1));
            assertThat(infos.getFirst().hasOutstandingChildren(), is(true));
        } finally {
            taskManager.unregister(parentTask);
        }
    }

    public void testAddedTaskListenerFiredForCancellableTask() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        List<Task> received = new ArrayList<>();
        taskManager.registerAddedTaskListener(received::add);

        Task task = taskManager.register("transport", "action", new CancellableRequest("1"));
        try {
            assertThat(received.size(), equalTo(1));
            assertThat(received.get(0), equalTo(task));
        } finally {
            taskManager.unregister(task);
        }
    }

    public void testAddedTaskListenerFiredForPlainTask() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        List<Task> received = new ArrayList<>();
        taskManager.registerAddedTaskListener(received::add);

        Task task = taskManager.register("transport", "action", makeTaskRequest(false, 1));
        try {
            assertThat(received.size(), equalTo(1));
            assertThat(received.get(0), equalTo(task));
        } finally {
            taskManager.unregister(task);
        }
    }

    public void testAddedTaskListenerFiredBeforeHandlerRuns() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of("X-Test-Header"));
        List<String> capturedHeaders = new ArrayList<>();
        taskManager.registerAddedTaskListener(t -> {
            String header = threadPool.getThreadContext().getHeader("X-Test-Header");
            capturedHeaders.add(header);
        });

        threadPool.getThreadContext().putHeader("X-Test-Header", "test-value");
        Task task = taskManager.register("transport", "action", makeTaskRequest(false, 1));
        try {
            assertThat(capturedHeaders.size(), equalTo(1));
            assertThat(capturedHeaders.get(0), equalTo("test-value"));
        } finally {
            taskManager.unregister(task);
        }
    }

    public void testUnregisterAddedTaskListenerStopsDelivery() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        List<Task> received = new ArrayList<>();
        AddedTaskListener listener = received::add;
        taskManager.registerAddedTaskListener(listener);
        taskManager.unregisterAddedTaskListener(listener);

        Task task = taskManager.register("transport", "action", makeTaskRequest(false, 1));
        try {
            assertThat(received, empty());
        } finally {
            taskManager.unregister(task);
        }
    }

    public void testRemovedTaskListenerStillFiresOnceRegression() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        List<Task> removed = new ArrayList<>();
        taskManager.registerRemovedTaskListener(removed::add);

        Task task = taskManager.register("transport", "action", makeTaskRequest(false, 1));
        taskManager.unregister(task);

        assertThat(removed.size(), equalTo(1));
        assertThat(removed.get(0), equalTo(task));
    }

}
