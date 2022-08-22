/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.Version;
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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
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
import org.mockito.Mockito;

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
import static org.hamcrest.Matchers.in;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.NOOP);
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
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.NOOP);
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
                            Version.CURRENT
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
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.NOOP);
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
                    Version.CURRENT
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
     * Check that registering a task also causes tracing to be started on that task.
     */
    public void testRegisterTaskStartsTracing() {
        final Tracer mockTracer = Mockito.mock(Tracer.class);
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), mockTracer);

        final Task task = taskManager.register("testType", "testAction", new TaskAwareRequest() {

            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }
        });

        verify(mockTracer).startTrace(any(), eq("task-" + task.getId()), eq("testAction"), anyMap());
    }

    /**
     * Check that unregistering a task also causes tracing to be stopped on that task.
     */
    public void testUnregisterTaskStopsTracing() {
        final Tracer mockTracer = Mockito.mock(Tracer.class);
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), mockTracer);

        final Task task = taskManager.register("testType", "testAction", new TaskAwareRequest() {

            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }
        });

        taskManager.unregister(task);

        verify(mockTracer).stopTrace("task-" + task.getId());
    }

    /**
     * Check that registering and executing a task also causes tracing to be started and stopped on that task.
     */
    public void testRegisterAndExecuteStartsAndStopsTracing() {
        final Tracer mockTracer = Mockito.mock(Tracer.class);
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), mockTracer);

        final Task task = taskManager.registerAndExecute(
            "testType",
            new TransportAction<ActionRequest, ActionResponse>("actionName", new ActionFilters(Set.of()), taskManager) {
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

        verify(mockTracer).startTrace(any(), eq("task-" + task.getId()), eq("actionName"), anyMap());
    }

    static class CancellableRequest extends TransportRequest {
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

}
