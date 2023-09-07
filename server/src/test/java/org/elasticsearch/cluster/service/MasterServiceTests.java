/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalMasterServiceTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.ReachabilityChecker;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.test.tasks.MockTaskManagerListener;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.service.MasterService.MAX_TASK_DESCRIPTION_CHARS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MasterServiceTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static long relativeTimeInMillis;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(MasterServiceTests.class.getName()) {
            @Override
            public long relativeTimeInMillis() {
                return relativeTimeInMillis;
            }

            @Override
            public long rawRelativeTimeInMillis() {
                return relativeTimeInMillis();
            }
        };
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void randomizeCurrentTime() {
        relativeTimeInMillis = randomLongBetween(0L, 1L << 62);
    }

    private MasterService createMasterService(boolean makeMaster) {
        return createMasterService(makeMaster, null);
    }

    private MasterService createMasterService(boolean makeMaster, TaskManager taskManager) {
        return createMasterService(makeMaster, taskManager, threadPool, null);
    }

    private MasterService createMasterService(
        boolean makeMaster,
        TaskManager taskManager,
        ThreadPool threadPool,
        ExecutorService threadPoolExecutor
    ) {
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        final Settings settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
            .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
            .build();

        if (taskManager == null) {
            taskManager = new TaskManager(settings, threadPool, emptySet());
        }

        final MasterService masterService = new MasterService(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            taskManager
        ) {
            @Override
            protected ExecutorService createThreadPoolExecutor() {
                if (threadPoolExecutor == null) {
                    return super.createThreadPoolExecutor();
                } else {
                    return threadPoolExecutor;
                }
            }
        };
        final ClusterState initialClusterState = ClusterState.builder(new ClusterName(MasterServiceTests.class.getSimpleName()))
            .nodes(
                DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(makeMaster ? localNode.getId() : null)
            )
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialClusterState);
        masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            clusterStateRef.set(clusterStatePublicationEvent.getNewState());
            ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
            threadPool.executor(randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC))
                .execute(() -> publishListener.onResponse(null));
        });
        masterService.setClusterStateSupplier(clusterStateRef::get);
        masterService.start();
        return masterService;
    }

    public void testMasterAwareExecution() throws Exception {
        try (var nonMaster = createMasterService(false)) {
            final CountDownLatch latch1 = new CountDownLatch(1);
            nonMaster.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask(randomFrom(Priority.values())) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    throw new AssertionError("should not execute this task");
                }

                @Override
                public void onFailure(Exception e) {
                    assert e instanceof NotMasterException : e;
                    latch1.countDown();
                }
            });
            assertTrue(latch1.await(10, TimeUnit.SECONDS));

            final CountDownLatch latch2 = new CountDownLatch(1);
            new LocalMasterServiceTask(randomFrom(Priority.values())) {
                @Override
                public void execute(ClusterState currentState) {
                    latch2.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("should not fail this task", e);
                }
            }.submit(nonMaster, "test");
            assertTrue(latch2.await(10, TimeUnit.SECONDS));
        }
    }

    /**
     * Check that when the master service publishes a cluster state update, it uses a dedicated task.
     */
    public void testCreatesChildTaskForPublishingClusterState() throws Exception {
        final MockTaskManager taskManager = new MockTaskManager(Settings.EMPTY, threadPool, emptySet());

        final List<String> registeredActions = new ArrayList<>();
        taskManager.addListener(new MockTaskManagerListener() {
            @Override
            public void onTaskRegistered(Task task) {
                registeredActions.add(task.getAction());
            }

            @Override
            public void onTaskUnregistered(Task task) {}

            @Override
            public void waitForTaskCompletion(Task task) {}
        });

        final CountDownLatch latch = new CountDownLatch(1);

        try (MasterService masterService = createMasterService(true, taskManager)) {
            masterService.createTaskQueue("test", Priority.NORMAL, new ClusterStateTaskExecutor<>() {
                @Override
                public ClusterState execute(BatchExecutionContext<ClusterStateTaskListener> batchExecutionContext) {
                    for (final var taskContext : batchExecutionContext.taskContexts()) {
                        taskContext.success(() -> {});
                    }
                    return ClusterState.builder(batchExecutionContext.initialState()).build();
                }

                @Override
                public void clusterStatePublished(ClusterState newClusterState) {
                    latch.countDown();
                }
            }).submitTask("testCreatesChildTaskForPublishingClusterState", new ExpectSuccessTask(), null);

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        assertThat(registeredActions.toString(), registeredActions, contains(MasterService.STATE_UPDATE_ACTION_NAME));
    }

    public void testThreadContext() throws InterruptedException {
        try (var master = createMasterService(true)) {
            final CountDownLatch latch = new CountDownLatch(1);

            try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
                final Map<String, String> expectedHeaders = Collections.singletonMap("test", "test");
                final Map<String, List<String>> expectedResponseHeaders = Collections.singletonMap(
                    "testResponse",
                    Collections.singletonList("testResponse")
                );
                threadPool.getThreadContext().putHeader(expectedHeaders);

                final TimeValue ackTimeout = randomBoolean() ? TimeValue.ZERO : TimeValue.timeValueMillis(randomInt(10000));
                final TimeValue masterTimeout = randomBoolean() ? TimeValue.ZERO : TimeValue.timeValueMillis(randomInt(10000));

                master.submitUnbatchedStateUpdateTask(
                    "test",
                    new AckedClusterStateUpdateTask(ackedRequest(ackTimeout, masterTimeout), null) {
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            assertTrue(threadPool.getThreadContext().isSystemContext());
                            assertEquals(Collections.emptyMap(), threadPool.getThreadContext().getHeaders());
                            threadPool.getThreadContext().addResponseHeader("testResponse", "testResponse");
                            assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());

                            if (randomBoolean()) {
                                return ClusterState.builder(currentState).build();
                            } else if (randomBoolean()) {
                                return currentState;
                            } else {
                                throw new IllegalArgumentException("mock failure");
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertFalse(threadPool.getThreadContext().isSystemContext());
                            assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                            assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                            latch.countDown();
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                            assertFalse(threadPool.getThreadContext().isSystemContext());
                            assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                            assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                            latch.countDown();
                        }

                        @Override
                        public void onAllNodesAcked() {
                            assertFalse(threadPool.getThreadContext().isSystemContext());
                            assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                            assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                            latch.countDown();
                        }

                        @Override
                        public void onAckFailure(Exception e) {
                            assertFalse(threadPool.getThreadContext().isSystemContext());
                            assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                            assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                            latch.countDown();
                        }

                        @Override
                        public void onAckTimeout() {
                            assertFalse(threadPool.getThreadContext().isSystemContext());
                            assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                            assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                            latch.countDown();
                        }

                    }
                );

                assertFalse(threadPool.getThreadContext().isSystemContext());
                assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                assertEquals(Collections.emptyMap(), threadPool.getThreadContext().getResponseHeaders());
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }

    /*
    * test that a listener throwing an exception while handling a
    * notification does not prevent publication notification to the
    * executor
    */
    public void testClusterStateTaskListenerThrowingExceptionIsOkay() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        try (MasterService masterService = createMasterService(true)) {
            masterService.createTaskQueue(
                "testClusterStateTaskListenerThrowingExceptionIsOkay",
                Priority.NORMAL,
                new ClusterStateTaskExecutor<ExpectSuccessTask>() {
                    @Override
                    public ClusterState execute(BatchExecutionContext<ExpectSuccessTask> batchExecutionContext) {
                        for (final var taskContext : batchExecutionContext.taskContexts()) {
                            taskContext.success(() -> { throw new RuntimeException("testing exception handling"); });
                        }
                        return ClusterState.builder(batchExecutionContext.initialState()).build();
                    }

                    @Override
                    public void clusterStatePublished(ClusterState newClusterState) {
                        latch.countDown();
                    }
                }
            ).submitTask("testClusterStateTaskListenerThrowingExceptionIsOkay", new ExpectSuccessTask(), null);
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }

    @TestLogging(value = "org.elasticsearch.cluster.service:TRACE", reason = "to ensure that we log cluster state events on TRACE level")
    public void testClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1 start",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [test1]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1 computation",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [1s] to compute cluster state update for [test1]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1 notification",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [0s] to notify listeners on unchanged cluster state for [test1]"
            )
        );

        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2 start",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [test2]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2 failure",
                MasterService.class.getCanonicalName(),
                Level.TRACE,
                "failed to execute cluster state update (on version: [*], uuid: [*]) for [test2]*"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2 computation",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [2s] to compute cluster state update for [test2]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2 notification",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [0s] to notify listeners on unchanged cluster state for [test2]"
            )
        );

        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3 start",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [test3]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3 computation",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [3s] to compute cluster state update for [test3]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3 notification",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [4s] to notify listeners on successful publication of cluster state (version: *, uuid: *) for [test3]"
            )
        );

        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test4",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [test4]"
            )
        );

        try (var ignored = mockAppender.capturing(MasterService.class); var masterService = createMasterService(true)) {
            masterService.submitUnbatchedStateUpdateTask("test1", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += TimeValue.timeValueSeconds(1).millis();
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            masterService.submitUnbatchedStateUpdateTask("test2", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += TimeValue.timeValueSeconds(2).millis();
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(Exception e) {}
            });
            masterService.submitUnbatchedStateUpdateTask("test3", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += TimeValue.timeValueSeconds(3).millis();
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    relativeTimeInMillis += TimeValue.timeValueSeconds(4).millis();
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            masterService.submitUnbatchedStateUpdateTask("test4", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            assertBusy(mockAppender::assertAllExpectationsMatched);
        }
    }

    public void testMultipleSubmissionBatching() throws Exception {

        final int executorCount = between(1, 5);
        final var executionCountDown = new CountDownLatch(executorCount);

        class Executor implements ClusterStateTaskExecutor<ExpectSuccessTask> {

            final AtomicBoolean executed = new AtomicBoolean();

            int expectedTaskCount;

            public void addExpectedTaskCount(int taskCount) {
                expectedTaskCount += taskCount;
            }

            @Override
            public ClusterState execute(BatchExecutionContext<ExpectSuccessTask> batchExecutionContext) {
                assertTrue("Should execute all tasks at once", executed.compareAndSet(false, true));
                assertThat("Should execute all tasks at once", batchExecutionContext.taskContexts().size(), equalTo(expectedTaskCount));
                executionCountDown.countDown();
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    taskContext.success(() -> {});
                }
                return batchExecutionContext.initialState();
            }
        }

        record QueueAndExecutor(MasterServiceTaskQueue<ExpectSuccessTask> queue, Executor executor) {}

        try (var masterService = createMasterService(true)) {

            final var executors = new QueueAndExecutor[executorCount];
            for (int i = 0; i < executors.length; i++) {
                final var executor = new Executor();
                executors[i] = new QueueAndExecutor(
                    masterService.createTaskQueue("executor-" + i, randomFrom(Priority.values()), executor),
                    executor
                );
            }

            final var executionBarrier = new CyclicBarrier(2);

            masterService.createTaskQueue("block", Priority.NORMAL, batchExecutionContext -> {
                executionBarrier.await(10, TimeUnit.SECONDS); // notify test thread that the master service is blocked
                executionBarrier.await(10, TimeUnit.SECONDS); // wait for test thread to release us
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    taskContext.success(() -> {});
                }
                return batchExecutionContext.initialState();
            }).submitTask("block", new ExpectSuccessTask(), null);

            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked

            final var submissionLatch = new CountDownLatch(1);

            final var submitThreads = new Thread[between(1, 10)];
            for (int i = 0; i < submitThreads.length; i++) {
                final var executor = randomFrom(executors);
                final var task = new ExpectSuccessTask();
                executor.executor().addExpectedTaskCount(1);
                submitThreads[i] = new Thread(() -> {
                    try {
                        assertTrue(submissionLatch.await(10, TimeUnit.SECONDS));
                        executor.queue().submitTask(Thread.currentThread().getName(), task, null);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }, "submit-thread-" + i);
            }

            for (var executor : executors) {
                if (executor.executor().expectedTaskCount == 0) {
                    executionCountDown.countDown();
                }
            }

            for (var submitThread : submitThreads) {
                submitThread.start();
            }

            submissionLatch.countDown();

            for (var submitThread : submitThreads) {
                submitThread.join();
            }

            for (var executor : executors) {
                assertFalse(executor.executor().executed.get());
            }

            assertThat(masterService.numberOfPendingTasks(), equalTo(submitThreads.length + 1));
            final var sources = masterService.pendingTasks().stream().map(t -> t.getSource().string()).collect(Collectors.toSet());
            assertThat(sources, hasSize(submitThreads.length + 1));
            assertTrue(sources.contains("block"));
            for (int i = 0; i < submitThreads.length; i++) {
                assertTrue("submit-thread-" + i, sources.contains("submit-thread-" + i));
            }

            executionBarrier.await(10, TimeUnit.SECONDS); // release block on master service
            assertTrue(executionCountDown.await(10, TimeUnit.SECONDS));
            for (var executor : executors) {
                assertTrue(executor.executor().executed.get() != (executor.executor().expectedTaskCount == 0));
            }
        }
    }

    public void testClusterStateBatchedUpdates() throws InterruptedException {

        AtomicInteger executedTasks = new AtomicInteger();
        AtomicInteger submittedTasks = new AtomicInteger();
        AtomicInteger processedStates = new AtomicInteger();
        SetOnce<CountDownLatch> processedStatesLatch = new SetOnce<>();
        final String responseHeaderName = randomAlphaOfLength(10);

        class Task implements ClusterStateTaskListener {
            private final AtomicBoolean executed = new AtomicBoolean();
            private final int id;

            Task(int id) {
                this.id = id;
            }

            public void execute() {
                threadPool.getThreadContext().addResponseHeader(responseHeaderName, toString());
                if (executed.compareAndSet(false, true) == false) {
                    throw new AssertionError("Task [" + id + "] should only be executed once");
                } else {
                    executedTasks.incrementAndGet();
                }
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("should not be called", e);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Task task = (Task) o;
                return id == task.id;
            }

            @Override
            public int hashCode() {
                return id;
            }

            @Override
            public String toString() {
                return Integer.toString(id);
            }
        }

        final int numberOfThreads = randomIntBetween(2, 8);
        final int taskSubmissionsPerThread = randomIntBetween(1, 64);
        final int numberOfExecutors = Math.max(1, numberOfThreads / 4);
        final Semaphore semaphore = new Semaphore(1);

        class TaskExecutor implements ClusterStateTaskExecutor<Task> {

            private final AtomicInteger executed = new AtomicInteger();
            private final AtomicInteger assigned = new AtomicInteger();
            private final AtomicInteger batches = new AtomicInteger();
            private final AtomicInteger published = new AtomicInteger();
            private final List<Task> assignments = new ArrayList<>();

            @Override
            public ClusterState execute(BatchExecutionContext<Task> batchExecutionContext) {
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    assertThat("All tasks should belong to this executor", assignments, hasItem(taskContext.getTask()));
                }

                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    if (randomBoolean()) {
                        try (var ignored = taskContext.captureResponseHeaders()) {
                            threadPool.getThreadContext().addResponseHeader(responseHeaderName, randomAlphaOfLength(10));
                        }
                    }
                    try (var ignored = taskContext.captureResponseHeaders()) {
                        taskContext.getTask().execute();
                    }
                    if (randomBoolean()) {
                        try (var ignored = taskContext.captureResponseHeaders()) {
                            threadPool.getThreadContext().addResponseHeader(responseHeaderName, randomAlphaOfLength(10));
                        }
                    }
                }

                executed.addAndGet(batchExecutionContext.taskContexts().size());
                ClusterState maybeUpdatedClusterState = batchExecutionContext.initialState();
                if (randomBoolean()) {
                    maybeUpdatedClusterState = ClusterState.builder(batchExecutionContext.initialState()).build();
                    batches.incrementAndGet();
                    assertThat(
                        "All cluster state modifications should be executed on a single thread",
                        semaphore.tryAcquire(),
                        equalTo(true)
                    );
                }

                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    taskContext.success(() -> {
                        assertThat(
                            threadPool.getThreadContext().getResponseHeaders().get(responseHeaderName),
                            hasItem(taskContext.getTask().toString())
                        );
                        processedStates.incrementAndGet();
                        processedStatesLatch.get().countDown();
                    });
                }

                return maybeUpdatedClusterState;
            }

            @Override
            public void clusterStatePublished(ClusterState newClusterState) {
                published.incrementAndGet();
                semaphore.release();
            }
        }

        record QueueAndExecutor(MasterServiceTaskQueue<Task> queue, TaskExecutor executor) {}

        try (var masterService = createMasterService(true)) {
            final var executors = new ArrayList<QueueAndExecutor>();
            for (int i = 0; i < numberOfExecutors; i++) {
                final var executor = new TaskExecutor();
                executors.add(
                    new QueueAndExecutor(masterService.createTaskQueue("queue-" + i, randomFrom(Priority.values()), executor), executor)
                );
            }

            // randomly assign tasks to queues
            List<Tuple<MasterServiceTaskQueue<Task>, Task>> assignments = new ArrayList<>();
            AtomicInteger totalTasks = new AtomicInteger();
            for (int i = 0; i < numberOfThreads; i++) {
                for (int j = 0; j < taskSubmissionsPerThread; j++) {
                    var executor = randomFrom(executors);
                    var task = new Task(totalTasks.getAndIncrement());

                    assignments.add(Tuple.tuple(executor.queue(), task));
                    executor.executor().assigned.incrementAndGet();
                    executor.executor().assignments.add(task);
                }
            }
            processedStatesLatch.set(new CountDownLatch(totalTasks.get()));

            final var barrier = new CyclicBarrier(1 + numberOfThreads);
            for (int i = 0; i < numberOfThreads; i++) {
                final int index = i;
                Thread thread = new Thread(() -> {
                    final String threadName = Thread.currentThread().getName();
                    safeAwait(barrier);
                    for (int j = 0; j < taskSubmissionsPerThread; j++) {
                        var assignment = assignments.get(index * taskSubmissionsPerThread + j);
                        var task = assignment.v2();
                        var executor = assignment.v1();
                        submittedTasks.incrementAndGet();
                        executor.submitTask(threadName, task, null);
                    }
                    safeAwait(barrier);
                });
                thread.start();
            }

            // wait for all threads to be ready
            safeAwait(barrier);
            // wait for all threads to finish
            safeAwait(barrier);

            // wait until all the cluster state updates have been processed
            safeAwait(processedStatesLatch.get());
            // and until all the publication callbacks have completed
            assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));

            // assert the number of executed tasks is correct
            assertThat(submittedTasks.get(), equalTo(totalTasks.get()));
            assertThat(executedTasks.get(), equalTo(totalTasks.get()));
            assertThat(processedStates.get(), equalTo(totalTasks.get()));

            // assert each executor executed the correct number of tasks
            for (var executor : executors) {
                assertEquals(executor.executor().assigned.get(), executor.executor().executed.get());
                assertEquals(executor.executor().batches.get(), executor.executor().published.get());
            }
        }
    }

    public void testTaskFailureNotification() throws Exception {

        final String testContextHeaderName = "test-context-header";
        final ThreadContext threadContext = threadPool.getThreadContext();
        final int taskCount = between(1, 10);
        final CountDownLatch taskCountDown = new CountDownLatch(taskCount);

        class Task implements ClusterStateTaskListener {

            private final String expectedHeaderValue;

            Task(String expectedHeaderValue) {
                this.expectedHeaderValue = expectedHeaderValue;
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(RuntimeException.class));
                assertThat(e.getMessage(), equalTo("simulated"));
                assertThat(threadContext.getHeader(testContextHeaderName), equalTo(expectedHeaderValue));
                taskCountDown.countDown();
            }
        }

        final ClusterStateTaskExecutor<Task> executor = batchExecutionContext -> {
            if (randomBoolean()) {
                throw new RuntimeException("simulated");
            } else {
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    taskContext.onFailure(new RuntimeException("simulated"));
                }
                return batchExecutionContext.initialState();
            }
        };

        final var executionBarrier = new CyclicBarrier(2);
        final ClusterStateUpdateTask blockMasterTask = new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                executionBarrier.await(10, TimeUnit.SECONDS); // notify test thread that the master service is blocked
                executionBarrier.await(10, TimeUnit.SECONDS); // wait for test thread to release us
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };

        try (var masterService = createMasterService(true)) {
            final var queue = masterService.createTaskQueue("test", Priority.NORMAL, executor);

            masterService.submitUnbatchedStateUpdateTask("block", blockMasterTask);
            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked

            masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
                throw new AssertionError("should not publish any states");
            });

            for (int i = 0; i < taskCount; i++) {
                try (ThreadContext.StoredContext ignored = threadContext.newStoredContext()) {
                    final String testContextHeaderValue = randomAlphaOfLength(10);
                    threadContext.putHeader(testContextHeaderName, testContextHeaderValue);
                    queue.submitTask("test", new Task(testContextHeaderValue), null);
                }
            }

            executionBarrier.await(10, TimeUnit.SECONDS); // release block on master service
            assertTrue(taskCountDown.await(10, TimeUnit.SECONDS));
        }
    }

    public void testTaskNotificationAfterPublication() throws Exception {

        class Task implements ClusterStateTaskListener {

            final ActionListener<ClusterState> publishListener;
            final String responseHeaderValue;
            final boolean expectFailure;

            Task(boolean expectFailure, String responseHeaderValue, ActionListener<ClusterState> publishListener) {
                this.expectFailure = expectFailure;
                this.responseHeaderValue = responseHeaderValue;
                this.publishListener = publishListener;
            }

            @Override
            public void onFailure(Exception e) {
                publishListener.onFailure(e);
            }
        }

        final String testContextHeaderName = "test-context-header";
        final ThreadContext threadContext = threadPool.getThreadContext();

        final var testResponseHeaderName = "test-response-header";

        final var taskFailedExceptionMessage = "simulated task failure";

        final var executor = new ClusterStateTaskExecutor<Task>() {
            @Override
            @SuppressForbidden(reason = "consuming published cluster state for legacy reasons")
            public ClusterState execute(BatchExecutionContext<Task> batchExecutionContext) {
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    try (var ignored = taskContext.captureResponseHeaders()) {
                        threadPool.getThreadContext().addResponseHeader(testResponseHeaderName, taskContext.getTask().responseHeaderValue);
                    }
                    if (taskContext.getTask().expectFailure) {
                        taskContext.onFailure(new ElasticsearchException(taskFailedExceptionMessage));
                    } else {
                        taskContext.success(taskContext.getTask().publishListener::onResponse);
                    }
                }
                return ClusterState.builder(batchExecutionContext.initialState()).build();
            }
        };

        final var blockedState = new AtomicReference<ClusterState>();
        final var executionBarrier = new CyclicBarrier(2);
        final ClusterStateUpdateTask blockMasterTask = new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                assertTrue(blockedState.compareAndSet(null, currentState));
                executionBarrier.await(10, TimeUnit.SECONDS); // notify test thread that the master service is blocked
                executionBarrier.await(10, TimeUnit.SECONDS); // wait for test thread to release us
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };

        try (var masterService = createMasterService(true)) {

            final var queue = masterService.createTaskQueue("test", Priority.NORMAL, executor);

            // success case: submit some tasks, possibly in different contexts, and verify that the expected listener is completed

            masterService.submitUnbatchedStateUpdateTask("block", blockMasterTask);
            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked
            final var stateBeforeSuccess = blockedState.get();
            assertNotNull(stateBeforeSuccess);

            final AtomicReference<ClusterState> publishedState = new AtomicReference<>();
            masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
                assertSame(stateBeforeSuccess, clusterStatePublicationEvent.getOldState());
                assertNotSame(stateBeforeSuccess, clusterStatePublicationEvent.getNewState());
                assertTrue(publishedState.compareAndSet(null, clusterStatePublicationEvent.getNewState()));
                ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
                publishListener.onResponse(null);
            });

            int toSubmit = between(1, 10);
            final CountDownLatch publishSuccessCountdown = new CountDownLatch(toSubmit);

            for (int i = 0; i < toSubmit; i++) {
                try (ThreadContext.StoredContext ignored = threadContext.newStoredContext()) {
                    final var testContextHeaderValue = randomAlphaOfLength(10);
                    final var testResponseHeaderValue = randomAlphaOfLength(10);
                    threadContext.putHeader(testContextHeaderName, testContextHeaderValue);
                    final var expectFailure = randomBoolean();
                    final var taskComplete = new AtomicBoolean();
                    final var task = new Task(expectFailure, testResponseHeaderValue, new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterState clusterState) {
                            assertFalse(expectFailure);
                            assertEquals(testContextHeaderValue, threadContext.getHeader(testContextHeaderName));
                            assertEquals(List.of(testResponseHeaderValue), threadContext.getResponseHeaders().get(testResponseHeaderName));
                            assertSame(publishedState.get(), clusterState);
                            assertNotSame(stateBeforeSuccess, publishedState.get());
                            assertTrue(taskComplete.compareAndSet(false, true));
                            publishSuccessCountdown.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertTrue(expectFailure);
                            assertThat(e, instanceOf(ElasticsearchException.class));
                            assertThat(e.getMessage(), equalTo(taskFailedExceptionMessage));
                            assertEquals(List.of(testResponseHeaderValue), threadContext.getResponseHeaders().get(testResponseHeaderName));
                            assertNotNull(publishedState.get());
                            assertNotSame(stateBeforeSuccess, publishedState.get());
                            assertTrue(taskComplete.compareAndSet(false, true));
                            publishSuccessCountdown.countDown();
                        }
                    });

                    queue.submitTask("test", task, null);
                }
            }

            executionBarrier.await(10, TimeUnit.SECONDS); // release block on master service
            assertTrue(publishSuccessCountdown.await(10, TimeUnit.SECONDS));

            // failure case: submit some tasks, possibly in different contexts, and verify that the expected listener is completed

            assertNotNull(blockedState.getAndSet(null));
            assertNotNull(publishedState.getAndSet(null));
            masterService.submitUnbatchedStateUpdateTask("block", blockMasterTask);
            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked
            final var stateBeforeFailure = blockedState.get();
            assertNotNull(stateBeforeFailure);

            final var publicationFailedExceptionMessage = "simulated publication failure";

            masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
                assertSame(stateBeforeFailure, clusterStatePublicationEvent.getOldState());
                assertNotSame(stateBeforeFailure, clusterStatePublicationEvent.getNewState());
                assertTrue(publishedState.compareAndSet(null, clusterStatePublicationEvent.getNewState()));
                ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
                publishListener.onFailure(new FailedToCommitClusterStateException(publicationFailedExceptionMessage));
            });

            toSubmit = between(1, 10);
            final CountDownLatch publishFailureCountdown = new CountDownLatch(toSubmit);

            for (int i = 0; i < toSubmit; i++) {
                try (ThreadContext.StoredContext ignored = threadContext.newStoredContext()) {
                    final String testContextHeaderValue = randomAlphaOfLength(10);
                    final String testResponseHeaderValue = randomAlphaOfLength(10);
                    threadContext.putHeader(testContextHeaderName, testContextHeaderValue);
                    final var expectFailure = randomBoolean();
                    final var taskComplete = new AtomicBoolean();
                    final var task = new Task(expectFailure, testResponseHeaderValue, new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterState clusterState) {
                            throw new AssertionError("should not succeed");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertEquals(testContextHeaderValue, threadContext.getHeader(testContextHeaderName));
                            assertEquals(List.of(testResponseHeaderValue), threadContext.getResponseHeaders().get(testResponseHeaderName));
                            assertThat(e, instanceOf(FailedToCommitClusterStateException.class));
                            assertThat(e.getMessage(), equalTo(publicationFailedExceptionMessage));
                            if (expectFailure) {
                                assertThat(e.getSuppressed().length, greaterThan(0));
                                var suppressed = e.getSuppressed()[0];
                                assertThat(suppressed, instanceOf(ElasticsearchException.class));
                                assertThat(suppressed.getMessage(), equalTo(taskFailedExceptionMessage));
                            }
                            assertNotNull(publishedState.get());
                            assertNotSame(stateBeforeFailure, publishedState.get());
                            assertTrue(taskComplete.compareAndSet(false, true));
                            publishFailureCountdown.countDown();
                        }
                    });

                    queue.submitTask("test", task, null);
                }
            }

            executionBarrier.await(10, TimeUnit.SECONDS); // release block on master service
            assertTrue(publishFailureCountdown.await(10, TimeUnit.SECONDS));
        }
    }

    public void testBlockingCallInClusterStateTaskListenerFails() throws InterruptedException {
        assumeTrue("assertions must be enabled for this test to work", PlainActionFuture.class.desiredAssertionStatus());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AssertionError> assertionRef = new AtomicReference<>();

        try (MasterService masterService = createMasterService(true)) {
            masterService.createTaskQueue("testBlockingCallInClusterStateTaskListenerFails", Priority.NORMAL, batchExecutionContext -> {
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    taskContext.success(() -> {
                        PlainActionFuture<Void> future = new PlainActionFuture<>();
                        try {
                            if (randomBoolean()) {
                                future.get(1L, TimeUnit.SECONDS);
                            } else {
                                future.get();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } catch (AssertionError e) {
                            assertionRef.set(e);
                            latch.countDown();
                        }
                    });
                }
                return ClusterState.builder(batchExecutionContext.initialState()).build();
            }).submitTask("testBlockingCallInClusterStateTaskListenerFails", new ExpectSuccessTask(), null);

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertNotNull(assertionRef.get());
            assertThat(assertionRef.get().getMessage(), containsString("Reason: [Blocking operation]"));
        }
    }

    @TestLogging(value = "org.elasticsearch.cluster.service:WARN", reason = "to ensure that we log cluster state events on WARN level")
    public void testLongClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "test1 shouldn't log because it was fast enough",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took*test1*"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took [*] to compute cluster state update for [test2], which exceeds the warn threshold of [10s]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took [*] to compute cluster state update for [test3], which exceeds the warn threshold of [10s]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test4",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took [*] to compute cluster state update for [test4], which exceeds the warn threshold of [10s]"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "test5 should not log despite publishing slowly",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took*test5*"
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test6 should log due to slow and failing publication",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "took [*] and then failed to publish updated cluster state (version: *, uuid: *) for [test6]:*"
            )
        );

        final Settings settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
            .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
            .build();
        try (
            var ignored = mockAppender.capturing(MasterService.class);
            MasterService masterService = new MasterService(
                settings,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                new TaskManager(settings, threadPool, emptySet())
            ) {
                @Override
                protected boolean publicationMayFail() {
                    // checking logging even during unexpected failures
                    return true;
                }
            }
        ) {
            final DiscoveryNode localNode = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
            final ClusterState initialClusterState = ClusterState.builder(new ClusterName(MasterServiceTests.class.getSimpleName()))
                .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                .build();
            final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialClusterState);
            masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
                ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
                if (clusterStatePublicationEvent.getSummary().toString().contains("test5")) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                }
                if (clusterStatePublicationEvent.getSummary().toString().contains("test6")) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                    throw new ElasticsearchException("simulated error during slow publication which should trigger logging");
                }
                clusterStateRef.set(clusterStatePublicationEvent.getNewState());
                publishListener.onResponse(null);
            });
            masterService.setClusterStateSupplier(clusterStateRef::get);
            masterService.start();

            final CountDownLatch latch = new CountDownLatch(6);
            final CountDownLatch processedFirstTask = new CountDownLatch(1);
            masterService.submitUnbatchedStateUpdateTask("test1", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += randomLongBetween(
                        0L,
                        MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                    );
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                    processedFirstTask.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });

            processedFirstTask.await();
            masterService.submitUnbatchedStateUpdateTask("test2", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            });
            masterService.submitUnbatchedStateUpdateTask("test3", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            masterService.submitUnbatchedStateUpdateTask("test4", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            masterService.submitUnbatchedStateUpdateTask("test5", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            masterService.submitUnbatchedStateUpdateTask("test6", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(); // maybe we should notify here?
                }
            });
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            masterService.submitUnbatchedStateUpdateTask("test7", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            latch.await();
            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testAcking() throws InterruptedException {
        final DiscoveryNode node1 = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        final DiscoveryNode node2 = DiscoveryNodeUtils.builder("node2").roles(emptySet()).build();
        final DiscoveryNode node3 = DiscoveryNodeUtils.builder("node3").roles(emptySet()).build();
        final Settings settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
            .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
            .build();
        try (
            MasterService masterService = new MasterService(
                settings,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                new TaskManager(settings, threadPool, emptySet())
            )
        ) {

            final var responseHeaderName = "test-response-header";

            final ClusterState initialClusterState = ClusterState.builder(new ClusterName(MasterServiceTests.class.getSimpleName()))
                .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3).localNodeId(node1.getId()).masterNodeId(node1.getId()))
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                .build();
            final AtomicReference<ClusterStatePublisher> publisherRef = new AtomicReference<>();
            masterService.setClusterStatePublisher((e, pl, al) -> {
                ClusterServiceUtils.setAllElapsedMillis(e);
                publisherRef.get().publish(e, pl, al);
            });
            masterService.setClusterStateSupplier(() -> initialClusterState);
            masterService.start();

            class LatchAckListener implements ClusterStateAckListener {
                private final CountDownLatch latch;

                LatchAckListener(CountDownLatch latch) {
                    this.latch = latch;
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked() {
                    latch.countDown();
                }

                @Override
                public void onAckFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                public void onAckTimeout() {
                    fail();
                }

                @Override
                public TimeValue ackTimeout() {
                    return TimeValue.timeValueDays(30);
                }
            }

            // check that we complete the ack listener
            {
                final CountDownLatch latch = new CountDownLatch(2);

                publisherRef.set((clusterChangedEvent, publishListener, ackListener) -> {
                    publishListener.onResponse(null);
                    ackListener.onCommit(TimeValue.ZERO);
                    ackListener.onNodeAck(node1, null);
                    ackListener.onNodeAck(node2, null);
                    ackListener.onNodeAck(node3, null);
                });

                class Task extends LatchAckListener implements ClusterStateTaskListener {
                    Task() {
                        super(latch);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError(e);
                    }
                }

                masterService.<Task>createTaskQueue("success-test", Priority.NORMAL, batchExecutionContext -> {
                    for (final var taskContext : batchExecutionContext.taskContexts()) {
                        final var responseHeaderValue = randomAlphaOfLength(10);
                        try (var ignored = taskContext.captureResponseHeaders()) {
                            threadPool.getThreadContext().addResponseHeader(responseHeaderName, responseHeaderValue);
                        }
                        taskContext.success(() -> {
                            assertThat(
                                threadPool.getThreadContext().getResponseHeaders().get(responseHeaderName),
                                equalTo(List.of(responseHeaderValue))
                            );
                            latch.countDown();
                        }, taskContext.getTask());
                    }
                    return randomBoolean()
                        ? batchExecutionContext.initialState()
                        : ClusterState.builder(batchExecutionContext.initialState()).build();
                }).submitTask("success-test", new Task(), null);

                assertTrue(latch.await(10, TimeUnit.SECONDS));
            }

            // check that we complete a dynamic ack listener supplied by the task
            {
                final CountDownLatch latch = new CountDownLatch(2);

                publisherRef.set((clusterChangedEvent, publishListener, ackListener) -> {
                    publishListener.onResponse(null);
                    ackListener.onCommit(TimeValue.ZERO);
                    ackListener.onNodeAck(node1, null);
                    ackListener.onNodeAck(node2, null);
                    ackListener.onNodeAck(node3, null);
                });

                class Task implements ClusterStateTaskListener {
                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError(e);
                    }
                }

                masterService.<Task>createTaskQueue("success-test", Priority.NORMAL, batchExecutionContext -> {
                    for (final var taskContext : batchExecutionContext.taskContexts()) {
                        taskContext.success(latch::countDown, new LatchAckListener(latch));
                    }
                    return randomBoolean()
                        ? batchExecutionContext.initialState()
                        : ClusterState.builder(batchExecutionContext.initialState()).build();
                }).submitTask("success-test", new Task(), null);

                assertTrue(latch.await(10, TimeUnit.SECONDS));
            }

            // check that we supply a no-op publish listener if we only care about acking
            {
                final CountDownLatch latch = new CountDownLatch(1);

                publisherRef.set((clusterChangedEvent, publishListener, ackListener) -> {
                    publishListener.onResponse(null);
                    ackListener.onCommit(TimeValue.ZERO);
                    ackListener.onNodeAck(node1, null);
                    ackListener.onNodeAck(node2, null);
                    ackListener.onNodeAck(node3, null);
                });

                class Task implements ClusterStateTaskListener {
                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError(e);
                    }
                }

                masterService.<Task>createTaskQueue("success-test", Priority.NORMAL, batchExecutionContext -> {
                    for (final var taskContext : batchExecutionContext.taskContexts()) {
                        taskContext.success(new LatchAckListener(latch));
                    }
                    return randomBoolean()
                        ? batchExecutionContext.initialState()
                        : ClusterState.builder(batchExecutionContext.initialState()).build();
                }).submitTask("success-test", new Task(), null);

                assertTrue(latch.await(10, TimeUnit.SECONDS));
            }

            // check that exception from acking is passed to listener
            {
                final CountDownLatch latch = new CountDownLatch(1);

                publisherRef.set((clusterChangedEvent, publishListener, ackListener) -> {
                    publishListener.onResponse(null);
                    ackListener.onCommit(TimeValue.ZERO);
                    ackListener.onNodeAck(node1, null);
                    ackListener.onNodeAck(node2, new ElasticsearchException("simulated"));
                    ackListener.onNodeAck(node3, null);
                });

                class Task implements ClusterStateTaskListener {
                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError(e);
                    }
                }

                masterService.<Task>createTaskQueue("node-ack-fail-test", Priority.NORMAL, batchExecutionContext -> {
                    for (final var taskContext : batchExecutionContext.taskContexts()) {
                        final var responseHeaderValue = randomAlphaOfLength(10);
                        try (var ignored = taskContext.captureResponseHeaders()) {
                            threadPool.getThreadContext().addResponseHeader(responseHeaderName, responseHeaderValue);
                        }
                        taskContext.success(new LatchAckListener(latch) {
                            @Override
                            public void onAllNodesAcked() {
                                fail();
                            }

                            @Override
                            public void onAckFailure(Exception e) {
                                assertThat(
                                    threadPool.getThreadContext().getResponseHeaders().get(responseHeaderName),
                                    equalTo(List.of(responseHeaderValue))
                                );
                                assertThat(e, instanceOf(ElasticsearchException.class));
                                assertThat(e.getMessage(), equalTo("simulated"));
                                latch.countDown();
                            }
                        });
                    }
                    return ClusterState.builder(batchExecutionContext.initialState()).build();
                }).submitTask("node-ack-fail-test", new Task(), null);

                assertTrue(latch.await(10, TimeUnit.SECONDS));
            }

            // check that we don't time out before even committing the cluster state
            {
                final CountDownLatch latch = new CountDownLatch(1);

                publisherRef.set(
                    (clusterChangedEvent, publishListener, ackListener) -> publishListener.onFailure(
                        new FailedToCommitClusterStateException("mock exception")
                    )
                );

                masterService.submitUnbatchedStateUpdateTask(
                    "test2",
                    new AckedClusterStateUpdateTask(ackedRequest(TimeValue.ZERO, null), null) {
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            return ClusterState.builder(currentState).build();
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                            fail();
                        }

                        @Override
                        protected AcknowledgedResponse newResponse(boolean acknowledged) {
                            fail();
                            return null;
                        }

                        @Override
                        public void onFailure(Exception e) {
                            latch.countDown();
                        }

                        @Override
                        public void onAckTimeout() {
                            fail();
                        }
                    }
                );

                latch.await();
            }

            // check that we timeout if commit took too long
            {
                final CountDownLatch latch = new CountDownLatch(2);

                final TimeValue ackTimeout = TimeValue.timeValueMillis(randomInt(100));

                publisherRef.set((clusterChangedEvent, publishListener, ackListener) -> {
                    publishListener.onResponse(null);
                    ackListener.onCommit(TimeValue.timeValueMillis(ackTimeout.millis() + randomInt(100)));
                    ackListener.onNodeAck(node1, null);
                    ackListener.onNodeAck(node2, null);
                    ackListener.onNodeAck(node3, null);
                });

                final var responseHeaderValue = randomAlphaOfLength(10);

                masterService.submitUnbatchedStateUpdateTask(
                    "test2",
                    new AckedClusterStateUpdateTask(ackedRequest(ackTimeout, null), null) {
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            threadPool.getThreadContext().addResponseHeader(responseHeaderName, responseHeaderValue);
                            return ClusterState.builder(currentState).build();
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                            latch.countDown();
                        }

                        @Override
                        protected AcknowledgedResponse newResponse(boolean acknowledged) {
                            fail();
                            return null;
                        }

                        @Override
                        public void onFailure(Exception e) {
                            fail();
                        }

                        @Override
                        public void onAckTimeout() {
                            assertThat(
                                threadPool.getThreadContext().getResponseHeaders().get(responseHeaderName),
                                equalTo(List.of(responseHeaderValue))
                            );
                            latch.countDown();
                        }
                    }
                );

                latch.await();
            }
        }
    }

    @TestLogging(value = "org.elasticsearch.cluster.service.MasterService:WARN", reason = "testing WARN logging")
    public void testStarvationLogging() throws Exception {
        final long warnThresholdMillis = MasterService.MASTER_SERVICE_STARVATION_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis();
        relativeTimeInMillis = randomLongBetween(0, Long.MAX_VALUE - warnThresholdMillis * 3);
        final long startTimeMillis = relativeTimeInMillis;
        final long taskDurationMillis = TimeValue.timeValueSeconds(1).millis();

        MockLogAppender mockAppender = new MockLogAppender();
        try (MasterService masterService = createMasterService(true); var ignored = mockAppender.capturing(MasterService.class)) {
            final AtomicBoolean keepRunning = new AtomicBoolean(true);

            final Runnable await = new Runnable() {
                private final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);

                @Override
                public void run() {
                    try {
                        cyclicBarrier.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                        throw new AssertionError("unexpected", e);
                    }
                }
            };
            final Runnable awaitNextTask = () -> {
                await.run();
                await.run();
            };

            final ClusterStateUpdateTask starvationCausingTask = new ClusterStateUpdateTask(Priority.HIGH) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    await.run();
                    relativeTimeInMillis += taskDurationMillis;
                    if (keepRunning.get()) {
                        masterService.submitUnbatchedStateUpdateTask("starvation-causing task", this);
                    }
                    await.run();
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            };
            masterService.submitUnbatchedStateUpdateTask("starvation-causing task", starvationCausingTask);

            final CountDownLatch starvedTaskExecuted = new CountDownLatch(1);
            masterService.submitUnbatchedStateUpdateTask("starved task", new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assertFalse(keepRunning.get());
                    starvedTaskExecuted.countDown();
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            });

            // check that a warning is logged after 5m
            final MockLogAppender.EventuallySeenEventExpectation expectation1 = new MockLogAppender.EventuallySeenEventExpectation(
                "starvation warning",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "pending task queue has been nonempty for [5m/300000ms] which is longer than the warn threshold of [300000ms];"
                    + " there are currently [2] pending tasks, the oldest of which has age [*"
            );
            mockAppender.addExpectation(expectation1);

            while (relativeTimeInMillis - startTimeMillis < warnThresholdMillis) {
                awaitNextTask.run();
                mockAppender.assertAllExpectationsMatched();
            }

            expectation1.setExpectSeen();
            awaitNextTask.run();
            // the master service thread is somewhere between completing the previous task and starting the next one, which is when the
            // logging happens, so we must wait for another task to run too to ensure that the message was logged
            awaitNextTask.run();
            mockAppender.assertAllExpectationsMatched();

            // check that another warning is logged after 10m
            final MockLogAppender.EventuallySeenEventExpectation expectation2 = new MockLogAppender.EventuallySeenEventExpectation(
                "starvation warning",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "pending task queue has been nonempty for [10m/600000ms] which is longer than the warn threshold of [300000ms];"
                    + " there are currently [2] pending tasks, the oldest of which has age [*"
            );
            mockAppender.addExpectation(expectation2);

            while (relativeTimeInMillis - startTimeMillis < warnThresholdMillis * 2) {
                awaitNextTask.run();
                mockAppender.assertAllExpectationsMatched();
            }

            expectation2.setExpectSeen();
            awaitNextTask.run();
            // the master service thread is somewhere between completing the previous task and starting the next one, which is when the
            // logging happens, so we must wait for another task to run too to ensure that the message was logged
            awaitNextTask.run();
            mockAppender.assertAllExpectationsMatched();

            // now stop the starvation and clean up
            keepRunning.set(false);
            awaitNextTask.run();
            assertTrue(starvedTaskExecuted.await(10, TimeUnit.SECONDS));
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.service:DEBUG",
        reason = "to ensure that we log the right batch description, which only happens at DEBUG level"
    )
    public void testBatchedUpdateSummaryLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        try (var ignored = mockAppender.capturing(MasterService.class); var masterService = createMasterService(true)) {

            final var barrier = new CyclicBarrier(2);
            final var blockingTask = new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    barrier.await(10, TimeUnit.SECONDS);
                    barrier.await(10, TimeUnit.SECONDS);
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            };

            class Task implements ClusterStateTaskListener {
                private final String description;

                Task(String description) {
                    this.description = description;
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }

                @Override
                public String toString() {
                    return description;
                }
            }

            class Executor implements ClusterStateTaskExecutor<Task> {

                final Semaphore semaphore = new Semaphore(0);

                @Override
                public ClusterState execute(BatchExecutionContext<Task> batchExecutionContext) {
                    for (final var taskContext : batchExecutionContext.taskContexts()) {
                        taskContext.success(() -> semaphore.release());
                    }
                    return batchExecutionContext.initialState();
                }
            }

            masterService.submitUnbatchedStateUpdateTask("block", blockingTask);
            barrier.await(10, TimeUnit.SECONDS);

            final var smallBatchExecutor = new Executor();
            final var smallBatchQueue = masterService.createTaskQueue("small-batch", Priority.NORMAL, smallBatchExecutor);
            for (int source = 0; source < 2; source++) {
                for (int task = 0; task < 2; task++) {
                    smallBatchQueue.submitTask("source-" + source, new Task("task-" + source + "-" + task), null);
                }
                mockAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "mention of tasks source-" + source,
                        MasterService.class.getCanonicalName(),
                        Level.DEBUG,
                        "executing cluster state update for [*source-" + source + "[task-" + source + "-0, task-" + source + "-1]*"
                    )
                );
            }

            final var manySourceExecutor = new Executor();
            final var manySourceQueue = masterService.createTaskQueue("many-source", Priority.NORMAL, manySourceExecutor);
            for (int source = 0; source < 1024; source++) {
                for (int task = 0; task < 2; task++) {
                    manySourceQueue.submitTask("source-" + source, new Task("task-" + task), null);
                }
            }
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "truncated description of batch with many sources",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "executing cluster state update for [* ... (1024 in total, *) (2048 tasks in total)]"
                ) {
                    @Override
                    public boolean innerMatch(LogEvent event) {
                        return event.getMessage().getFormattedMessage().length() < MAX_TASK_DESCRIPTION_CHARS + 200;
                    }
                }
            );

            final var manyTasksPerSourceExecutor = new Executor();
            final var manyTasksPerSourceQueue = masterService.createTaskQueue(
                "many-tasks-per-source",
                Priority.NORMAL,
                manyTasksPerSourceExecutor
            );
            for (int task = 0; task < 2048; task++) {
                manyTasksPerSourceQueue.submitTask("unique-source", new Task("task-" + task), null);
            }
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "truncated description of batch with many tasks from a single source",
                    MasterService.class.getCanonicalName(),
                    Level.DEBUG,
                    "executing cluster state update for [unique-source[task-0, task-1, task-2, task-3, task-4, * ... (2048 in total, *)]]"
                ) {
                    @Override
                    public boolean innerMatch(LogEvent event) {
                        return event.getMessage().getFormattedMessage().length() < 1500;
                    }
                }
            );

            barrier.await(10, TimeUnit.SECONDS);
            assertTrue(smallBatchExecutor.semaphore.tryAcquire(4, 10, TimeUnit.SECONDS));
            assertTrue(manySourceExecutor.semaphore.tryAcquire(2048, 10, TimeUnit.SECONDS));
            assertTrue(manyTasksPerSourceExecutor.semaphore.tryAcquire(2048, 10, TimeUnit.SECONDS));
            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testPendingTasksReporting() {

        final var deterministicTaskQueue = new DeterministicTaskQueue();

        final var threadPool = deterministicTaskQueue.getThreadPool();
        try (var masterService = createMasterService(true, null, threadPool, new StoppableExecutorServiceWrapper(threadPool.generic()))) {

            final var actionCount = new AtomicInteger();

            class BatchedTask implements ClusterStateTaskListener {
                final int queueIndex;
                final int taskIndex;
                final Priority priority;
                final long insertionTimeMillis;
                final TimeValue timeout;
                boolean isComplete;

                BatchedTask(int queueIndex, int taskIndex, Priority priority, long insertionTimeMillis, TimeValue timeout) {
                    this.queueIndex = queueIndex;
                    this.taskIndex = taskIndex;
                    this.priority = priority;
                    this.insertionTimeMillis = insertionTimeMillis;
                    this.timeout = timeout;
                }

                void assertPendingTaskEntry(boolean expectExecuting) {
                    assertFalse(isComplete);
                    final var pendingTaskEntry = getPendingTasks().stream()
                        .filter(t -> t.getInsertOrder() == taskIndex)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("task not found"));

                    assertEquals(getSource(), pendingTaskEntry.getSource().string());
                    assertEquals(expectExecuting, pendingTaskEntry.isExecuting());
                    assertEquals(priority, pendingTaskEntry.getPriority());
                    assertEquals(
                        deterministicTaskQueue.getCurrentTimeMillis() - insertionTimeMillis,
                        pendingTaskEntry.getTimeInQueueInMillis()
                    );
                    assertThat(pendingTaskEntry.getTimeInQueueInMillis(), lessThanOrEqualTo(masterService.getMaxTaskWaitTime().millis()));
                }

                private List<PendingClusterTask> getPendingTasks() {
                    final var pendingTasks = masterService.pendingTasks();
                    assertEquals(pendingTasks.size(), masterService.numberOfPendingTasks());
                    return pendingTasks;
                }

                void assertNoPendingTaskEntry() {
                    assertTrue(isComplete);
                    assertTrue(getPendingTasks().stream().noneMatch(t -> t.getInsertOrder() == taskIndex));
                }

                void onExecute() {
                    assertPendingTaskEntry(true);
                    actionCount.incrementAndGet();
                }

                void onSuccess() {
                    assertPendingTaskEntry(true);
                    actionCount.incrementAndGet();
                    isComplete = true;
                }

                String getSource() {
                    return "task-" + (queueIndex < 0 ? "unbatched" : Integer.toString(queueIndex)) + "-" + taskIndex;
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(ProcessClusterEventTimeoutException.class));
                    assertThat(e.getMessage(), equalTo("failed to process cluster event (" + getSource() + ") within " + timeout));
                    assertFalse(isComplete);
                    isComplete = true;
                    assertNoPendingTaskEntry();
                    actionCount.incrementAndGet();
                    actionCount.incrementAndGet();
                }

            }

            final var batchingPriorities = new ArrayList<Priority>();
            final var taskQueues = new ArrayList<MasterServiceTaskQueue<BatchedTask>>();
            for (int i = 0; i < 3; i++) {
                final var batchingPriority = randomFrom(Priority.values());
                batchingPriorities.add(batchingPriority);
                taskQueues.add(masterService.createTaskQueue("queue-" + i, batchingPriority, batchExecutionContext -> {
                    for (final var taskContext : batchExecutionContext.taskContexts()) {
                        final var task = taskContext.getTask();
                        task.onExecute();
                        taskContext.success(() -> {
                            deterministicTaskQueue.scheduleNow(task::assertNoPendingTaskEntry);
                            task.onSuccess();
                        });
                    }
                    return batchExecutionContext.initialState();
                }));
            }

            final var taskCount = between(1, 10);
            final var tasks = new ArrayList<BatchedTask>(taskCount);
            long firstTaskInsertTimeMillis = 0L;
            for (int i = 1; i <= taskCount; i++) {

                if (randomBoolean()) {
                    var targetTime = deterministicTaskQueue.getCurrentTimeMillis() + between(1, 30000);
                    deterministicTaskQueue.scheduleAt(targetTime, () -> {});

                    while (deterministicTaskQueue.getCurrentTimeMillis() < targetTime) {
                        deterministicTaskQueue.advanceTime();
                    }
                }
                if (i == 1) {
                    firstTaskInsertTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
                }

                final var queueIndex = between(-1, taskQueues.size() - 1);
                final var priority = queueIndex == -1 ? randomFrom(Priority.values()) : batchingPriorities.get(queueIndex);

                final var task = new BatchedTask(
                    queueIndex,
                    i,
                    priority,
                    deterministicTaskQueue.getCurrentTimeMillis(),
                    TimeValue.timeValueMillis(between(0, 30000))
                );
                tasks.add(task);

                if (queueIndex == -1) {
                    masterService.submitUnbatchedStateUpdateTask(task.getSource(), new ClusterStateUpdateTask(priority, task.timeout) {
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            task.onExecute();
                            return currentState;
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                            task.onSuccess();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            task.onFailure(e);
                        }
                    });
                } else {
                    taskQueues.get(queueIndex).submitTask(task.getSource(), task, task.timeout);
                }

                assertThat(
                    masterService.getMaxTaskWaitTime().millis(),
                    equalTo(deterministicTaskQueue.getCurrentTimeMillis() - firstTaskInsertTimeMillis)
                );
            }

            for (final var task : tasks) {
                task.assertPendingTaskEntry(false);
            }

            while (deterministicTaskQueue.hasDeferredTasks()) {
                deterministicTaskQueue.advanceTime();
            }

            for (final var task : tasks) {
                task.assertPendingTaskEntry(false);
            }

            threadPool.getThreadContext().markAsSystemContext();
            deterministicTaskQueue.runAllTasks();
            assertThat(actionCount.get(), equalTo(taskCount * 2));
            for (final var task : tasks) {
                task.assertNoPendingTaskEntry();
            }
            assertThat(masterService.getMaxTaskWaitTime(), equalTo(TimeValue.ZERO));
        }
    }

    public void testRejectionBehaviourAtSubmission() {

        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var threadPoolExecutor = EsExecutors.newScaling("Rejecting", 1, 1, 1, TimeUnit.SECONDS, true, r -> {
            throw new AssertionError("should not create new threads");
        }, threadPool.getThreadContext());
        threadPoolExecutor.shutdown();

        try (var masterService = createMasterService(true, null, threadPool, threadPoolExecutor)) {

            final var actionCount = new AtomicInteger();
            final var testHeader = "test-header";

            class TestTask implements ClusterStateTaskListener {
                private final String expectedHeader = threadPool.getThreadContext().getHeader(testHeader);

                @Override
                public void onFailure(Exception e) {
                    assertEquals(expectedHeader, threadPool.getThreadContext().getHeader(testHeader));
                    if ((e instanceof FailedToCommitClusterStateException
                        && e.getCause() instanceof EsRejectedExecutionException esre
                        && esre.isExecutorShutdown()) == false) {
                        throw new AssertionError("unexpected exception", e);
                    }
                    actionCount.incrementAndGet();
                }
            }

            final var queue = masterService.createTaskQueue("queue", randomFrom(Priority.values()), batchExecutionContext -> {
                throw new AssertionError("should not execute batch");
            });

            try (var ignored = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader(testHeader, randomAlphaOfLength(10));
                queue.submitTask("batched", new TestTask(), null);
            }
            try (var ignored = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader(testHeader, randomAlphaOfLength(10));
                masterService.submitUnbatchedStateUpdateTask("unbatched", new ClusterStateUpdateTask() {
                    private final TestTask innerTask = new TestTask();

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        throw new AssertionError("should not execute task");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        innerTask.onFailure(e);
                    }
                });
            }
            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertFalse(deterministicTaskQueue.hasDeferredTasks());
            assertEquals(2, actionCount.get());
        }
    }

    @TestLogging(reason = "verifying DEBUG logs", value = "org.elasticsearch.cluster.service.MasterService:DEBUG")
    public void testRejectionBehaviourAtCompletion() {

        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var threadPoolExecutor = new StoppableExecutorServiceWrapper(threadPool.generic()) {

            boolean executedTask = false;

            @Override
            public void execute(Runnable command) {
                if (command instanceof AbstractRunnable abstractRunnable) {
                    if (executedTask) {
                        abstractRunnable.onRejection(new EsRejectedExecutionException("simulated", true));
                    } else {
                        executedTask = true;
                        super.execute(command);
                    }
                } else {
                    fail("not an AbstractRunnable: " + command);
                }
            }
        };

        final var appender = new MockLogAppender();
        appender.addExpectation(
            new MockLogAppender.UnseenEventExpectation("warning", MasterService.class.getCanonicalName(), Level.WARN, "*")
        );
        appender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "debug",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "shut down during publication of cluster state version*"
            )
        );

        try (
            var ignored = appender.capturing(MasterService.class);
            var masterService = createMasterService(true, null, threadPool, threadPoolExecutor)
        ) {

            final var testHeader = "test-header";

            class TestTask implements ClusterStateTaskListener {
                private final String expectedHeader = threadPool.getThreadContext().getHeader(testHeader);

                @Override
                public void onFailure(Exception e) {
                    // post-publication rejections are currently just dropped, see https://github.com/elastic/elasticsearch/issues/94930
                    throw new AssertionError("unexpected exception", e);
                }
            }

            final var queue = masterService.createTaskQueue("queue", randomFrom(Priority.values()), batchExecutionContext -> {
                for (var taskContext : batchExecutionContext.taskContexts()) {
                    taskContext.success(() -> fail("should not succeed"));
                }
                return ClusterState.builder(batchExecutionContext.initialState()).build();
            });

            try (var ignoredContext = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader(testHeader, randomAlphaOfLength(10));
                queue.submitTask("batched", new TestTask(), null);
            }

            threadPool.getThreadContext().markAsSystemContext();
            deterministicTaskQueue.runAllTasks();
            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertFalse(deterministicTaskQueue.hasDeferredTasks());

            appender.assertAllExpectationsMatched();
        }
    }

    public void testLifecycleBehaviour() {

        final var deterministicTaskQueue = new DeterministicTaskQueue();

        final var threadPool = deterministicTaskQueue.getThreadPool();
        try (var masterService = createMasterService(true, null, threadPool, new StoppableExecutorServiceWrapper(threadPool.generic()))) {

            final var actionCount = new AtomicInteger();
            final var testHeader = "test-header";

            class TestTask implements ClusterStateTaskListener {
                private final String expectedHeader = threadPool.getThreadContext().getHeader(testHeader);

                @Override
                public void onFailure(Exception e) {
                    assertEquals(expectedHeader, threadPool.getThreadContext().getHeader(testHeader));
                    if ((e instanceof FailedToCommitClusterStateException
                        && e.getCause() instanceof EsRejectedExecutionException esre
                        && esre.isExecutorShutdown()) == false) {
                        throw new AssertionError("unexpected exception", e);
                    }
                    actionCount.incrementAndGet();
                }
            }

            final var queue = masterService.createTaskQueue("queue", randomFrom(Priority.values()), batchExecutionContext -> {
                throw new AssertionError("should not execute batch");
            });

            while (true) {
                try (var ignored = threadPool.getThreadContext().stashContext()) {
                    threadPool.getThreadContext().putHeader(testHeader, randomAlphaOfLength(10));
                    queue.submitTask("batched", new TestTask(), null);
                }
                try (var ignored = threadPool.getThreadContext().stashContext()) {
                    threadPool.getThreadContext().putHeader(testHeader, randomAlphaOfLength(10));
                    masterService.submitUnbatchedStateUpdateTask("unbatched", new ClusterStateUpdateTask() {
                        private final TestTask innerTask = new TestTask();

                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            throw new AssertionError("should not execute task");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            innerTask.onFailure(e);
                        }
                    });
                }

                if (masterService.lifecycleState() == Lifecycle.State.STARTED) {
                    masterService.close();
                } else {
                    break;
                }
            }

            threadPool.getThreadContext().markAsSystemContext();
            deterministicTaskQueue.runAllTasks();
            assertEquals(4, actionCount.get());
        }
    }

    public void testTimeoutBehaviour() {

        final var deterministicTaskQueue = new DeterministicTaskQueue();

        final var threadPool = deterministicTaskQueue.getThreadPool();
        try (var masterService = createMasterService(true, null, threadPool, new StoppableExecutorServiceWrapper(threadPool.generic()))) {

            final var actionCount = new AtomicInteger();
            final var testHeader = "test-header";

            class BlockingTask extends ClusterStateUpdateTask {
                BlockingTask() {
                    super(Priority.IMMEDIATE);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    var targetTime = deterministicTaskQueue.getCurrentTimeMillis() + between(1, 1000);
                    deterministicTaskQueue.scheduleAt(targetTime, () -> {});

                    while (deterministicTaskQueue.getCurrentTimeMillis() < targetTime) {
                        deterministicTaskQueue.advanceTime();
                    }

                    return currentState;
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    if (actionCount.get() < 2) {
                        masterService.submitUnbatchedStateUpdateTask("blocker", BlockingTask.this);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("unexpected", e);
                }
            }

            masterService.submitUnbatchedStateUpdateTask("blocker", new BlockingTask());

            class TestTask implements ClusterStateTaskListener {
                private final String expectedHeader = threadPool.getThreadContext().getHeader(testHeader);
                private final TimeValue timeout;

                TestTask(TimeValue timeout) {
                    this.timeout = timeout;
                }

                @Override
                public void onFailure(Exception e) {
                    assertEquals(expectedHeader, threadPool.getThreadContext().getHeader(testHeader));
                    assertThat(deterministicTaskQueue.getCurrentTimeMillis(), greaterThanOrEqualTo(timeout.millis()));
                    assertThat(e, instanceOf(ProcessClusterEventTimeoutException.class));
                    assertThat(
                        e.getMessage(),
                        allOf(containsString("failed to process cluster event"), containsString(timeout.toString()))
                    );
                    actionCount.incrementAndGet();
                }
            }

            final var queue = masterService.createTaskQueue("queue", Priority.NORMAL, batchExecutionContext -> {
                throw new AssertionError("should not execute batch");
            });

            try (var ignored = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader(testHeader, randomAlphaOfLength(10));
                final var testTask = new TestTask(TimeValue.timeValueMillis(between(1, 30000)));
                queue.submitTask("batched", testTask, testTask.timeout);
            }

            try (var ignored = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader(testHeader, randomAlphaOfLength(10));
                final var innerTask = new TestTask(TimeValue.timeValueMillis(between(1, 30000)));
                masterService.submitUnbatchedStateUpdateTask("unbatched", new ClusterStateUpdateTask(innerTask.timeout) {

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        throw new AssertionError("should not execute task");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        innerTask.onFailure(e);
                    }
                });
            }

            threadPool.getThreadContext().markAsSystemContext();
            deterministicTaskQueue.runAllTasks();
            assertEquals(2, actionCount.get());
        }
    }

    public void testReleaseOnTimeout() {

        final var deterministicTaskQueue = new DeterministicTaskQueue();

        final var threadPool = deterministicTaskQueue.getThreadPool();
        try (var masterService = createMasterService(true, null, threadPool, new StoppableExecutorServiceWrapper(threadPool.generic()))) {

            final var actionCount = new AtomicInteger();

            class BlockingTask extends ClusterStateUpdateTask {
                BlockingTask() {
                    super(Priority.IMMEDIATE);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    var targetTime = deterministicTaskQueue.getCurrentTimeMillis() + between(1, 1000);
                    deterministicTaskQueue.scheduleAt(targetTime, () -> {});

                    while (deterministicTaskQueue.getCurrentTimeMillis() < targetTime) {
                        deterministicTaskQueue.advanceTime();
                    }

                    return currentState;
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    if (actionCount.get() < 1) {
                        masterService.submitUnbatchedStateUpdateTask("blocker", BlockingTask.this);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("unexpected", e);
                }
            }

            masterService.submitUnbatchedStateUpdateTask("blocker", new BlockingTask());

            final var queue = masterService.createTaskQueue("queue", Priority.NORMAL, batchExecutionContext -> {
                assertEquals(1, batchExecutionContext.taskContexts().size());
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    taskContext.success(actionCount::incrementAndGet);
                }
                return batchExecutionContext.initialState();
            });

            final var reachabilityChecker = new ReachabilityChecker();

            class TestTask implements ClusterStateTaskListener {
                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(ProcessClusterEventTimeoutException.class));
                    deterministicTaskQueue.scheduleNow(() -> {
                        reachabilityChecker.ensureUnreachable();
                        actionCount.incrementAndGet();
                    });
                }
            }

            final var timeout = TimeValue.timeValueMillis(between(1, 30000));
            queue.submitTask("will timeout", reachabilityChecker.register(new TestTask()), timeout);
            queue.submitTask("no timeout", new TestTask(), null);

            threadPool.getThreadContext().markAsSystemContext();
            deterministicTaskQueue.runAllTasks();
            assertEquals(2, actionCount.get());
        }
    }

    public void testPrioritization() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        try (var masterService = createMasterService(true, null, threadPool, new StoppableExecutorServiceWrapper(threadPool.generic()))) {

            // specify the order in which the priorities should run, rather than relying on their enum order which would be easy to reverse
            final var prioritiesOrder = List.of(
                Priority.IMMEDIATE,
                Priority.URGENT,
                Priority.HIGH,
                Priority.NORMAL,
                Priority.LOW,
                Priority.LANGUID
            );
            final var prioritiesQueue = new ArrayDeque<>(prioritiesOrder);

            final var simpleExecutor = new SimpleBatchedExecutor<ClusterStateUpdateTask, Void>() {
                @Override
                public Tuple<ClusterState, Void> executeTask(ClusterStateUpdateTask task, ClusterState clusterState) throws Exception {
                    return Tuple.tuple(task.execute(clusterState), null);
                }

                @Override
                public void taskSucceeded(ClusterStateUpdateTask clusterStateTaskListener, Void result) {}
            };

            final var queues = new EnumMap<Priority, MasterServiceTaskQueue<ClusterStateUpdateTask>>(Priority.class);
            final var tasks = new ArrayList<ClusterStateUpdateTask>();
            for (final var priority : Priority.values()) {
                queues.put(priority, masterService.createTaskQueue(priority.name(), priority, simpleExecutor));
                tasks.add(new ClusterStateUpdateTask(priority) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        assertEquals(priority, prioritiesQueue.poll());
                        assertEquals(priority, priority());
                        return randomBoolean() ? currentState : ClusterState.builder(currentState).build();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError("unexpected", e);
                    }
                });
            }

            Randomness.shuffle(tasks);
            for (final var task : tasks) {
                if (randomBoolean()) {
                    queues.get(task.priority()).submitTask("test", task, null);
                } else {
                    masterService.submitUnbatchedStateUpdateTask("test", task);
                }
            }

            assertEquals(
                prioritiesOrder,
                masterService.pendingTasks().stream().map(PendingClusterTask::priority).collect(Collectors.toList())
            );

            threadPool.getThreadContext().markAsSystemContext();
            deterministicTaskQueue.runAllTasks();
            assertThat(prioritiesQueue, empty());
        }
    }

    /**
     * Returns the cluster state that the master service uses (and that is provided by the discovery layer)
     */
    public static ClusterState discoveryState(MasterService masterService) {
        return masterService.state();
    }

    /**
     * Returns a plain {@link AckedRequest} that does not implement any functionality outside of the timeout getters.
     */
    public static AckedRequest ackedRequest(TimeValue ackTimeout, TimeValue masterNodeTimeout) {
        return new AckedRequest() {
            @Override
            public TimeValue ackTimeout() {
                return ackTimeout;
            }

            @Override
            public TimeValue masterNodeTimeout() {
                return masterNodeTimeout;
            }
        };
    }

    /**
     * Task that asserts it does not fail.
     */
    private static class ExpectSuccessTask implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            throw new AssertionError("should not be called", e);
        }
    }
}
