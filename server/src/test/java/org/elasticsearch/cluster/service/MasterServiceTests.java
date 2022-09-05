/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalMasterServiceTask;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.BaseFuture;
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
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.test.tasks.MockTaskManagerListener;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

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
        final DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
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
        );
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
            publishListener.onResponse(null);
        });
        masterService.setClusterStateSupplier(clusterStateRef::get);
        masterService.start();
        return masterService;
    }

    public void testMasterAwareExecution() throws Exception {
        final MasterService nonMaster = createMasterService(false);

        final boolean[] taskFailed = { false };
        final CountDownLatch latch1 = new CountDownLatch(1);
        nonMaster.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                latch1.countDown();
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                taskFailed[0] = true;
                latch1.countDown();
            }
        });

        latch1.await();
        assertTrue("cluster state update task was executed on a non-master", taskFailed[0]);

        final CountDownLatch latch2 = new CountDownLatch(1);
        new LocalMasterServiceTask(Priority.NORMAL) {
            @Override
            public void execute(ClusterState currentState) {
                taskFailed[0] = false;
                latch2.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                taskFailed[0] = true;
                latch2.countDown();
            }
        }.submit(nonMaster, "test");
        latch2.await();
        assertFalse("non-master cluster state update task was not executed", taskFailed[0]);

        nonMaster.close();
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
            masterService.submitStateUpdateTask(
                "testCreatesChildTaskForPublishingClusterState",
                new ExpectSuccessTask(),
                ClusterStateTaskConfig.build(Priority.NORMAL),
                new ClusterStateTaskExecutor<>() {
                    @Override
                    public ClusterState execute(BatchExecutionContext<ExpectSuccessTask> batchExecutionContext) {
                        for (final var taskContext : batchExecutionContext.taskContexts()) {
                            taskContext.success(() -> {});
                        }
                        return ClusterState.builder(batchExecutionContext.initialState()).build();
                    }

                    @Override
                    public void clusterStatePublished(ClusterState newClusterState) {
                        latch.countDown();
                    }
                }
            );

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        assertThat(registeredActions.toString(), registeredActions, contains(MasterService.STATE_UPDATE_ACTION_NAME));
    }

    public void testThreadContext() throws InterruptedException {
        final MasterService master = createMasterService(true);
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

            master.submitUnbatchedStateUpdateTask("test", new AckedClusterStateUpdateTask(ackedRequest(ackTimeout, masterTimeout), null) {
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

            });

            assertFalse(threadPool.getThreadContext().isSystemContext());
            assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
            assertEquals(Collections.emptyMap(), threadPool.getThreadContext().getResponseHeaders());
        }

        latch.await();

        master.close();
    }

    /*
    * test that a listener throwing an exception while handling a
    * notification does not prevent publication notification to the
    * executor
    */
    public void testClusterStateTaskListenerThrowingExceptionIsOkay() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        try (MasterService masterService = createMasterService(true)) {
            masterService.submitStateUpdateTask(
                "testClusterStateTaskListenerThrowingExceptionIsOkay",
                new ExpectSuccessTask(),
                ClusterStateTaskConfig.build(Priority.NORMAL),
                new ClusterStateTaskExecutor<>() {
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
            );

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }

    @TestLogging(value = "org.elasticsearch.cluster.service:TRACE", reason = "to ensure that we log cluster state events on TRACE level")
    public void testClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
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

        Logger clusterLogger = LogManager.getLogger(MasterService.class);
        Loggers.addAppender(clusterLogger, mockAppender);
        try (MasterService masterService = createMasterService(true)) {
            masterService.submitUnbatchedStateUpdateTask("test1", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += TimeValue.timeValueSeconds(1).millis();
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {}

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
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {}

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            });
            assertBusy(mockAppender::assertAllExpectationsMatched);
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
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

        final var executors = new Executor[executorCount];
        for (int i = 0; i < executors.length; i++) {
            executors[i] = new Executor();
        }

        try (var masterService = createMasterService(true)) {

            final var executionBarrier = new CyclicBarrier(2);

            masterService.submitStateUpdateTask(
                "block",
                new ExpectSuccessTask(),
                ClusterStateTaskConfig.build(Priority.NORMAL),
                batchExecutionContext -> {
                    executionBarrier.await(10, TimeUnit.SECONDS); // notify test thread that the master service is blocked
                    executionBarrier.await(10, TimeUnit.SECONDS); // wait for test thread to release us
                    for (final var taskContext : batchExecutionContext.taskContexts()) {
                        taskContext.success(() -> {});
                    }
                    return batchExecutionContext.initialState();
                }
            );

            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked

            final var submissionLatch = new CountDownLatch(1);

            final var submitThreads = new Thread[between(1, 10)];
            for (int i = 0; i < submitThreads.length; i++) {
                final var executor = randomFrom(executors);
                final var task = new ExpectSuccessTask();
                executor.addExpectedTaskCount(1);
                submitThreads[i] = new Thread(() -> {
                    try {
                        assertTrue(submissionLatch.await(10, TimeUnit.SECONDS));
                        masterService.submitStateUpdateTask(
                            Thread.currentThread().getName(),
                            task,
                            ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                            executor
                        );
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }

                }, "submit-thread-" + i);
            }

            for (var executor : executors) {
                if (executor.expectedTaskCount == 0) {
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
                assertFalse(executor.executed.get());
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
                assertTrue(executor.executed.get() != (executor.expectedTaskCount == 0));
            }
        }
    }

    public void testClusterStateBatchedUpdates() throws BrokenBarrierException, InterruptedException {

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

        List<TaskExecutor> executors = new ArrayList<>();
        for (int i = 0; i < numberOfExecutors; i++) {
            executors.add(new TaskExecutor());
        }

        // randomly assign tasks to executors
        List<Tuple<TaskExecutor, Task>> assignments = new ArrayList<>();
        AtomicInteger totalTasks = new AtomicInteger();
        for (int i = 0; i < numberOfThreads; i++) {
            for (int j = 0; j < taskSubmissionsPerThread; j++) {
                var executor = randomFrom(executors);
                var task = new Task(totalTasks.getAndIncrement());

                assignments.add(Tuple.tuple(executor, task));
                executor.assigned.incrementAndGet();
                executor.assignments.add(task);
            }
        }
        processedStatesLatch.set(new CountDownLatch(totalTasks.get()));

        try (MasterService masterService = createMasterService(true)) {
            CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
            for (int i = 0; i < numberOfThreads; i++) {
                final int index = i;
                Thread thread = new Thread(() -> {
                    final String threadName = Thread.currentThread().getName();
                    try {
                        barrier.await();
                        for (int j = 0; j < taskSubmissionsPerThread; j++) {
                            var assignment = assignments.get(index * taskSubmissionsPerThread + j);
                            var task = assignment.v2();
                            var executor = assignment.v1();
                            submittedTasks.incrementAndGet();
                            masterService.submitStateUpdateTask(
                                threadName,
                                task,
                                ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                                executor
                            );
                        }
                        barrier.await();
                    } catch (BrokenBarrierException | InterruptedException e) {
                        throw new AssertionError(e);
                    }
                });
                thread.start();
            }

            // wait for all threads to be ready
            barrier.await();
            // wait for all threads to finish
            barrier.await();

            // wait until all the cluster state updates have been processed
            processedStatesLatch.get().await();
            // and until all the publication callbacks have completed
            semaphore.acquire();

            // assert the number of executed tasks is correct
            assertThat(submittedTasks.get(), equalTo(totalTasks.get()));
            assertThat(executedTasks.get(), equalTo(totalTasks.get()));
            assertThat(processedStates.get(), equalTo(totalTasks.get()));

            // assert each executor executed the correct number of tasks
            for (TaskExecutor executor : executors) {
                assertEquals(executor.assigned.get(), executor.executed.get());
                assertEquals(executor.batches.get(), executor.published.get());
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

            masterService.submitUnbatchedStateUpdateTask("block", blockMasterTask);
            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked

            masterService.setClusterStatePublisher(
                (clusterStatePublicationEvent, publishListener, ackListener) -> {
                    throw new AssertionError("should not publish any states");
                }
            );

            for (int i = 0; i < taskCount; i++) {
                try (ThreadContext.StoredContext ignored = threadContext.newStoredContext()) {
                    final String testContextHeaderValue = randomAlphaOfLength(10);
                    threadContext.putHeader(testContextHeaderName, testContextHeaderValue);
                    final var task = new Task(testContextHeaderValue);
                    final var clusterStateTaskConfig = ClusterStateTaskConfig.build(Priority.NORMAL);
                    masterService.submitStateUpdateTask("test", task, clusterStateTaskConfig, executor);
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

            Task(String responseHeaderValue, ActionListener<ClusterState> publishListener) {
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

        final var executor = new ClusterStateTaskExecutor<Task>() {
            @Override
            @SuppressForbidden(reason = "consuming published cluster state for legacy reasons")
            public ClusterState execute(BatchExecutionContext<Task> batchExecutionContext) {
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    try (var ignored = taskContext.captureResponseHeaders()) {
                        threadPool.getThreadContext().addResponseHeader(testResponseHeaderName, taskContext.getTask().responseHeaderValue);
                    }
                    taskContext.success(taskContext.getTask().publishListener::onResponse);
                }
                return ClusterState.builder(batchExecutionContext.initialState()).build();
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

            // success case: submit some tasks, possibly in different contexts, and verify that the expected listener is completed

            masterService.submitUnbatchedStateUpdateTask("block", blockMasterTask);
            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked

            final AtomicReference<ClusterState> publishedState = new AtomicReference<>();
            masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
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
                    final var task = new Task(testResponseHeaderValue, new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterState clusterState) {
                            assertEquals(testContextHeaderValue, threadContext.getHeader(testContextHeaderName));
                            assertEquals(List.of(testResponseHeaderValue), threadContext.getResponseHeaders().get(testResponseHeaderName));
                            assertSame(publishedState.get(), clusterState);
                            publishSuccessCountdown.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new AssertionError(e);
                        }
                    });

                    final ClusterStateTaskConfig clusterStateTaskConfig = ClusterStateTaskConfig.build(Priority.NORMAL);
                    masterService.submitStateUpdateTask("test", task, clusterStateTaskConfig, executor);
                }
            }

            executionBarrier.await(10, TimeUnit.SECONDS); // release block on master service
            assertTrue(publishSuccessCountdown.await(10, TimeUnit.SECONDS));

            // failure case: submit some tasks, possibly in different contexts, and verify that the expected listener is completed

            masterService.submitUnbatchedStateUpdateTask("block", blockMasterTask);
            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked

            final String exceptionMessage = "simulated";
            masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
                ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
                publishListener.onFailure(new FailedToCommitClusterStateException(exceptionMessage));
            });

            toSubmit = between(1, 10);
            final CountDownLatch publishFailureCountdown = new CountDownLatch(toSubmit);

            for (int i = 0; i < toSubmit; i++) {
                try (ThreadContext.StoredContext ignored = threadContext.newStoredContext()) {
                    final String testContextHeaderValue = randomAlphaOfLength(10);
                    final String testResponseHeaderValue = randomAlphaOfLength(10);
                    threadContext.putHeader(testContextHeaderName, testContextHeaderValue);
                    final var task = new Task(testResponseHeaderValue, new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterState clusterState) {
                            throw new AssertionError("should not succeed");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertEquals(testContextHeaderValue, threadContext.getHeader(testContextHeaderName));
                            assertEquals(List.of(testResponseHeaderValue), threadContext.getResponseHeaders().get(testResponseHeaderName));
                            assertThat(e, instanceOf(FailedToCommitClusterStateException.class));
                            assertThat(e.getMessage(), equalTo(exceptionMessage));
                            publishFailureCountdown.countDown();
                        }
                    });

                    final ClusterStateTaskConfig clusterStateTaskConfig = ClusterStateTaskConfig.build(Priority.NORMAL);
                    masterService.submitStateUpdateTask("test", task, clusterStateTaskConfig, executor);
                }
            }

            executionBarrier.await(10, TimeUnit.SECONDS); // release block on master service
            assertTrue(publishFailureCountdown.await(10, TimeUnit.SECONDS));
        }
    }

    public void testBlockingCallInClusterStateTaskListenerFails() throws InterruptedException {
        assumeTrue("assertions must be enabled for this test to work", BaseFuture.class.desiredAssertionStatus());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AssertionError> assertionRef = new AtomicReference<>();

        try (MasterService masterService = createMasterService(true)) {
            masterService.submitStateUpdateTask(
                "testBlockingCallInClusterStateTaskListenerFails",
                new ExpectSuccessTask(),
                ClusterStateTaskConfig.build(Priority.NORMAL),
                batchExecutionContext -> {
                    for (final var taskContext : batchExecutionContext.taskContexts()) {
                        taskContext.success(() -> {
                            BaseFuture<Void> future = new BaseFuture<Void>() {
                            };
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
                }
            );

            latch.await();
            assertNotNull(assertionRef.get());
            assertThat(assertionRef.get().getMessage(), containsString("Reason: [Blocking operation]"));
        }
    }

    @TestLogging(value = "org.elasticsearch.cluster.service:WARN", reason = "to ensure that we log cluster state events on WARN level")
    public void testLongClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
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

        Logger clusterLogger = LogManager.getLogger(MasterService.class);
        Loggers.addAppender(clusterLogger, mockAppender);
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
            final DiscoveryNode localNode = new DiscoveryNode(
                "node1",
                buildNewFakeTransportAddress(),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            );
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
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
        mockAppender.assertAllExpectationsMatched();
    }

    public void testAcking() throws InterruptedException {
        final DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final DiscoveryNode node3 = new DiscoveryNode("node3", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
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

                masterService.submitStateUpdateTask(
                    "success-test",
                    new Task(),
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    batchExecutionContext -> {
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
                    }
                );

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

                masterService.submitStateUpdateTask(
                    "success-test",
                    new Task(),
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    batchExecutionContext -> {
                        for (final var taskContext : batchExecutionContext.taskContexts()) {
                            taskContext.success(latch::countDown, new LatchAckListener(latch));
                        }
                        return randomBoolean()
                            ? batchExecutionContext.initialState()
                            : ClusterState.builder(batchExecutionContext.initialState()).build();
                    }
                );

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

                masterService.submitStateUpdateTask(
                    "success-test",
                    new Task(),
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    batchExecutionContext -> {
                        for (final var taskContext : batchExecutionContext.taskContexts()) {
                            taskContext.success(new LatchAckListener(latch));
                        }
                        return randomBoolean()
                            ? batchExecutionContext.initialState()
                            : ClusterState.builder(batchExecutionContext.initialState()).build();
                    }
                );

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

                masterService.submitStateUpdateTask(
                    "node-ack-fail-test",
                    new Task(),
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    batchExecutionContext -> {
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
                    }
                );

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
        mockAppender.start();

        Logger clusterLogger = LogManager.getLogger(MasterService.class);
        Loggers.addAppender(clusterLogger, mockAppender);
        try (MasterService masterService = createMasterService(true)) {
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
                    fail();
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
                    fail();
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

        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.service:DEBUG",
        reason = "to ensure that we log the right batch description, which only happens at DEBUG level"
    )
    public void testBatchedUpdateSummaryLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();

        Logger masterServiceLogger = LogManager.getLogger(MasterService.class);
        Loggers.addAppender(masterServiceLogger, mockAppender);
        try (MasterService masterService = createMasterService(true)) {

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
            for (int source = 0; source < 2; source++) {
                for (int task = 0; task < 2; task++) {
                    masterService.submitStateUpdateTask(
                        "source-" + source,
                        new Task("task-" + task),
                        ClusterStateTaskConfig.build(Priority.NORMAL),
                        smallBatchExecutor
                    );
                }
                mockAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "mention of tasks source-" + source,
                        MasterService.class.getCanonicalName(),
                        Level.DEBUG,
                        "executing cluster state update for [*source-" + source + "[task-0, task-1]*"
                    )
                );
            }

            final var manySourceExecutor = new Executor();
            for (int source = 0; source < 1024; source++) {
                for (int task = 0; task < 2; task++) {
                    masterService.submitStateUpdateTask(
                        "source-" + source,
                        new Task("task-" + task),
                        ClusterStateTaskConfig.build(Priority.NORMAL),
                        manySourceExecutor
                    );
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
                        return event.getMessage().getFormattedMessage().length() < BatchSummary.MAX_TASK_DESCRIPTION_CHARS + 200;
                    }
                }
            );

            final var manyTasksPerSourceExecutor = new Executor();
            for (int task = 0; task < 2048; task++) {
                masterService.submitStateUpdateTask(
                    "unique-source",
                    new Task("task-" + task),
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    manyTasksPerSourceExecutor
                );
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
        } finally {
            Loggers.removeAppender(masterServiceLogger, mockAppender);
            mockAppender.stop();
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
