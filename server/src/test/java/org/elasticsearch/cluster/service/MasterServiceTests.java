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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
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
        final DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final MasterService masterService = new MasterService(
            Settings.builder()
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
                .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
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
        nonMaster.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
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
        }, ClusterStateTaskExecutor.unbatched());

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

            master.submitStateUpdateTask("test", new AckedClusterStateUpdateTask(ackedRequest(ackTimeout, masterTimeout), null) {
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

            }, ClusterStateTaskExecutor.unbatched());

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
                    public ClusterState execute(ClusterState currentState, List<TaskContext<ExpectSuccessTask>> taskContexts) {
                        for (final var taskContext : taskContexts) {
                            taskContext.success(
                                EXPECT_SUCCESS_LISTENER.delegateFailure(
                                    (delegate, cs) -> { throw new RuntimeException("testing exception handling"); }
                                )
                            );
                        }
                        return ClusterState.builder(currentState).build();
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
            masterService.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
            masterService.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
            masterService.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
            masterService.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
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
            public ClusterState execute(ClusterState currentState, List<TaskContext<ExpectSuccessTask>> taskContexts) {
                assertTrue("Should execute all tasks at once", executed.compareAndSet(false, true));
                assertThat("Should execute all tasks at once", taskContexts.size(), equalTo(expectedTaskCount));
                executionCountDown.countDown();
                for (final var taskContext : taskContexts) {
                    taskContext.success(EXPECT_SUCCESS_LISTENER);
                }
                return currentState;
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
                (currentState, taskContexts) -> {
                    executionBarrier.await(10, TimeUnit.SECONDS); // notify test thread that the master service is blocked
                    executionBarrier.await(10, TimeUnit.SECONDS); // wait for test thread to release us
                    for (final var taskContext : taskContexts) {
                        taskContext.success(EXPECT_SUCCESS_LISTENER);
                    }
                    return currentState;
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

        class Task implements ClusterStateTaskListener {
            private final AtomicBoolean executed = new AtomicBoolean();
            private final int id;

            Task(int id) {
                this.id = id;
            }

            public void execute() {
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
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                throw new AssertionError("should not be called");
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
            public ClusterState execute(ClusterState currentState, List<TaskContext<Task>> taskContexts) {
                for (final var taskContext : taskContexts) {
                    assertThat("All tasks should belong to this executor", assignments, hasItem(taskContext.getTask()));
                }

                for (final var taskContext : taskContexts) {
                    taskContext.getTask().execute();
                }

                executed.addAndGet(taskContexts.size());
                ClusterState maybeUpdatedClusterState = currentState;
                if (randomBoolean()) {
                    maybeUpdatedClusterState = ClusterState.builder(currentState).build();
                    batches.incrementAndGet();
                    assertThat(
                        "All cluster state modifications should be executed on a single thread",
                        semaphore.tryAcquire(),
                        equalTo(true)
                    );
                }

                for (final var taskContext : taskContexts) {
                    taskContext.success(new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterState clusterState) {
                            processedStates.incrementAndGet();
                            processedStatesLatch.get().countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new AssertionError("should not be called", e);
                        }
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
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                throw new AssertionError("should not complete task");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(RuntimeException.class));
                assertThat(e.getMessage(), equalTo("simulated"));
                assertThat(threadContext.getHeader(testContextHeaderName), equalTo(expectedHeaderValue));
                taskCountDown.countDown();
            }
        }

        final ClusterStateTaskExecutor<Task> executor = (currentState, taskContexts) -> {
            if (randomBoolean()) {
                throw new RuntimeException("simulated");
            } else {
                for (final var taskContext : taskContexts) {
                    taskContext.onFailure(new RuntimeException("simulated"));
                }
                return currentState;
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

            masterService.submitStateUpdateTask("block", blockMasterTask, ClusterStateTaskExecutor.unbatched());
            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked

            masterService.setClusterStatePublisher(
                (clusterStatePublicationEvent, publishListener, ackListener) -> {
                    throw new AssertionError("should not publish any states");
                }
            );

            for (int i = 0; i < taskCount; i++) {
                try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(false)) {
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

            Task(ActionListener<ClusterState> publishListener) {
                this.publishListener = publishListener;
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                throw new AssertionError("should not complete task");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        }

        final String testContextHeaderName = "test-context-header";
        final ThreadContext threadContext = threadPool.getThreadContext();

        final ClusterStateTaskExecutor<Task> executor = (currentState, taskContexts) -> {
            for (final var taskContext : taskContexts) {
                taskContext.success(taskContext.getTask().publishListener);
            }
            return ClusterState.builder(currentState).build();
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

            masterService.submitStateUpdateTask("block", blockMasterTask, ClusterStateTaskExecutor.unbatched());
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
                try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(false)) {
                    final var testContextHeaderValue = randomAlphaOfLength(10);
                    threadContext.putHeader(testContextHeaderName, testContextHeaderValue);
                    final var task = new Task(new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterState clusterState) {
                            assertEquals(testContextHeaderValue, threadContext.getHeader(testContextHeaderName));
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

            masterService.submitStateUpdateTask("block", blockMasterTask, ClusterStateTaskExecutor.unbatched());
            executionBarrier.await(10, TimeUnit.SECONDS); // wait for the master service to be blocked

            final String exceptionMessage = "simulated";
            masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
                ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
                publishListener.onFailure(new FailedToCommitClusterStateException(exceptionMessage));
            });

            toSubmit = between(1, 10);
            final CountDownLatch publishFailureCountdown = new CountDownLatch(toSubmit);

            for (int i = 0; i < toSubmit; i++) {
                try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(false)) {
                    final String testContextHeaderValue = randomAlphaOfLength(10);
                    threadContext.putHeader(testContextHeaderName, testContextHeaderValue);
                    final var task = new Task(new ActionListener<>() {
                        @Override
                        public void onResponse(ClusterState clusterState) {
                            throw new AssertionError("should not succeed");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertEquals(testContextHeaderValue, threadContext.getHeader(testContextHeaderName));
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
                (currentState, taskContexts) -> {
                    for (final var taskContext : taskContexts) {
                        taskContext.success(EXPECT_SUCCESS_LISTENER.delegateFailure((delegate, cs) -> {
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
                        }));
                    }
                    return ClusterState.builder(currentState).build();
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
        try (
            MasterService masterService = new MasterService(
                Settings.builder()
                    .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
                    .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
                    .build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool
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
                if (clusterStatePublicationEvent.getSummary().contains("test5")) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                }
                if (clusterStatePublicationEvent.getSummary().contains("test6")) {
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
            masterService.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());

            processedFirstTask.await();
            masterService.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
            masterService.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
            masterService.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
            masterService.submitStateUpdateTask("test5", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
            masterService.submitStateUpdateTask("test6", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            masterService.submitStateUpdateTask("test7", new ClusterStateUpdateTask() {
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
            }, ClusterStateTaskExecutor.unbatched());
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
        try (
            MasterService masterService = new MasterService(
                Settings.builder()
                    .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
                    .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
                    .build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool
            )
        ) {

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

                    @Override
                    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                        fail();
                    }
                }

                masterService.submitStateUpdateTask(
                    "success-test",
                    new Task(),
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    (currentState, taskContexts) -> {
                        for (final var taskContext : taskContexts) {
                            taskContext.success(new ActionListener<>() {
                                @Override
                                public void onResponse(ClusterState clusterState) {
                                    latch.countDown();
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    throw new AssertionError(e);
                                }
                            }, taskContext.getTask());
                        }
                        return randomBoolean() ? currentState : ClusterState.builder(currentState).build();
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

                    @Override
                    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                        fail();
                    }
                }

                masterService.submitStateUpdateTask(
                    "success-test",
                    new Task(),
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    (currentState, taskContexts) -> {
                        for (final var taskContext : taskContexts) {
                            taskContext.success(new ActionListener<>() {
                                @Override
                                public void onResponse(ClusterState clusterState) {
                                    latch.countDown();
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    throw new AssertionError(e);
                                }
                            }, new LatchAckListener(latch));
                        }
                        return randomBoolean() ? currentState : ClusterState.builder(currentState).build();
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

                    @Override
                    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                        fail();
                    }
                }

                masterService.submitStateUpdateTask(
                    "success-test",
                    new Task(),
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    (currentState, taskContexts) -> {
                        for (final var taskContext : taskContexts) {
                            taskContext.success(new LatchAckListener(latch));
                        }
                        return randomBoolean() ? currentState : ClusterState.builder(currentState).build();
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

                masterService.submitStateUpdateTask("test2", new AckedClusterStateUpdateTask(ackedRequest(TimeValue.ZERO, null), null) {
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
                }, ClusterStateTaskExecutor.unbatched());

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

                masterService.submitStateUpdateTask("test2", new AckedClusterStateUpdateTask(ackedRequest(ackTimeout, null), null) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
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
                        latch.countDown();
                    }
                }, ClusterStateTaskExecutor.unbatched());

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
                        masterService.submitStateUpdateTask("starvation-causing task", this, ClusterStateTaskExecutor.unbatched());
                    }
                    await.run();
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            };
            masterService.submitStateUpdateTask("starvation-causing task", starvationCausingTask, ClusterStateTaskExecutor.unbatched());

            final CountDownLatch starvedTaskExecuted = new CountDownLatch(1);
            masterService.submitStateUpdateTask("starved task", new ClusterStateUpdateTask(Priority.NORMAL) {
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
            }, ClusterStateTaskExecutor.unbatched());

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
     * Listener that asserts it does not fail.
     */
    private static final ActionListener<ClusterState> EXPECT_SUCCESS_LISTENER = new ActionListener<>() {
        @Override
        public void onResponse(ClusterState clusterState) {}

        @Override
        public void onFailure(Exception e) {
            throw new AssertionError("should not be called", e);
        }
    };

    /**
     * Task that asserts it does not fail.
     */
    private static class ExpectSuccessTask implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            throw new AssertionError("should not be called", e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            // see parent method javadoc, we use dedicated listeners rather than calling this method
            throw new AssertionError("should not be called");
        }
    }
}
