/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class ClusterServiceTests extends ESTestCase {

    static ThreadPool threadPool;
    TimedClusterService clusterService;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(ClusterServiceTests.class.getName());
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createTimedClusterService(true);
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        super.tearDown();
    }

    TimedClusterService createTimedClusterService(boolean makeMaster) throws InterruptedException {
        TimedClusterService timedClusterService = new TimedClusterService(Settings.builder().put("cluster.name",
            "ClusterServiceTests").build(), new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool, () -> new DiscoveryNode("node1",  LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT));
        timedClusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes) {
                // skip
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                // skip
            }
        });
        timedClusterService.setClusterStatePublisher((event, ackListener) -> {
        });
        timedClusterService.setDiscoverySettings(new DiscoverySettings(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
        timedClusterService.start();
        ClusterState state = timedClusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(nodes)
            .masterNodeId(makeMaster ? nodes.getLocalNodeId() : null);
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .nodes(nodesBuilder).build();
        setState(timedClusterService, state);
        return timedClusterService;
    }

    public void testMasterAwareExecution() throws Exception {
        ClusterService nonMaster = createTimedClusterService(false);

        final boolean[] taskFailed = {false};
        final CountDownLatch latch1 = new CountDownLatch(1);
        nonMaster.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                latch1.countDown();
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                taskFailed[0] = true;
                latch1.countDown();
            }
        });

        latch1.await();
        assertTrue("cluster state update task was executed on a non-master", taskFailed[0]);

        taskFailed[0] = true;
        final CountDownLatch latch2 = new CountDownLatch(1);
        nonMaster.submitStateUpdateTask("test", new LocalClusterUpdateTask() {
            @Override
            public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) throws Exception {
                taskFailed[0] = false;
                latch2.countDown();
                return unchanged();
            }

            @Override
            public void onFailure(String source, Exception e) {
                taskFailed[0] = true;
                latch2.countDown();
            }
        });
        latch2.await();
        assertFalse("non-master cluster state update task was not executed", taskFailed[0]);

        nonMaster.close();
    }

    /*
   * test that a listener throwing an exception while handling a
   * notification does not prevent publication notification to the
   * executor
   */
    public void testClusterStateTaskListenerThrowingExceptionIsOkay() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean published = new AtomicBoolean();

        clusterService.submitStateUpdateTask(
            "testClusterStateTaskListenerThrowingExceptionIsOkay",
            new Object(),
            ClusterStateTaskConfig.build(Priority.NORMAL),
            new ClusterStateTaskExecutor<Object>() {
                @Override
                public ClusterTasksResult<Object> execute(ClusterState currentState, List<Object> tasks) throws Exception {
                    ClusterState newClusterState = ClusterState.builder(currentState).build();
                    return ClusterTasksResult.builder().successes(tasks).build(newClusterState);
                }

                @Override
                public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                    published.set(true);
                    latch.countDown();
                }
            },
            new ClusterStateTaskListener() {
                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    throw new IllegalStateException(source);
                }

                @Override
                public void onFailure(String source, Exception e) {
                }
            }
        );

        latch.await();
        assertTrue(published.get());
    }

    public void testBlockingCallInClusterStateTaskListenerFails() throws InterruptedException {
        assumeTrue("assertions must be enabled for this test to work", BaseFuture.class.desiredAssertionStatus());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AssertionError> assertionRef = new AtomicReference<>();

        clusterService.submitStateUpdateTask(
            "testBlockingCallInClusterStateTaskListenerFails",
            new Object(),
            ClusterStateTaskConfig.build(Priority.NORMAL),
            new ClusterStateTaskExecutor<Object>() {
                @Override
                public ClusterTasksResult<Object> execute(ClusterState currentState, List<Object> tasks) throws Exception {
                    ClusterState newClusterState = ClusterState.builder(currentState).build();
                    return ClusterTasksResult.builder().successes(tasks).build(newClusterState);
                }
            },
            new ClusterStateTaskListener() {
                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    BaseFuture<Void> future = new BaseFuture<Void>() {};
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
                }

                @Override
                public void onFailure(String source, Exception e) {
                }
            }
        );

        latch.await();
        assertNotNull(assertionRef.get());
        assertThat(assertionRef.get().getMessage(), containsString("not be the cluster state update thread. Reason: [Blocking operation]"));
    }

    public void testClusterStateBatchedUpdates() throws BrokenBarrierException, InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        class Task {
            private AtomicBoolean state = new AtomicBoolean();
            private final int id;

            Task(int id) {
                this.id = id;
            }

            public void execute() {
                if (!state.compareAndSet(false, true)) {
                    throw new IllegalStateException();
                } else {
                    counter.incrementAndGet();
                }
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

        int numberOfThreads = randomIntBetween(2, 8);
        int taskSubmissionsPerThread = randomIntBetween(1, 64);
        int numberOfExecutors = Math.max(1, numberOfThreads / 4);
        final Semaphore semaphore = new Semaphore(numberOfExecutors);

        class TaskExecutor implements ClusterStateTaskExecutor<Task> {
            private final List<Set<Task>> taskGroups;
            private AtomicInteger counter = new AtomicInteger();
            private AtomicInteger batches = new AtomicInteger();
            private AtomicInteger published = new AtomicInteger();

            TaskExecutor(List<Set<Task>> taskGroups) {
                this.taskGroups = taskGroups;
            }

            @Override
            public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
                for (Set<Task> expectedSet : taskGroups) {
                    long count = tasks.stream().filter(expectedSet::contains).count();
                    assertThat("batched set should be executed together or not at all. Expected " + expectedSet + "s. Executing " + tasks,
                        count, anyOf(equalTo(0L), equalTo((long) expectedSet.size())));
                }
                tasks.forEach(Task::execute);
                counter.addAndGet(tasks.size());
                ClusterState maybeUpdatedClusterState = currentState;
                if (randomBoolean()) {
                    maybeUpdatedClusterState = ClusterState.builder(currentState).build();
                    batches.incrementAndGet();
                    semaphore.acquire();
                }
                return ClusterTasksResult.<Task>builder().successes(tasks).build(maybeUpdatedClusterState);
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                published.incrementAndGet();
                semaphore.release();
            }
        }

        ConcurrentMap<String, AtomicInteger> processedStates = new ConcurrentHashMap<>();

        List<Set<Task>> taskGroups = new ArrayList<>();
        List<TaskExecutor> executors = new ArrayList<>();
        for (int i = 0; i < numberOfExecutors; i++) {
            executors.add(new TaskExecutor(taskGroups));
        }

        // randomly assign tasks to executors
        List<Tuple<TaskExecutor, Set<Task>>> assignments = new ArrayList<>();
        int taskId = 0;
        for (int i = 0; i < numberOfThreads; i++) {
            for (int j = 0; j < taskSubmissionsPerThread; j++) {
                TaskExecutor executor = randomFrom(executors);
                Set<Task> tasks = new HashSet<>();
                for (int t = randomInt(3); t >= 0; t--) {
                    tasks.add(new Task(taskId++));
                }
                taskGroups.add(tasks);
                assignments.add(Tuple.tuple(executor, tasks));
            }
        }

        Map<TaskExecutor, Integer> counts = new HashMap<>();
        int totalTaskCount = 0;
        for (Tuple<TaskExecutor, Set<Task>> assignment : assignments) {
            final int taskCount = assignment.v2().size();
            counts.merge(assignment.v1(), taskCount, (previous, count) -> previous + count);
            totalTaskCount += taskCount;
        }
        final CountDownLatch updateLatch = new CountDownLatch(totalTaskCount);
        final ClusterStateTaskListener listener = new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {
                fail(ExceptionsHelper.detailedMessage(e));
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                processedStates.computeIfAbsent(source, key -> new AtomicInteger()).incrementAndGet();
                updateLatch.countDown();
            }
        };

        final ConcurrentMap<String, AtomicInteger> submittedTasksPerThread = new ConcurrentHashMap<>();
        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            final int index = i;
            Thread thread = new Thread(() -> {
                final String threadName = Thread.currentThread().getName();
                try {
                    barrier.await();
                    for (int j = 0; j < taskSubmissionsPerThread; j++) {
                        Tuple<TaskExecutor, Set<Task>> assignment = assignments.get(index * taskSubmissionsPerThread + j);
                        final Set<Task> tasks = assignment.v2();
                        submittedTasksPerThread.computeIfAbsent(threadName, key -> new AtomicInteger()).addAndGet(tasks.size());
                        final TaskExecutor executor = assignment.v1();
                        if (tasks.size() == 1) {
                            clusterService.submitStateUpdateTask(
                                threadName,
                                tasks.stream().findFirst().get(),
                                ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                                executor,
                                listener);
                        } else {
                            Map<Task, ClusterStateTaskListener> taskListeners = new HashMap<>();
                            tasks.stream().forEach(t -> taskListeners.put(t, listener));
                            clusterService.submitStateUpdateTasks(
                                threadName,
                                taskListeners, ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                                executor
                            );
                        }
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
        updateLatch.await();
        // and until all of the publication callbacks have completed
        semaphore.acquire(numberOfExecutors);

        // assert the number of executed tasks is correct
        assertEquals(totalTaskCount, counter.get());

        // assert each executor executed the correct number of tasks
        for (TaskExecutor executor : executors) {
            if (counts.containsKey(executor)) {
                assertEquals((int) counts.get(executor), executor.counter.get());
                assertEquals(executor.batches.get(), executor.published.get());
            }
        }

        // assert the correct number of clusterStateProcessed events were triggered
        for (Map.Entry<String, AtomicInteger> entry : processedStates.entrySet()) {
            assertThat(submittedTasksPerThread, hasKey(entry.getKey()));
            assertEquals("not all tasks submitted by " + entry.getKey() + " received a processed event",
                entry.getValue().get(), submittedTasksPerThread.get(entry.getKey()).get());
        }
    }

    @TestLogging("org.elasticsearch.cluster.service:TRACE") // To ensure that we log cluster state events on TRACE level
    public void testClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test1",
                        "org.elasticsearch.cluster.service.ClusterServiceTests$TimedClusterService",
                        Level.DEBUG,
                        "*processing [test1]: took [1s] no change in cluster_state"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test2",
                        "org.elasticsearch.cluster.service.ClusterServiceTests$TimedClusterService",
                        Level.TRACE,
                        "*failed to execute cluster state update in [2s]*"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test3",
                        "org.elasticsearch.cluster.service.ClusterServiceTests$TimedClusterService",
                        Level.DEBUG,
                        "*processing [test3]: took [3s] done applying updated cluster_state (version: *, uuid: *)"));

        Logger clusterLogger = Loggers.getLogger("org.elasticsearch.cluster.service");
        Loggers.addAppender(clusterLogger, mockAppender);
        try {
            final CountDownLatch latch = new CountDownLatch(4);
            clusterService.currentTimeOverride = System.nanoTime();
            clusterService.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterService.currentTimeOverride += TimeValue.timeValueSeconds(1).nanos();
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            clusterService.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    clusterService.currentTimeOverride += TimeValue.timeValueSeconds(2).nanos();
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    latch.countDown();
                }
            });
            clusterService.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    clusterService.currentTimeOverride += TimeValue.timeValueSeconds(3).nanos();
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            clusterService.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
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

    @TestLogging("org.elasticsearch.cluster.service:WARN") // To ensure that we log cluster state events on WARN level
    public void testLongClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                        "test1 shouldn't see because setting is too low",
                        "org.elasticsearch.cluster.service.ClusterServiceTests$TimedClusterService",
                        Level.WARN,
                        "*cluster state update task [test1] took [*] above the warn threshold of *"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test2",
                        "org.elasticsearch.cluster.service.ClusterServiceTests$TimedClusterService",
                        Level.WARN,
                        "*cluster state update task [test2] took [32s] above the warn threshold of *"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test3",
                        "org.elasticsearch.cluster.service.ClusterServiceTests$TimedClusterService",
                        Level.WARN,
                        "*cluster state update task [test3] took [33s] above the warn threshold of *"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test4",
                        "org.elasticsearch.cluster.service.ClusterServiceTests$TimedClusterService",
                        Level.WARN,
                        "*cluster state update task [test4] took [34s] above the warn threshold of *"));

        Logger clusterLogger = Loggers.getLogger("org.elasticsearch.cluster.service");
        Loggers.addAppender(clusterLogger, mockAppender);
        try {
            final CountDownLatch latch = new CountDownLatch(5);
            final CountDownLatch processedFirstTask = new CountDownLatch(1);
            clusterService.currentTimeOverride = System.nanoTime();
            clusterService.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterService.currentTimeOverride += TimeValue.timeValueSeconds(1).nanos();
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                    processedFirstTask.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });

            processedFirstTask.await();
            clusterService.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterService.currentTimeOverride += TimeValue.timeValueSeconds(32).nanos();
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    latch.countDown();
                }
            });
            clusterService.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterService.currentTimeOverride += TimeValue.timeValueSeconds(33).nanos();
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            clusterService.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterService.currentTimeOverride += TimeValue.timeValueSeconds(34).nanos();
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            clusterService.submitStateUpdateTask("test5", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
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

    public void testDisconnectFromNewlyAddedNodesIfClusterStatePublishingFails() throws InterruptedException {
        TimedClusterService timedClusterService = new TimedClusterService(Settings.builder().put("cluster.name",
            "ClusterServiceTests").build(), new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool, () -> new DiscoveryNode("node1", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT));
        Set<DiscoveryNode> currentNodes = new HashSet<>();
        timedClusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes) {
                discoveryNodes.forEach(currentNodes::add);
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                Set<DiscoveryNode> nodeSet = new HashSet<>();
                nodesToKeep.iterator().forEachRemaining(nodeSet::add);
                currentNodes.removeIf(node -> nodeSet.contains(node) == false);
            }
        });
        AtomicBoolean failToCommit = new AtomicBoolean();
        timedClusterService.setClusterStatePublisher((event, ackListener) -> {
            if (failToCommit.get()) {
                throw new Discovery.FailedToCommitClusterStateException("just to test this");
            }
        });
        timedClusterService.setDiscoverySettings(new DiscoverySettings(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
        timedClusterService.start();
        ClusterState state = timedClusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(nodes)
            .masterNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .nodes(nodesBuilder).build();
        setState(timedClusterService, state);

        assertThat(currentNodes, equalTo(Sets.newHashSet(timedClusterService.state().getNodes())));

        final CountDownLatch latch = new CountDownLatch(1);

        // try to add node when cluster state publishing fails
        failToCommit.set(true);
        timedClusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                DiscoveryNode newNode = new DiscoveryNode("node2", LocalTransportAddress.buildUnique(), emptyMap(),
                    emptySet(), Version.CURRENT);
                return ClusterState.builder(currentState).nodes(DiscoveryNodes.builder(currentState.nodes()).add(newNode)).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                latch.countDown();
            }
        });

        latch.await();
        assertThat(currentNodes, equalTo(Sets.newHashSet(timedClusterService.state().getNodes())));
        timedClusterService.close();
    }

    public void testLocalNodeMasterListenerCallbacks() throws Exception {
        TimedClusterService timedClusterService = createTimedClusterService(false);

        AtomicBoolean isMaster = new AtomicBoolean();
        timedClusterService.addLocalNodeMasterListener(new LocalNodeMasterListener() {
            @Override
            public void onMaster() {
                isMaster.set(true);
            }

            @Override
            public void offMaster() {
                isMaster.set(false);
            }

            @Override
            public String executorName() {
                return ThreadPool.Names.SAME;
            }
        });

        ClusterState state = timedClusterService.state();
        DiscoveryNodes nodes = state.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).nodes(nodesBuilder).build();
        setState(timedClusterService, state);
        assertThat(isMaster.get(), is(true));

        nodes = state.nodes();
        nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(null);
        state = ClusterState.builder(state).blocks(ClusterBlocks.builder().addGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_WRITES))
            .nodes(nodesBuilder).build();
        setState(timedClusterService, state);
        assertThat(isMaster.get(), is(false));
        nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).nodes(nodesBuilder).build();
        setState(timedClusterService, state);
        assertThat(isMaster.get(), is(true));

        timedClusterService.close();
    }

    public void testClusterStateApplierCantSampleClusterState() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterService.addStateApplier(event -> {
            try {
                applierCalled.set(true);
                clusterService.state();
                error.set(new AssertionError("successfully sampled state"));
            } catch (AssertionError e) {
                if (e.getMessage().contains("should not be called by a cluster state applier") == false) {
                    error.set(e);
                }
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                error.compareAndSet(null, e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());
    }

    public void testClusterStateApplierCanCreateAnObserver() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterService.addStateApplier(event -> {
            try {
                applierCalled.set(true);
                ClusterStateObserver observer = new ClusterStateObserver(event.state(),
                    clusterService, null, logger, threadPool.getThreadContext());
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {

                    }

                    @Override
                    public void onClusterServiceClose() {

                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {

                    }
                });
            } catch (AssertionError e) {
                    error.set(e);
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                error.compareAndSet(null, e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());
    }

    static class TimedClusterService extends ClusterService {

        public volatile Long currentTimeOverride = null;

        TimedClusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool,
                                   Supplier<DiscoveryNode> localNodeSupplier) {
            super(settings, clusterSettings, threadPool, localNodeSupplier);
        }

        @Override
        protected long currentTimeInNanos() {
            if (currentTimeOverride != null) {
                return currentTimeOverride;
            }
            return super.currentTimeInNanos();
        }
    }
}
