/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.service;

import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ClusterServiceIT extends ESIntegTestCase {

    private static final TimeValue TEN_SECONDS = TimeValue.timeValueSeconds(10L);

    public void testAckedUpdateTask() throws Exception {
        internalCluster().startNode();
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackFailure = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch processedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask(
            "test",
            new AckedClusterStateUpdateTask(MasterServiceTests.ackedRequest(TEN_SECONDS, TEN_SECONDS), null) {
                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked() {
                    allNodesAcked.set(true);
                    latch.countDown();
                }

                @Override
                public void onAckFailure(Exception e) {
                    ackFailure.set(true);
                    latch.countDown();
                }

                @Override
                public void onAckTimeout() {
                    ackTimeout.set(true);
                    latch.countDown();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    processedLatch.countDown();
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    executed.set(true);
                    return ClusterState.builder(currentState).build();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("failed to execute callback in test", e);
                    onFailure.set(true);
                    latch.countDown();
                }
            },
            ClusterStateTaskExecutor.unbatched()
        );

        ensureGreen();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(true));
        assertThat(ackFailure.get(), equalTo(false));
        assertThat(ackTimeout.get(), equalTo(false));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));

        assertThat(processedLatch.await(1, TimeUnit.SECONDS), equalTo(true));
    }

    public void testAckedUpdateTaskSameClusterState() throws Exception {
        internalCluster().startNode();
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackFailure = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch processedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask(
            "test",
            new AckedClusterStateUpdateTask(MasterServiceTests.ackedRequest(TEN_SECONDS, TEN_SECONDS), null) {
                @Override
                public void onAllNodesAcked() {
                    allNodesAcked.set(true);
                    latch.countDown();
                }

                @Override
                public void onAckFailure(Exception e) {
                    ackFailure.set(true);
                    latch.countDown();
                }

                @Override
                public void onAckTimeout() {
                    ackTimeout.set(true);
                    latch.countDown();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    processedLatch.countDown();
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    executed.set(true);
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("failed to execute callback in test", e);
                    onFailure.set(true);
                    latch.countDown();
                }
            },
            ClusterStateTaskExecutor.unbatched()
        );

        ensureGreen();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(true));
        assertThat(ackFailure.get(), equalTo(false));
        assertThat(ackTimeout.get(), equalTo(false));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));

        assertThat(processedLatch.await(1, TimeUnit.SECONDS), equalTo(true));
    }

    public void testAckedUpdateTaskNoAckExpected() throws Exception {
        internalCluster().startNode();
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackFailure = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);

        clusterService.submitStateUpdateTask(
            "test",
            new AckedClusterStateUpdateTask(MasterServiceTests.ackedRequest(TEN_SECONDS, TEN_SECONDS), null) {
                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return false;
                }

                @Override
                public void onAllNodesAcked() {
                    allNodesAcked.set(true);
                    latch.countDown();
                }

                @Override
                public void onAckFailure(Exception e) {
                    ackFailure.set(true);
                    latch.countDown();
                }

                @Override
                public void onAckTimeout() {
                    ackTimeout.set(true);
                    latch.countDown();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {}

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    executed.set(true);
                    return ClusterState.builder(currentState).build();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("failed to execute callback in test", e);
                    onFailure.set(true);
                    latch.countDown();
                }
            },
            ClusterStateTaskExecutor.unbatched()
        );

        ensureGreen();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(true));
        assertThat(ackFailure.get(), equalTo(false));
        assertThat(ackTimeout.get(), equalTo(false));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));
    }

    public void testAckedUpdateTaskTimeoutZero() throws Exception {
        internalCluster().startNode();
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackFailure = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch processedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask(
            "test",
            new AckedClusterStateUpdateTask(MasterServiceTests.ackedRequest(TimeValue.ZERO, TEN_SECONDS), null) {
                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return false;
                }

                @Override
                public void onAllNodesAcked() {
                    allNodesAcked.set(true);
                    latch.countDown();
                }

                @Override
                public void onAckFailure(Exception e) {
                    ackFailure.set(true);
                    latch.countDown();
                }

                @Override
                public void onAckTimeout() {
                    ackTimeout.set(true);
                    latch.countDown();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    processedLatch.countDown();
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    executed.set(true);
                    return ClusterState.builder(currentState).build();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("failed to execute callback in test", e);
                    onFailure.set(true);
                    latch.countDown();
                }
            },
            ClusterStateTaskExecutor.unbatched()
        );

        ensureGreen();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(false));
        assertThat(ackFailure.get(), equalTo(false));
        assertThat(ackTimeout.get(), equalTo(true));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));

        assertThat(processedLatch.await(1, TimeUnit.SECONDS), equalTo(true));
    }

    public void testPendingUpdateTask() throws Exception {
        String node_0 = internalCluster().startNode();
        internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node_0);
        final CountDownLatch block1 = new CountDownLatch(1);
        final CountDownLatch invoked1 = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("1", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                invoked1.countDown();
                try {
                    block1.await();
                } catch (InterruptedException e) {
                    fail();
                }
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                invoked1.countDown();
                fail();
            }
        }, ClusterStateTaskExecutor.unbatched());
        invoked1.await();
        final CountDownLatch invoked2 = new CountDownLatch(9);
        for (int i = 2; i <= 10; i++) {
            clusterService.submitStateUpdateTask(Integer.toString(i), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    invoked2.countDown();
                }
            }, ClusterStateTaskExecutor.unbatched());
        }

        // there might be other tasks in this node, make sure to only take the ones we add into account in this test

        // The tasks can be re-ordered, so we need to check out-of-order
        Set<String> controlSources = new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
        List<PendingClusterTask> pendingClusterTasks = clusterService.getMasterService().pendingTasks();
        assertThat(pendingClusterTasks.size(), greaterThanOrEqualTo(10));
        assertThat(pendingClusterTasks.get(0).getSource().string(), equalTo("1"));
        assertThat(pendingClusterTasks.get(0).isExecuting(), equalTo(true));
        for (PendingClusterTask task : pendingClusterTasks) {
            controlSources.remove(task.getSource().string());
        }
        assertTrue(controlSources.isEmpty());

        controlSources = new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
        PendingClusterTasksResponse response = internalCluster().coordOnlyNodeClient().admin().cluster().preparePendingClusterTasks().get();
        assertThat(response.pendingTasks().size(), greaterThanOrEqualTo(10));
        assertThat(response.pendingTasks().get(0).getSource().string(), equalTo("1"));
        assertThat(response.pendingTasks().get(0).isExecuting(), equalTo(true));
        for (PendingClusterTask task : response) {
            controlSources.remove(task.getSource().string());
        }
        assertTrue(controlSources.isEmpty());
        block1.countDown();
        invoked2.await();

        // whenever we test for no tasks, we need to wait since this is a live node
        assertBusy(() -> assertTrue("Pending tasks not empty", clusterService.getMasterService().pendingTasks().isEmpty()));
        waitNoPendingTasksOnAll();

        final CountDownLatch block2 = new CountDownLatch(1);
        final CountDownLatch invoked3 = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("1", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                invoked3.countDown();
                try {
                    block2.await();
                } catch (InterruptedException e) {
                    fail();
                }
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                invoked3.countDown();
                fail();
            }
        }, ClusterStateTaskExecutor.unbatched());
        invoked3.await();

        for (int i = 2; i <= 5; i++) {
            clusterService.submitStateUpdateTask(Integer.toString(i), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            }, ClusterStateTaskExecutor.unbatched());
        }
        Thread.sleep(100);

        pendingClusterTasks = clusterService.getMasterService().pendingTasks();
        assertThat(pendingClusterTasks.size(), greaterThanOrEqualTo(5));
        controlSources = new HashSet<>(Arrays.asList("1", "2", "3", "4", "5"));
        for (PendingClusterTask task : pendingClusterTasks) {
            controlSources.remove(task.getSource().string());
        }
        assertTrue(controlSources.isEmpty());

        response = internalCluster().coordOnlyNodeClient().admin().cluster().preparePendingClusterTasks().get();
        assertThat(response.pendingTasks().size(), greaterThanOrEqualTo(5));
        controlSources = new HashSet<>(Arrays.asList("1", "2", "3", "4", "5"));
        for (PendingClusterTask task : response) {
            if (controlSources.remove(task.getSource().string())) {
                assertThat(task.getTimeInQueueInMillis(), greaterThan(0L));
            }
        }
        assertTrue(controlSources.isEmpty());
        block2.countDown();
    }
}
