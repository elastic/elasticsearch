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
package org.elasticsearch.cluster;

import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ClusterServiceTests extends ElasticsearchIntegrationTest {

    @Test
    public void testTimeoutUpdateTask() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();
        internalCluster().startNode(settings);
        ClusterService clusterService1 = internalCluster().getInstance(ClusterService.class);
        final CountDownLatch block = new CountDownLatch(1);
        clusterService1.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    fail();
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                fail();
            }
        });

        final CountDownLatch timedOut = new CountDownLatch(1);
        final AtomicBoolean executeCalled = new AtomicBoolean();
        clusterService1.submitStateUpdateTask("test2", new TimeoutClusterStateUpdateTask() {
            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueMillis(2);
            }

            @Override
            public void onFailure(String source, Throwable t) {
                timedOut.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                executeCalled.set(true);
                return currentState;
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            }
        });

        assertThat(timedOut.await(500, TimeUnit.MILLISECONDS), equalTo(true));
        block.countDown();
        Thread.sleep(100); // sleep a bit to double check that execute on the timed out update task is not called...
        assertThat(executeCalled.get(), equalTo(false));
    }

    @Test
    public void testAckedUpdateTask() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();
        internalCluster().startNode(settings);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch processedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask<Void>(null, null) {
            @Override
            protected Void newResponse(boolean acknowledged) {
                return null;
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                allNodesAcked.set(true);
                latch.countDown();
            }

            @Override
            public void onAckTimeout() {
                ackTimeout.set(true);
                latch.countDown();
            }

            @Override
            public TimeValue ackTimeout() {
                return TimeValue.timeValueSeconds(10);
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueSeconds(10);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                processedLatch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                executed.set(true);
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("failed to execute callback in test {}", t, source);
                onFailure.set(true);
                latch.countDown();
            }
        });

        ensureGreen();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(true));
        assertThat(ackTimeout.get(), equalTo(false));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));

        assertThat(processedLatch.await(1, TimeUnit.SECONDS), equalTo(true));
    }

    @Test
    public void testAckedUpdateTaskSameClusterState() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();
        internalCluster().startNode(settings);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch processedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask<Void>(null, null) {
            @Override
            protected Void newResponse(boolean acknowledged) {
                return null;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                allNodesAcked.set(true);
                latch.countDown();
            }

            @Override
            public void onAckTimeout() {
                ackTimeout.set(true);
                latch.countDown();
            }

            @Override
            public TimeValue ackTimeout() {
                return TimeValue.timeValueSeconds(10);
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueSeconds(10);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                processedLatch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                executed.set(true);
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("failed to execute callback in test {}", t, source);
                onFailure.set(true);
                latch.countDown();
            }
        });

        ensureGreen();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(true));
        assertThat(ackTimeout.get(), equalTo(false));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));

        assertThat(processedLatch.await(1, TimeUnit.SECONDS), equalTo(true));
    }

    @Test
    public void testMasterAwareExecution() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();

        ListenableFuture<String> master = internalCluster().startNodeAsync(settings);
        ListenableFuture<String> nonMaster = internalCluster().startNodeAsync(settingsBuilder().put(settings).put("node.master", false).build());
        master.get();
        ensureGreen(); // make sure we have a cluster

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nonMaster.get());

        final boolean[] taskFailed = {false};
        final CountDownLatch latch1 = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                latch1.countDown();
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                taskFailed[0] = true;
                latch1.countDown();
            }
        });

        latch1.await();
        assertTrue("cluster state update task was executed on a non-master", taskFailed[0]);

        taskFailed[0] = true;
        final CountDownLatch latch2 = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new ClusterStateNonMasterUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                taskFailed[0] = false;
                latch2.countDown();
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                taskFailed[0] = true;
                latch2.countDown();
            }
        });
        latch2.await();
        assertFalse("non-master cluster state update task was not executed", taskFailed[0]);
    }

    @Test
    public void testAckedUpdateTaskNoAckExpected() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();
        internalCluster().startNode(settings);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask<Void>(null, null) {
            @Override
            protected Void newResponse(boolean acknowledged) {
                return null;
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return false;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                allNodesAcked.set(true);
                latch.countDown();
            }

            @Override
            public void onAckTimeout() {
                ackTimeout.set(true);
                latch.countDown();
            }

            @Override
            public TimeValue ackTimeout() {
                return TimeValue.timeValueSeconds(10);
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueSeconds(10);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                executed.set(true);
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("failed to execute callback in test {}", t, source);
                onFailure.set(true);
                latch.countDown();
            }
        });

        ensureGreen();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(true));
        assertThat(ackTimeout.get(), equalTo(false));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));
    }

    @Test
    public void testAckedUpdateTaskTimeoutZero() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();
        internalCluster().startNode(settings);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch processedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask<Void>(null, null) {
            @Override
            protected Void newResponse(boolean acknowledged) {
                return null;
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return false;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                allNodesAcked.set(true);
                latch.countDown();
            }

            @Override
            public void onAckTimeout() {
                ackTimeout.set(true);
                latch.countDown();
            }

            @Override
            public TimeValue ackTimeout() {
                return TimeValue.timeValueSeconds(0);
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueSeconds(10);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                processedLatch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                executed.set(true);
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("failed to execute callback in test {}", t, source);
                onFailure.set(true);
                latch.countDown();
            }
        });

        ensureGreen();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(false));
        assertThat(ackTimeout.get(), equalTo(true));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));

        assertThat(processedLatch.await(1, TimeUnit.SECONDS), equalTo(true));
    }

    @Test
    public void testPendingUpdateTask() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();
        String node_0 = internalCluster().startNode(settings);
        internalCluster().startNodeClient(settings);

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
            public void onFailure(String source, Throwable t) {
                invoked1.countDown();
                fail();
            }
        });
        invoked1.await();
        final CountDownLatch invoked2 = new CountDownLatch(9);
        for (int i = 2; i <= 10; i++) {
            clusterService.submitStateUpdateTask(Integer.toString(i), new ProcessedClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    fail();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    invoked2.countDown();
                }
            });
        }

        // there might be other tasks in this node, make sure to only take the ones we add into account in this test

        // The tasks can be re-ordered, so we need to check out-of-order
        Set<String> controlSources = new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
        List<PendingClusterTask> pendingClusterTasks = clusterService.pendingTasks();
        assertThat(pendingClusterTasks.size(), greaterThanOrEqualTo(10));
        assertThat(pendingClusterTasks.get(0).getSource().string(), equalTo("1"));
        assertThat(pendingClusterTasks.get(0).isExecuting(), equalTo(true));
        for (PendingClusterTask task : pendingClusterTasks) {
            controlSources.remove(task.getSource().string());
        }
        assertTrue(controlSources.isEmpty());

        controlSources = new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
        PendingClusterTasksResponse response = internalCluster().clientNodeClient().admin().cluster().preparePendingClusterTasks().execute().actionGet();
        assertThat(response.pendingTasks().size(), greaterThanOrEqualTo(10));
        assertThat(response.pendingTasks().get(0).getSource().string(), equalTo("1"));
        assertThat(response.pendingTasks().get(0).isExecuting(), equalTo(true));
        for (PendingClusterTask task : response) {
            controlSources.remove(task.getSource().string());
        }
        assertTrue(controlSources.isEmpty());
        block1.countDown();
        invoked2.await();

        // whenever we test for no tasks, we need to awaitBusy since this is a live node
        assertTrue(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return clusterService.pendingTasks().isEmpty();
            }
        }));
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
            public void onFailure(String source, Throwable t) {
                invoked3.countDown();
                fail();
            }
        });
        invoked3.await();

        for (int i = 2; i <= 5; i++) {
            clusterService.submitStateUpdateTask(Integer.toString(i), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    fail();
                }
            });
        }
        Thread.sleep(100);

        pendingClusterTasks = clusterService.pendingTasks();
        assertThat(pendingClusterTasks.size(), greaterThanOrEqualTo(5));
        controlSources = new HashSet<>(Arrays.asList("1", "2", "3", "4", "5"));
        for (PendingClusterTask task : pendingClusterTasks) {
            controlSources.remove(task.getSource().string());
        }
        assertTrue(controlSources.isEmpty());

        response = internalCluster().clientNodeClient().admin().cluster().preparePendingClusterTasks().get();
        assertThat(response.pendingTasks().size(), greaterThanOrEqualTo(5));
        controlSources = new HashSet<>(Arrays.asList("1", "2", "3", "4", "5"));
        for (PendingClusterTask task : response) {
            if (controlSources.remove(task.getSource().string())) {
                assertThat(task.getTimeInQueueInMillis(), greaterThan(0l));
            }
        }
        assertTrue(controlSources.isEmpty());
        block2.countDown();
    }

    @Test @Slow
    public void testLocalNodeMasterListenerCallbacks() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 1)
                .put("discovery.zen.ping_timeout", "400ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("plugin.types", TestPlugin.class.getName())
                .build();

        String node_0 = internalCluster().startNode(settings);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        MasterAwareService testService = internalCluster().getInstance(MasterAwareService.class);

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("1").get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // the first node should be a master as the minimum required is 1
        assertThat(clusterService.state().nodes().masterNode(), notNullValue());
        assertThat(clusterService.state().nodes().localNodeMaster(), is(true));
        assertThat(testService.master(), is(true));

        String node_1 = internalCluster().startNode(settings);
        final ClusterService clusterService1 = internalCluster().getInstance(ClusterService.class, node_1);
        MasterAwareService testService1 = internalCluster().getInstance(MasterAwareService.class, node_1);

        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // the second node should not be the master as node1 is already the master.
        assertThat(clusterService1.state().nodes().localNodeMaster(), is(false));
        assertThat(testService1.master(), is(false));

        internalCluster().stopCurrentMasterNode();
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("1").get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // now that node0 is closed, node1 should be elected as master
        assertThat(clusterService1.state().nodes().localNodeMaster(), is(true));
        assertThat(testService1.master(), is(true));

        // start another node and set min_master_node
        internalCluster().startNode(ImmutableSettings.builder().put(settings));
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());

        Settings transientSettings = settingsBuilder()
                .put("discovery.zen.minimum_master_nodes", 2)
                .build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(transientSettings).get();

        // and shutdown the second node
        internalCluster().stopRandomNonMasterNode();

        // there should not be any master as the minimum number of required eligible masters is not met
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object obj) {
                return clusterService1.state().nodes().masterNode() == null && clusterService1.state().status() == ClusterState.ClusterStateStatus.APPLIED;
            }
        });
        assertThat(testService1.master(), is(false));

        // bring the node back up
        String node_2 = internalCluster().startNode(ImmutableSettings.builder().put(settings).put(transientSettings));
        ClusterService clusterService2 = internalCluster().getInstance(ClusterService.class, node_2);
        MasterAwareService testService2 = internalCluster().getInstance(MasterAwareService.class, node_2);

        // make sure both nodes see each other otherwise the masternode below could be null if node 2 is master and node 1 did'r receive the updated cluster state...
        assertThat(internalCluster().client(node_1).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setLocal(true).setWaitForNodes("2").get().isTimedOut(), is(false));
        assertThat(internalCluster().client(node_2).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setLocal(true).setWaitForNodes("2").get().isTimedOut(), is(false));

        // now that we started node1 again, a new master should be elected
        assertThat(clusterService2.state().nodes().masterNode(), is(notNullValue()));
        if (node_2.equals(clusterService2.state().nodes().masterNode().name())) {
            assertThat(testService1.master(), is(false));
            assertThat(testService2.master(), is(true));
        } else {
            assertThat(testService1.master(), is(true));
            assertThat(testService2.master(), is(false));
        }
    }

    /**
     * Note, this test can only work as long as we have a single thread executor executing the state update tasks!
     */
    @Test
    public void testPrioritizedTasks() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();
        internalCluster().startNode(settings);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        BlockingTask block = new BlockingTask();
        clusterService.submitStateUpdateTask("test", Priority.IMMEDIATE, block);
        int taskCount = randomIntBetween(5, 20);
        Priority[] priorities = Priority.values();

        // will hold all the tasks in the order in which they were executed
        List<PrioritiezedTask> tasks = new ArrayList<>(taskCount);
        CountDownLatch latch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            Priority priority = priorities[randomIntBetween(0, priorities.length - 1)];
            clusterService.submitStateUpdateTask("test", priority, new PrioritiezedTask(priority, latch, tasks));
        }

        block.release();
        latch.await();

        Priority prevPriority = null;
        for (PrioritiezedTask task : tasks) {
            if (prevPriority == null) {
                prevPriority = task.priority;
            } else {
                assertThat(task.priority.sameOrAfter(prevPriority), is(true));
            }
        }
    }

    private static class BlockingTask extends ClusterStateUpdateTask {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            latch.await();
            return currentState;
        }

        @Override
        public void onFailure(String source, Throwable t) {
        }

        public void release() {
            latch.countDown();
        }

    }

    private static class PrioritiezedTask extends ClusterStateUpdateTask {

        private final Priority priority;
        private final CountDownLatch latch;
        private final List<PrioritiezedTask> tasks;

        private PrioritiezedTask(Priority priority, CountDownLatch latch, List<PrioritiezedTask> tasks) {
            this.priority = priority;
            this.latch = latch;
            this.tasks = tasks;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            tasks.add(this);
            latch.countDown();
            return currentState;
        }

        @Override
        public void onFailure(String source, Throwable t) {
            latch.countDown();
        }
    }

    public static class TestPlugin extends AbstractPlugin {

        @Override
        public String name() {
            return "test plugin";
        }

        @Override
        public String description() {
            return "test plugin";
        }

        @Override
        public Collection<Class<? extends LifecycleComponent>> services() {
            List<Class<? extends LifecycleComponent>> services = new ArrayList<>(1);
            services.add(MasterAwareService.class);
            return services;
        }
    }

    @Singleton
    public static class MasterAwareService extends AbstractLifecycleComponent<MasterAwareService> implements LocalNodeMasterListener {

        private final ClusterService clusterService;
        private volatile boolean master;

        @Inject
        public MasterAwareService(Settings settings, ClusterService clusterService) {
            super(settings);
            clusterService.add(this);
            this.clusterService = clusterService;
            logger.info("initialized test service");
        }

        @Override
        public void onMaster() {
            logger.info("on master [" + clusterService.localNode() + "]");
            master = true;
        }

        @Override
        public void offMaster() {
            logger.info("off master [" + clusterService.localNode() + "]");
            master = false;
        }

        public boolean master() {
            return master;
        }

        @Override
        protected void doStart() throws ElasticsearchException {
        }

        @Override
        protected void doStop() throws ElasticsearchException {
        }

        @Override
        protected void doClose() throws ElasticsearchException {
        }

        @Override
        public String executorName() {
            return ThreadPool.Names.SAME;
        }

    }
}
