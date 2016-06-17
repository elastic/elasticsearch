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

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
@ESIntegTestCase.SuppressLocalMode
public class ClusterServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TestPlugin.class);
    }

    public void testAckedUpdateTask() throws Exception {
        Settings settings = Settings.builder()
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

    public void testAckedUpdateTaskSameClusterState() throws Exception {
        Settings settings = Settings.builder()
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

    public void testAckedUpdateTaskNoAckExpected() throws Exception {
        Settings settings = Settings.builder()
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

    public void testAckedUpdateTaskTimeoutZero() throws Exception {
        Settings settings = Settings.builder()
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

    @TestLogging("_root:debug,action.admin.cluster.tasks:trace")
    public void testPendingUpdateTask() throws Exception {
        Settings settings = Settings.builder()
                .put("discovery.type", "local")
                .build();
        String node_0 = internalCluster().startNode(settings);
        internalCluster().startCoordinatingOnlyNode(settings);

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
            clusterService.submitStateUpdateTask(Integer.toString(i), new ClusterStateUpdateTask() {
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

        // whenever we test for no tasks, we need to awaitBusy since this is a live node
        assertTrue(awaitBusy(() -> clusterService.pendingTasks().isEmpty()));
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

    public void testLocalNodeMasterListenerCallbacks() throws Exception {
        Settings settings = Settings.builder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 1)
                .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "400ms")
                .put("discovery.initial_state_timeout", "500ms")
                .build();

        String node_0 = internalCluster().startNode(settings);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        MasterAwareService testService = internalCluster().getInstance(MasterAwareService.class);

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("1").get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // the first node should be a master as the minimum required is 1
        assertThat(clusterService.state().nodes().getMasterNode(), notNullValue());
        assertThat(clusterService.state().nodes().isLocalNodeElectedMaster(), is(true));
        assertThat(testService.master(), is(true));

        String node_1 = internalCluster().startNode(settings);
        final ClusterService clusterService1 = internalCluster().getInstance(ClusterService.class, node_1);
        MasterAwareService testService1 = internalCluster().getInstance(MasterAwareService.class, node_1);

        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // the second node should not be the master as node1 is already the master.
        assertThat(clusterService1.state().nodes().isLocalNodeElectedMaster(), is(false));
        assertThat(testService1.master(), is(false));

        internalCluster().stopCurrentMasterNode();
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("1").get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // now that node0 is closed, node1 should be elected as master
        assertThat(clusterService1.state().nodes().isLocalNodeElectedMaster(), is(true));
        assertThat(testService1.master(), is(true));

        // start another node and set min_master_node
        internalCluster().startNode(Settings.builder().put(settings));
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());

        Settings transientSettings = Settings.builder()
                .put("discovery.zen.minimum_master_nodes", 2)
                .build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(transientSettings).get();

        // and shutdown the second node
        internalCluster().stopRandomNonMasterNode();

        // there should not be any master as the minimum number of required eligible masters is not met
        awaitBusy(() -> clusterService1.state().nodes().getMasterNode() == null &&
                clusterService1.state().status() == ClusterState.ClusterStateStatus.APPLIED);
        assertThat(testService1.master(), is(false));

        // bring the node back up
        String node_2 = internalCluster().startNode(Settings.builder().put(settings).put(transientSettings));
        ClusterService clusterService2 = internalCluster().getInstance(ClusterService.class, node_2);
        MasterAwareService testService2 = internalCluster().getInstance(MasterAwareService.class, node_2);

        // make sure both nodes see each other otherwise the masternode below could be null if node 2 is master and node 1 did'r receive
        // the updated cluster state...
        assertThat(internalCluster().client(node_1).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setLocal(true)
                .setWaitForNodes("2").get().isTimedOut(), is(false));
        assertThat(internalCluster().client(node_2).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setLocal(true)
                .setWaitForNodes("2").get().isTimedOut(), is(false));

        // now that we started node1 again, a new master should be elected
        assertThat(clusterService2.state().nodes().getMasterNode(), is(notNullValue()));
        if (node_2.equals(clusterService2.state().nodes().getMasterNode().getName())) {
            assertThat(testService1.master(), is(false));
            assertThat(testService2.master(), is(true));
        } else {
            assertThat(testService1.master(), is(true));
            assertThat(testService2.master(), is(false));
        }
    }

    public static class TestPlugin extends Plugin {

        @Override
        public Collection<Class<? extends LifecycleComponent>> nodeServices() {
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
            logger.info("on master [{}]", clusterService.localNode());
            master = true;
        }

        @Override
        public void offMaster() {
            logger.info("off master [{}]", clusterService.localNode());
            master = false;
        }

        public boolean master() {
            return master;
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }

        @Override
        protected void doClose() {
        }

        @Override
        public String executorName() {
            return ThreadPool.Names.SAME;
        }

    }
}
