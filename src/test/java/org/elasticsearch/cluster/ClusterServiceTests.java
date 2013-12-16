/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.ElasticSearchException;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numNodes = 0)
public class ClusterServiceTests extends ElasticsearchIntegrationTest {

    @Test
    public void testTimeoutUpdateTask() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();
        cluster().startNode(settings);
        ClusterService clusterService1 = cluster().getInstance(ClusterService.class);
        final CountDownLatch block = new CountDownLatch(1);
        clusterService1.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    assert false;
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                assert false;
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
        cluster().startNode(settings);
        ClusterService clusterService = cluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch processedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask() {
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
        cluster().startNode(settings);
        ClusterService clusterService = cluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch processedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask() {
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
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("failed to execute callback in test {}", t, source);
                onFailure.set(true);
                latch.countDown();
            }
        });

        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(true));
        assertThat(ackTimeout.get(), equalTo(false));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));

        assertThat(processedLatch.await(1, TimeUnit.SECONDS), equalTo(true));
    }

    @Test
    public void testAckedUpdateTaskNoAckExpected() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "local")
                .build();
        cluster().startNode(settings);
        ClusterService clusterService = cluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask() {
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
        cluster().startNode(settings);
        ClusterService clusterService = cluster().getInstance(ClusterService.class);

        final AtomicBoolean allNodesAcked = new AtomicBoolean(false);
        final AtomicBoolean ackTimeout = new AtomicBoolean(false);
        final AtomicBoolean onFailure = new AtomicBoolean(false);
        final AtomicBoolean executed = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch processedLatch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask() {
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

        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(allNodesAcked.get(), equalTo(false));
        assertThat(ackTimeout.get(), equalTo(true));
        assertThat(executed.get(), equalTo(true));
        assertThat(onFailure.get(), equalTo(false));

        assertThat(processedLatch.await(1, TimeUnit.SECONDS), equalTo(true));
    }

    @Test
    public void testPendingUpdateTask() throws Exception {
        Settings zenSettings = settingsBuilder()
                .put("discovery.type", "zen").build();
        String node_0 = cluster().startNode(zenSettings);
        cluster().startNodeClient(zenSettings);


        ClusterService clusterService = cluster().getInstance(ClusterService.class, node_0);
        final CountDownLatch block1 = new CountDownLatch(1);
        final CountDownLatch invoked1 = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("1", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                invoked1.countDown();
                try {
                    block1.await();
                } catch (InterruptedException e) {
                    assert false;
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                invoked1.countDown();
                assert false;
            }
        });
        invoked1.await();
        final CountDownLatch invoked2 = new CountDownLatch(9);
        for (int i = 2; i <= 10; i++) {
            clusterService.submitStateUpdateTask(Integer.toString(i), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    invoked2.countDown();
                    return currentState;
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    assert false;
                }
            });
        }

        // The tasks can be re-ordered, so we need to check out-of-order
        Set<String> controlSources = new HashSet<String>(Arrays.asList("2", "3", "4", "5", "6", "7", "8", "9", "10"));
        List<PendingClusterTask> pendingClusterTasks = clusterService.pendingTasks();
        assertThat(pendingClusterTasks.size(), equalTo(9));
        for (PendingClusterTask task : pendingClusterTasks) {
            assertTrue(controlSources.remove(task.source().string()));
        }
        assertTrue(controlSources.isEmpty());

        controlSources = new HashSet<String>(Arrays.asList("2", "3", "4", "5", "6", "7", "8", "9", "10"));
        PendingClusterTasksResponse response = cluster().clientNodeClient().admin().cluster().preparePendingClusterTasks().execute().actionGet();
        assertThat(response.pendingTasks().size(), equalTo(9));
        for (PendingClusterTask task : response) {
            assertTrue(controlSources.remove(task.source().string()));
        }
        assertTrue(controlSources.isEmpty());
        block1.countDown();
        invoked2.await();

        pendingClusterTasks = clusterService.pendingTasks();
        assertThat(pendingClusterTasks, empty());
        response = cluster().clientNodeClient().admin().cluster().preparePendingClusterTasks().execute().actionGet();
        assertThat(response.pendingTasks(), empty());

        final CountDownLatch block2 = new CountDownLatch(1);
        final CountDownLatch invoked3 = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("1", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                invoked3.countDown();
                try {
                    block2.await();
                } catch (InterruptedException e) {
                    assert false;
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                invoked3.countDown();
                assert false;
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
                    assert false;
                }
            });
        }
        Thread.sleep(100);

        pendingClusterTasks = clusterService.pendingTasks();
        assertThat(pendingClusterTasks.size(), equalTo(4));
        controlSources = new HashSet<String>(Arrays.asList("2", "3", "4", "5"));
        for (PendingClusterTask task : pendingClusterTasks) {
            assertTrue(controlSources.remove(task.source().string()));
        }
        assertTrue(controlSources.isEmpty());

        response = cluster().clientNodeClient().admin().cluster().preparePendingClusterTasks().execute().actionGet();
        assertThat(response.pendingTasks().size(), equalTo(4));
        controlSources = new HashSet<String>(Arrays.asList("2", "3", "4", "5"));
        for (PendingClusterTask task : response) {
            assertTrue(controlSources.remove(task.source().string()));
            assertThat(task.getTimeInQueueInMillis(), greaterThan(0l));
        }
        assertTrue(controlSources.isEmpty());
        block2.countDown();
    }

    @Test
    public void testListenerCallbacks() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 1)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("plugin.types", TestPlugin.class.getName())
                .build();

        cluster().startNode(settings);
        ClusterService clusterService1 = cluster().getInstance(ClusterService.class);
        MasterAwareService testService1 = cluster().getInstance(MasterAwareService.class);

        // the first node should be a master as the minimum required is 1
        assertThat(clusterService1.state().nodes().masterNode(), notNullValue());
        assertThat(clusterService1.state().nodes().localNodeMaster(), is(true));
        assertThat(testService1.master(), is(true));

        String node_1 = cluster().startNode(settings);
        ClusterService clusterService2 = cluster().getInstance(ClusterService.class, node_1);
        MasterAwareService testService2 = cluster().getInstance(MasterAwareService.class, node_1);

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // the second node should not be the master as node1 is already the master.
        assertThat(clusterService2.state().nodes().localNodeMaster(), is(false));
        assertThat(testService2.master(), is(false));

        cluster().stopCurrentMasterNode();
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("1").execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // now that node1 is closed, node2 should be elected as master
        assertThat(clusterService2.state().nodes().localNodeMaster(), is(true));
        assertThat(testService2.master(), is(true));

        Settings newSettings = settingsBuilder()
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.type", "zen")
                .build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(newSettings).execute().actionGet();
        Thread.sleep(200);

        // there should not be any master as the minimum number of required eligible masters is not met
        assertThat(clusterService2.state().nodes().masterNode(), is(nullValue()));
        assertThat(testService2.master(), is(false));


        String node_2 = cluster().startNode(settings);
        clusterService1 = cluster().getInstance(ClusterService.class, node_2);
        testService1 = cluster().getInstance(MasterAwareService.class, node_2);

        // make sure both nodes see each other otherwise the masternode below could be null if node 2 is master and node 1 did'r receive the updated cluster state...
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setLocal(true).setWaitForNodes("2").execute().actionGet().isTimedOut(), is(false));
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setLocal(true).setWaitForNodes("2").execute().actionGet().isTimedOut(), is(false));

        // now that we started node1 again, a new master should be elected
        assertThat(clusterService1.state().nodes().masterNode(), is(notNullValue()));
        if (node_2.equals(clusterService1.state().nodes().masterNode().name())) {
            assertThat(testService1.master(), is(true));
            assertThat(testService2.master(), is(false));
        } else {
            assertThat(testService1.master(), is(false));
            assertThat(testService2.master(), is(true));
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
            List<Class<? extends LifecycleComponent>> services = new ArrayList<Class<? extends LifecycleComponent>>(1);
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
        protected void doStart() throws ElasticSearchException {
        }

        @Override
        protected void doStop() throws ElasticSearchException {
        }

        @Override
        protected void doClose() throws ElasticSearchException {
        }

        @Override
        public String executorName() {
            return ThreadPool.Names.SAME;
        }

    }
}
