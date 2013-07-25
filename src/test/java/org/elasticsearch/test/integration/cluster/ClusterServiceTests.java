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

package org.elasticsearch.test.integration.cluster;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
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
public class ClusterServiceTests extends AbstractZenNodesTests {

    @After
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testTimeoutUpdateTask() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.zen.minimum_master_nodes", 1)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .build();

        InternalNode node1 = (InternalNode) startNode("node1", settings);
        ClusterService clusterService1 = node1.injector().getInstance(ClusterService.class);
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
    public void testPendingUpdateTask() throws Exception {
        InternalNode node1 = (InternalNode) startNode("node1");
        Client client = startNode("client-node", settingsBuilder().put("node.client", true).build()).client();

        ClusterService clusterService = node1.injector().getInstance(ClusterService.class);
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
        PendingClusterTasksResponse response = client.admin().cluster().preparePendingClusterTasks().execute().actionGet();
        assertThat(response.pendingTasks().size(), equalTo(9));
        for (PendingClusterTask task : response) {
            assertTrue(controlSources.remove(task.source().string()));
        }
        assertTrue(controlSources.isEmpty());
        block1.countDown();
        invoked2.await();

        pendingClusterTasks = clusterService.pendingTasks();
        assertThat(pendingClusterTasks, empty());
        response = client.admin().cluster().preparePendingClusterTasks().execute().actionGet();
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

        response = client.admin().cluster().preparePendingClusterTasks().execute().actionGet();
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
                .put("discovery.zen.minimum_master_nodes", 1)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("plugin.types", TestPlugin.class.getName())
                .build();

        InternalNode node1 = (InternalNode) startNode("node1", settings);
        ClusterService clusterService1 = node1.injector().getInstance(ClusterService.class);
        MasterAwareService testService1 = node1.injector().getInstance(MasterAwareService.class);

        // the first node should be a master as the minimum required is 1
        assertThat(clusterService1.state().nodes().masterNode(), notNullValue());
        assertThat(clusterService1.state().nodes().localNodeMaster(), is(true));
        assertThat(testService1.master(), is(true));

        InternalNode node2 = (InternalNode) startNode("node2", settings);
        ClusterService clusterService2 = node2.injector().getInstance(ClusterService.class);
        MasterAwareService testService2 = node2.injector().getInstance(MasterAwareService.class);

        ClusterHealthResponse clusterHealth = node2.client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // the second node should not be the master as node1 is already the master.
        assertThat(clusterService2.state().nodes().localNodeMaster(), is(false));
        assertThat(testService2.master(), is(false));

        closeNode("node1");
        clusterHealth = node2.client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("1").execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // now that node1 is closed, node2 should be elected as master
        assertThat(clusterService2.state().nodes().localNodeMaster(), is(true));
        assertThat(testService2.master(), is(true));

        Settings newSettings = settingsBuilder()
                .put("discovery.zen.minimum_master_nodes", 2)
                .build();
        node2.client().admin().cluster().prepareUpdateSettings().setTransientSettings(newSettings).execute().actionGet();
        Thread.sleep(200);

        // there should not be any master as the minimum number of required eligible masters is not met
        assertThat(clusterService2.state().nodes().masterNode(), is(nullValue()));
        assertThat(testService2.master(), is(false));


        node1 = (InternalNode) startNode("node1", settings);
        clusterService1 = node1.injector().getInstance(ClusterService.class);
        testService1 = node1.injector().getInstance(MasterAwareService.class);

        clusterHealth = node2.client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // now that we started node1 again, a new master should be elected
        assertThat(clusterService1.state().nodes().masterNode(), is(notNullValue()));
        if ("node1".equals(clusterService1.state().nodes().masterNode().name())) {
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
            logger.info("on master [" + clusterService.state().nodes().localNode() + "]");
            master = true;
        }

        @Override
        public void offMaster() {
            logger.info("off master [" + clusterService.state().nodes().localNode() + "]");
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
