/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.discovery.zookeeper;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.zookeeper.ZooKeeperClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.collect.Maps.*;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;


/**
 * @author imotov
 */
public class ZooKeeperDiscoveryTests extends AbstractZooKeeperTests {

    protected Random rand = new Random();

    private Map<String, Node> nodes = newHashMap();

    private Map<String, Client> clients = newHashMap();

    public Node buildNode(String id) {
        return buildNode(id, EMPTY_SETTINGS);
    }

    public Node buildNode(String id, Settings.Builder settings) {
        return buildNode(id, settings.build());
    }

    public Node buildNode(String id, Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(defaultSettings())
                .put(settings)
                .put("name", id)
                .build();

        if (finalSettings.get("gateway.type") == null) {
            // default to non gateway
            finalSettings = settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
        }

        Node node = nodeBuilder()
                .settings(finalSettings)
                .build();
        nodes.put(id, node);
        clients.put(id, node.client());
        return node;
    }

    public Node node(String id) {
        return nodes.get(id);
    }

    public Client client(String id) {
        return clients.get(id);
    }

    public void closeAllNodes() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            node.close();
        }
        nodes.clear();
    }

    @BeforeClass public void addDefaultSettings() {
        putDefaultSettings(ImmutableSettings.settingsBuilder()
                .put(defaultSettings())
                .put("discovery.zookeeper.state_publishing.enabled", true)
                .put("discovery.type", "zoo_keeper")
                .put("transport.type", "local")
        );

    }

    @AfterMethod public void nodesCleanup() {
        closeAllNodes();
    }

    @Test public void testSingleNodeStartup() throws Exception {
        Node node = buildNode("node1");
        final CountDownLatch latch = new CountDownLatch(1);
        clusterService("node1").add(new ClusterStateListener() {
            @Override public void clusterChanged(ClusterChangedEvent event) {
                if (event.localNodeMaster()) {
                    latch.countDown();
                }
            }
        });
        node.start();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));
        node.stop();
    }

    @Test public void testTwoNodeStartup() throws Exception {
        Node node1 = buildNode("node1");
        Node node2 = buildNode("node2");
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch21 = new CountDownLatch(1);
        final CountDownLatch latch22 = new CountDownLatch(1);
        clusterService("node1").add(new ClusterStateListener() {
            @Override public void clusterChanged(ClusterChangedEvent event) {
                if (event.localNodeMaster()) {
                    latch1.countDown();
                }
            }
        });
        clusterService("node2").add(new ClusterStateListener() {
            @Override public void clusterChanged(ClusterChangedEvent event) {
                if (!event.localNodeMaster()) {
                    latch21.countDown();
                } else {
                    latch22.countDown();
                }
            }
        });
        node1.start();
        assertThat(latch1.await(1, TimeUnit.SECONDS), equalTo(true));
        node2.start();
        assertThat(latch21.await(1, TimeUnit.SECONDS), equalTo(true));
        assertThat(latch22.getCount(), equalTo(1L));
        node1.stop();
        assertThat(latch22.await(1, TimeUnit.SECONDS), equalTo(true));
        node2.stop();
    }

    @Test public void testJoinAndLeave() throws Exception {
        Node node1 = buildNode("node1");
        Node node2 = buildNode("node2");
        Node node3 = buildNode("node3");
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final CountDownLatch latch3 = new CountDownLatch(1);
        final CountDownLatch latch1Node3Gone = new CountDownLatch(1);
        final CountDownLatch latch2Node3Gone = new CountDownLatch(1);
        clusterService("node1").add(new ClusterStateListener() {
            @Override public void clusterChanged(ClusterChangedEvent event) {
                if (event.localNodeMaster()) {
                    latch1.countDown();
                }
                if (event.nodesRemoved() && !nodeExists("node3", event.state())) {
                    logger.trace("Node3 is removed from master. Current nodes: [{}]", event.state().nodes());
                    latch1Node3Gone.countDown();
                }
            }
        });
        clusterService("node2").add(new ClusterStateListener() {
            @Override public void clusterChanged(ClusterChangedEvent event) {
                if (!event.localNodeMaster()) {
                    latch2.countDown();
                }
                if (event.nodesRemoved() && !nodeExists("node3", event.state())) {
                    logger.trace("Node3 is removed from node2. Current nodes: [{}]", event.state().nodes());
                    latch2Node3Gone.countDown();
                }
            }
        });
        clusterService("node3").add(new ClusterStateListener() {
            @Override public void clusterChanged(ClusterChangedEvent event) {
                if (!event.localNodeMaster()) {
                    latch3.countDown();
                }
            }
        });
        node1.start();
        // Wait for the node 1 to start to ensure that it's master
        assertThat(latch1.await(1, TimeUnit.SECONDS), equalTo(true));
        node2.start();
        node3.start();
        // Wait for the node 2 and 3 to start
        assertThat(latch2.await(1, TimeUnit.SECONDS), equalTo(true));
        assertThat(latch3.await(1, TimeUnit.SECONDS), equalTo(true));
        node3.stop();
        // Wait for the node 3 to disappear from master
        assertThat(latch1Node3Gone.await(1, TimeUnit.SECONDS), equalTo(true));
        // Wait for the node 3 to disappear from node 2
        assertThat(latch2Node3Gone.await(1, TimeUnit.SECONDS), equalTo(true));
        node1.stop();
        node2.stop();
    }


    @Test public void testClientCannotBecomeMaster() throws Exception {
        buildNode("client", ImmutableSettings.settingsBuilder()
                .put("node.client", true)
                .put("discovery.initial_state_timeout", 100, TimeUnit.MILLISECONDS)
        );
        buildNode("node1");

        // Start client before master
        ClusterStateMonitor clientMonitor = new ClusterStateMonitor("client");
        ClusterStateMonitor nodeMonitor = new ClusterStateMonitor("node1");
        node("client").start();
        node("node1").start();
        ClusterState nodeState = nodeMonitor.await();
        ClusterState clientState = clientMonitor.await();
        assertThat(clientState.nodes().masterNode().name(), equalTo("node1"));
        assertThat(nodeState.nodes().masterNode().name(), equalTo("node1"));

        // Shutdown master node and wait until state is updated
        clientMonitor = new ClusterStateMonitor("client", new ClusterStateCondition() {
            @Override public boolean check(ClusterChangedEvent event) {
                return event.state().nodes().masterNode() == null;
            }
        });
        node("node1").stop();
        clientState = clientMonitor.await();
        assertThat(clientState.nodes().masterNode(), nullValue());

        clientMonitor = new ClusterStateMonitor("client");
        buildNode("node2").start();
        clientState = clientMonitor.await();
        assertThat(clientState.nodes().masterNode().name(), equalTo("node2"));
    }

    @Test public void testMasterReelection() throws Exception {
        buildNode("client", ImmutableSettings.settingsBuilder()
                .put("node.client", true)
                .put("discovery.initial_state_timeout", 100, TimeUnit.MILLISECONDS)
        );
        buildNode("node1");
        buildNode("node2");

        // Ensure node1 is master
        ClusterStateMonitor nodeMonitor = new ClusterStateMonitor("node1");
        node("node1").start();
        assertThat(nodeMonitor.await().nodes().masterNode().name(), equalTo("node1"));

        // Start all other nodes
        ClusterStateMonitor clientMonitor = new ClusterStateMonitor("client");
        nodeMonitor = new ClusterStateMonitor("node2");
        node("node2").start();
        node("client").start();
        assertThat(nodeMonitor.await().nodes().masterNode().name(), equalTo("node1"));
        assertThat(clientMonitor.await().nodes().masterNode().name(), equalTo("node1"));

        // Shutdown master node and wait until another node is master
        clientMonitor = new ClusterStateMonitor("client", new ClusterStateCondition() {
            @Override public boolean check(ClusterChangedEvent event) {
                return event.state().nodes().masterNode() != null;
            }
        });
        node("node1").stop();
        assertThat(clientMonitor.await().nodes().masterNode().name(), equalTo("node2"));
    }

    @Test public void testSessionExpiration() throws Exception {
        buildNode("node1");

        // Ensure node1 is master
        ClusterStateMonitor nodeMonitor = new ClusterStateMonitor("node1");
        node("node1").start();
        assertThat(nodeMonitor.await().nodes().masterNode().name(), equalTo("node1"));

        nodeMonitor = new ClusterStateMonitor("node1", new ClusterStateCondition() {
            @Override public boolean check(ClusterChangedEvent event) {
                return event.state().nodes().masterNode() != null;
            }
        });
        expireSession("node1");
        assertThat(nodeMonitor.await(5, TimeUnit.SECONDS).nodes().masterNode().name(), equalTo("node1"));
    }

    @Test public void testMasterSwitchDuringSessionExpiration() throws Exception {
        buildNode("node1");
        buildNode("node2");

        // Ensure node1 is master
        ClusterStateMonitor nodeMonitor = new ClusterStateMonitor("node1");
        node("node1").start();
        assertThat(nodeMonitor.await().nodes().masterNode().name(), equalTo("node1"));

        // Wait for the second node to start
        nodeMonitor = new ClusterStateMonitor("node2");
        node("node2").start();
        assertThat(nodeMonitor.await().nodes().masterNode().name(), equalTo("node1"));

        // Kill the session for the first node and wait for it to switch to the second node as a master
        nodeMonitor = new ClusterStateMonitor("node1", new ClusterStateCondition() {
            @Override public boolean check(ClusterChangedEvent event) {
                return event.state().nodes().masterNode() != null;
            }
        });
        expireSession("node1");
        assertThat(nodeMonitor.await(5, TimeUnit.SECONDS).nodes().masterNode().name(), equalTo("node2"));
    }


    // TODO: need a faster test
    @Test(enabled = false) public void testIndexingWithNodeFailures() throws Exception {

        final int nodeCount = 5;
        final long recordCount = 500;
        final int indexCount = 5;
        final AtomicBoolean done = new AtomicBoolean();

        for (int i = 0; i < nodeCount; i++) {
            buildNode("node" + i).start();
        }

        buildNode("client", ImmutableSettings.settingsBuilder()
                .put("node.client", true)
                .put("discovery.initial_state_timeout", 1000, TimeUnit.MILLISECONDS)
        ).start();


        Thread failureThreads = new Thread() {
            public void run() {
                try {
                    while (!done.get()) {
                        Thread.sleep(1000);
                        waitForGreen("client");
                        restartNode(nodeCount);
                    }
                } catch (InterruptedException ex) {
                    // Ignore
                }
            }
        };


        for (int i = 0; i < recordCount; i++) {
            boolean retry = true;
            while (retry) {
                retry = false;
                try {
                    client("client").prepareIndex("test" + (i % indexCount), "test", Integer.toString(i))
                            .setSource(jsonBuilder()
                                    .startObject()
                                    .field("user", "kimchy")
                                    .field("postDate", new Date())
                                    .field("message", "Message number " + i)
                                    .endObject()
                            )
                            .setRefresh(true)
                            .execute()
                            .actionGet();
                    Thread.sleep(10);
                    if (i % 10 == 0) {
                        logHealth("client");
                    }
                    if (i == indexCount) {
                        // Start failure after all indices are created
                        failureThreads.start();
                    }
                } catch (ClusterBlockException ex) {
                    retry = true;
                }
            }
        }

        done.set(true);
        failureThreads.interrupt();
        failureThreads.join();

        assertThat(countResults(indexCount), equalTo(recordCount));
    }

    private long countResults(int indexCount) throws InterruptedException {
        long sum = 0;
        for (int i = 0; i < indexCount; i++) {
            long count = count("client", "test" + i);
            logger.info("Found {} in the index {}", count, i);
            sum += count;

        }
        return sum;
    }


    @Test public void testNewMasterShouldPreserveState() throws Exception {
        buildNode("node1");
        buildNode("node2");
        buildNode("node3", ImmutableSettings.settingsBuilder()
                .put("node.master", false)
        );

        // Ensure node1 is master
        ClusterStateMonitor nodeMonitor = new ClusterStateMonitor("node1");
        node("node1").start();
        assertThat(nodeMonitor.await().nodes().masterNode().name(), equalTo("node1"));

        // Wait for the second node to start
        nodeMonitor = new ClusterStateMonitor("node2");
        node("node2").start();
        assertThat(nodeMonitor.await().nodes().masterNode().name(), equalTo("node1"));

        // Wait for the third node to start
        nodeMonitor = new ClusterStateMonitor("node3");
        node("node3").start();
        assertThat(nodeMonitor.await().nodes().masterNode().name(), equalTo("node1"));

        // Kill the session for the first node and wait for it to switch to the second node as a master
        nodeMonitor = new ClusterStateMonitor("node2", new ClusterStateCondition() {
            @Override public boolean check(ClusterChangedEvent event) {
                return event.state().nodes().masterNode() != null;
            }
        });
        node("node1").stop();
        ClusterState state = nodeMonitor.await();
        assertThat(state.nodes().masterNode().name(), equalTo("node2"));
        assertThat(nodeExists("node3", state), equalTo(true));

    }

    private long count(String id, String index) throws InterruptedException {

        CountResponse countResponse = client(id).prepareCount(index)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute()
                .actionGet();

        return countResponse.count();
    }

    private void logHealth(String id) throws InterruptedException {
        ClusterHealthResponse response = client(id).admin().cluster().prepareHealth().execute().actionGet();
        logger.info("Health: [{}] shards A/P/I {}/{}/{} indices: {} ", response.status(),
                response.activeShards(), response.activePrimaryShards(),
                response.initializingShards(),
                response.indices().size()
        );

    }

    private void waitForGreen(String id) throws InterruptedException {
        while (true) {
            ClusterHealthResponse response = client(id).admin().cluster().prepareHealth().execute().actionGet();
            if (response.status() == ClusterHealthStatus.GREEN) {
                return;
            }
            Thread.sleep(100);
        }
    }

    private void restartNode(int nodeCount) throws InterruptedException {
        int nodeToRestart = rand.nextInt(nodeCount);
        logger.info("Restarting node [{}]", nodeToRestart);
        node("node" + nodeToRestart).stop();
        Thread.sleep(200);
        node("node" + nodeToRestart).start();
    }

    private ClusterService clusterService(String id) {
        InternalNode node = (InternalNode) node(id);
        return node.injector().getInstance(ClusterService.class);
    }

    private ZooKeeperClient zooKeeperClient(String id) {
        InternalNode node = (InternalNode) node(id);
        return node.injector().getInstance(ZooKeeperClient.class);
    }

    private boolean nodeExists(String id, ClusterState state) {
        for (DiscoveryNode node : state.nodes()) {
            if (node.name().equals(id)) {
                return true;
            }
        }
        return false;

    }

    private interface ClusterStateCondition {
        public boolean check(ClusterChangedEvent event);
    }

    private class ClusterStateMonitor {
        private final CountDownLatch latchNode = new CountDownLatch(1);
        private ClusterStateListener clusterStateListener;
        private volatile ClusterState state;
        private final ClusterStateCondition condition;

        public ClusterStateMonitor(final String id) {
            this(id, null);
        }

        public ClusterStateMonitor(final String id, ClusterStateCondition condition) {
            this.condition = condition;
            clusterStateListener = new ClusterStateListener() {
                @Override public void clusterChanged(ClusterChangedEvent event) {
                    if (checkCondition(event)) {
                        logger.info("clusterChangedEvent {} state {} ", event.source(), state);
                        state = event.state();
                        clusterService(id).remove(this);
                        latchNode.countDown();
                    } else {
                        logger.info("Event {} with new state {} is ignored", event.source(), state);
                    }
                }
            };
            clusterService(id).add(clusterStateListener);
        }

        private boolean checkCondition(ClusterChangedEvent event) {
            return condition == null || condition.check(event);
        }

        public ClusterState await() throws InterruptedException {
            return await(1, TimeUnit.SECONDS);
        }

        public ClusterState await(long timeout, TimeUnit timeUnit) throws InterruptedException {
            assertThat(latchNode.await(timeout, timeUnit), equalTo(true));
            return state;
        }
    }

    private void expireSession(String id) {
        logger.info("Disconnecting node {} from ZooKeeper", id);
        embeddedZooKeeperService.expireSession(zooKeeperClient(id).sessionId());
    }

}
