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
package org.elasticsearch.discovery.zen;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.membership.MembershipAction;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

@TestLogging("discovery.zen:TRACE")
public class NodeJoinControllerTests extends ElasticsearchTestCase {

    TestClusterService clusterService;
    NodeJoinController nodeJoinController;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = new TestClusterService();
        final DiscoveryNodes initialNodes = clusterService.state().nodes();
        final DiscoveryNode localNode = initialNodes.localNode();
        // make sure we have a master
        clusterService.setState(ClusterState.builder(clusterService.state()).nodes(DiscoveryNodes.builder(initialNodes).masterNodeId(localNode.id())));
        nodeJoinController = new NodeJoinController(clusterService, new NoopRoutingService(Settings.EMPTY),
                new DiscoverySettings(Settings.EMPTY, new NodeSettingsService(Settings.EMPTY)), logger);
    }

    public void testNormalConcurrentJoins() throws InterruptedException {
        Thread[] threads = new Thread[3 + randomInt(5)];
        ArrayList<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(clusterService.localNode());
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final List<Throwable> backgroundExceptions = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads.length; i++) {
            final DiscoveryNode node = newNode(i);
            final int iterations = rarely() ? randomIntBetween(1, 4) : 1;
            nodes.add(node);
            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    logger.error("unexpected error in join thread", t);
                    backgroundExceptions.add(t);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        logger.debug("{} joining", node);
                        joinNode(node);
                    }
                }
            }, "t_" + i);
            threads[i].start();
        }

        logger.info("--> waiting for joins to complete");
        for (Thread thread : threads) {
            thread.join();
        }

        assertNodesInCurrentState(nodes);
    }

    public void testSimpleJoinAccumulation() throws InterruptedException {
        List<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(clusterService.localNode());

        int nodeId = 0;
        for (int i = randomInt(5); i > 0; i--) {
            DiscoveryNode node = newNode(nodeId++);
            nodes.add(node);
            joinNode(node);
        }
        nodeJoinController.startAccumulatingJoins();
        for (int i = randomInt(5); i > 0; i--) {
            DiscoveryNode node = newNode(nodeId++);
            nodes.add(node);
            joinNode(node);
        }
        nodeJoinController.stopAccumulatingJoins();
        for (int i = randomInt(5); i > 0; i--) {
            DiscoveryNode node = newNode(nodeId++);
            nodes.add(node);
            joinNode(node);
        }
        assertNodesInCurrentState(nodes);
    }

    public void testNewClusterStateOnExistingNodeJoin() throws InterruptedException {
        ClusterState state = clusterService.state();
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(state.nodes());
        final DiscoveryNode other_node = new DiscoveryNode("other_node", DummyTransportAddress.INSTANCE, Version.CURRENT);
        nodesBuilder.put(other_node);
        clusterService.setState(ClusterState.builder(state).nodes(nodesBuilder));

        state = clusterService.state();
        joinNode(other_node);
        assertTrue("failed to publish a new state upon existing join", clusterService.state() != state);
    }


    static class NoopRoutingService extends RoutingService {

        public NoopRoutingService(Settings settings) {
            super(settings, null, null, null);
        }

        @Override
        protected void performReroute(String reason) {

        }
    }

    protected void assertNodesInCurrentState(List<DiscoveryNode> expectedNodes) {
        DiscoveryNodes discoveryNodes = clusterService.state().nodes();
        assertThat(discoveryNodes.prettyPrint() + "\nexpected: " + expectedNodes.toString(), discoveryNodes.size(), equalTo(expectedNodes.size()));
        for (DiscoveryNode node : expectedNodes) {
            assertThat("missing " + node + "\n" + discoveryNodes.prettyPrint(), discoveryNodes.get(node.id()), equalTo(node));
        }
    }

    private Future<> joinNodeAsync(final DiscoveryNode node) throws InterruptedException {
    }

    private void joinNode(final DiscoveryNode node) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> backgroundException = new AtomicReference<>();
        nodeJoinController.handleJoinRequest(node, new MembershipAction.JoinCallback() {
            @Override
            public void onSuccess() {
                logger.debug("node join completed for {}", node);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("unexpected error while joining {}", t, node);
                backgroundException.set(t);
                latch.countDown();
            }
        });
        latch.await();
        ExceptionsHelper.reThrowIfNotNull(backgroundException.get());
    }

    protected DiscoveryNode newNode(int i) {
        return new DiscoveryNode("node_" + i, new LocalTransportAddress("test_" + i), Version.CURRENT);
    }
}
