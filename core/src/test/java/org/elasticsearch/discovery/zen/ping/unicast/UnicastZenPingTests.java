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

package org.elasticsearch.discovery.zen.ping.unicast;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.ping.PingContextProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

public class UnicastZenPingTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;

    private TrackingMockTransportService transportA;
    private TrackingMockTransportService transportB;
    private DiscoveryNode nodeA;
    private DiscoveryNode nodeB;

    @BeforeClass
    public static void createThreadPool() {
        THREAD_POOL = new TestThreadPool(UnicastZenPingTests.class.getName());
    }

    @AfterClass
    public static void destroyThreadPool() throws InterruptedException {
        terminate(THREAD_POOL);
        THREAD_POOL = null;
    }

    @Before
    public void startTransports() {
        transportA = createTransportService();
        nodeA = new DiscoveryNode("node_a", transportA.boundAddress().publishAddress(), Version.CURRENT);
        transportA.setLocalNode(nodeA);

        transportB = createTransportService();
        nodeB = new DiscoveryNode("node_b", transportB.boundAddress().publishAddress(), Version.CURRENT);
        transportB.setLocalNode(nodeB);
    }

    @After
    public void stopTransports() {
        transportA.stop();
        transportB.stop();
    }

    /**
     * Test the Unicast Zen Ping responses when a node is unreachable at first and then becomes
     * reachable. It also ensure that no disconnections are issued to an unreachable node.
     */
    public void testPingUnreachableThenReachableNode() throws Exception {
        try (
                UnicastZenPing zenPingB = createUnicastZenPing(transportB, nodeA.getAddress().toString());
                UnicastZenPing zenPingA = createUnicastZenPing(transportA, nodeB.getAddress().toString());
        ) {
            PingContextProvider contextA = createPingContextProvider(nodeA, null);
            assertNull("Node A must not resolve Node B by address", contextA.nodes().findByAddress(nodeB.getAddress()));
            zenPingA.setPingContextProvider(contextA);
            zenPingA.start();

            PingContextProvider contextB = createPingContextProvider(nodeB, nodeA);
            assertNotNull("Node B must resolve Node A by address", contextB.nodes().findByAddress(nodeA.getAddress()));
            zenPingB.setPingContextProvider(contextB);
            zenPingB.start();

            logger.trace("Node A can't reach Node B");
            transportA.addFailToSendNoConnectRule(nodeB.getAddress());

            logger.trace("Node A pings Node B, no response is expected");
            ZenPing.PingResponse[] pings = zenPingA.pingAndWait(TimeValue.timeValueSeconds(1L));
            assertThat(pings, allOf(notNullValue(), arrayWithSize(0)));

            logger.trace("Node A has no connection to Node B and did not initiate a disconnection");
            assertFalse(transportA.nodeConnected(nodeB));
            assertTrue(transportA.getDisconnects().isEmpty());

            logger.trace("Node A can now reach Node B");
            transportA.clearAllRules();

            logger.trace("Node A pings Node B, one successful ping response is expected");
            pings = zenPingA.pingAndWait(TimeValue.timeValueSeconds(1L));
            assertThat(pings, arrayWithSize(1));

            logger.trace("Node B pings Node A, one successful ping response is expected");
            pings = zenPingB.pingAndWait(TimeValue.timeValueSeconds(1L));
            assertThat(pings, arrayWithSize(1));

            logger.trace("Node B kept the connection to Node A because it has been resolved by address");
            assertBusy(() -> {
                assertTrue(transportB.nodeConnected(nodeA));
                assertTrue(transportB.getDisconnects().isEmpty());
            });

            logger.trace("Node A closed the connection to Node B");
            assertBusy(() -> {
                assertFalse(transportA.nodeConnected(nodeB));
                assertThat(transportA.getDisconnects(), hasItem(nodeB.getAddress()));
            });
        }
    }

    private TrackingMockTransportService createTransportService() {
        MockTcpTransport transport =
                new MockTcpTransport(
                        Settings.EMPTY,
                        THREAD_POOL,
                        BigArrays.NON_RECYCLING_INSTANCE,
                        new NoneCircuitBreakerService(),
                        new NamedWriteableRegistry(emptyList()),
                        new NetworkService(Settings.EMPTY, emptyList()));

        TrackingMockTransportService transportService = new TrackingMockTransportService(Settings.EMPTY, transport, THREAD_POOL);
        transportService.start();
        transportService.acceptIncomingRequests();
        return transportService;
    }

    private UnicastZenPing createUnicastZenPing(TransportService transportService, String... unicastHosts) {
        Settings settings = Settings.builder()
                .putArray(UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey(), unicastHosts)
                .build();
        return new UnicastZenPing(settings, THREAD_POOL, transportService, new ElectMasterService(settings), emptySet());
    }

    private PingContextProvider createPingContextProvider(DiscoveryNode local, DiscoveryNode other) {
        return new PingContextProvider() {
            @Override
            public boolean nodeHasJoinedClusterOnce() {
                return false;
            }

            @Override
            public DiscoveryNodes nodes() {
                DiscoveryNodes.Builder builder = DiscoveryNodes.builder().localNodeId(local.getId()).add(local);
                if (other != null) {
                    builder.add(other);
                }
                return builder.build();
            }
        };
    }

    /**
     * A MockTransportService that tracks the number of disconnect attempts
     **/
    static class TrackingMockTransportService extends MockTransportService {

        private final List<TransportAddress> disconnects = new CopyOnWriteArrayList<>();

        TrackingMockTransportService(Settings settings, Transport transport, ThreadPool threadPool) {
            super(settings, transport, threadPool);
        }

        @Override
        public void disconnectFromNode(DiscoveryNode node) {
            disconnects.add(node.getAddress());
            super.disconnectFromNode(node);
        }

        List<TransportAddress> getDisconnects() {
            return disconnects;
        }
    }
}
