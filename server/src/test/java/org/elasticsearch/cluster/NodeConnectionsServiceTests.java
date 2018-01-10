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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportStats;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class NodeConnectionsServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private MockTransport transport;
    private TransportService transportService;

    private List<DiscoveryNode> generateNodes() {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int i = randomIntBetween(20, 50); i > 0; i--) {
            Set<DiscoveryNode.Role> roles = new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values())));
            nodes.add(new DiscoveryNode("node_" + i, "" + i, buildNewFakeTransportAddress(), Collections.emptyMap(),
                    roles, Version.CURRENT));
        }
        return nodes;
    }

    private ClusterState clusterStateFromNodes(List<DiscoveryNode> nodes) {
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodes) {
            builder.add(node);
        }
        return ClusterState.builder(new ClusterName("test")).nodes(builder).build();
    }

    public void testConnectAndDisconnect() {
        List<DiscoveryNode> nodes = generateNodes();
        NodeConnectionsService service = new NodeConnectionsService(Settings.EMPTY, threadPool, transportService);

        ClusterState current = clusterStateFromNodes(Collections.emptyList());
        ClusterChangedEvent event = new ClusterChangedEvent("test", clusterStateFromNodes(randomSubsetOf(nodes)), current);

        service.connectToNodes(event.state().nodes());
        assertConnected(event.state().nodes());

        service.disconnectFromNodesExcept(event.state().nodes());
        assertConnectedExactlyToNodes(event.state());

        current = event.state();
        event = new ClusterChangedEvent("test", clusterStateFromNodes(randomSubsetOf(nodes)), current);

        service.connectToNodes(event.state().nodes());
        assertConnected(event.state().nodes());

        service.disconnectFromNodesExcept(event.state().nodes());
        assertConnectedExactlyToNodes(event.state());
    }


    public void testReconnect() {
        List<DiscoveryNode> nodes = generateNodes();
        NodeConnectionsService service = new NodeConnectionsService(Settings.EMPTY, threadPool, transportService);

        ClusterState current = clusterStateFromNodes(Collections.emptyList());
        ClusterChangedEvent event = new ClusterChangedEvent("test", clusterStateFromNodes(randomSubsetOf(nodes)), current);

        transport.randomConnectionExceptions = true;

        service.connectToNodes(event.state().nodes());

        for (int i = 0; i < 3; i++) {
            // simulate disconnects
            for (DiscoveryNode node : randomSubsetOf(nodes)) {
                transport.disconnectFromNode(node);
            }
            service.new ConnectionChecker().run();
        }

        // disable exceptions so things can be restored
        transport.randomConnectionExceptions = false;
        service.new ConnectionChecker().run();
        assertConnectedExactlyToNodes(event.state());
    }

    private void assertConnectedExactlyToNodes(ClusterState state) {
        assertConnected(state.nodes());
        assertThat(transport.connectedNodes.size(), equalTo(state.nodes().getSize()));
    }

    private void assertConnected(Iterable<DiscoveryNode> nodes) {
        for (DiscoveryNode node : nodes) {
            assertTrue("not connected to " + node, transport.connectedNodes.contains(node));
        }
    }

    private void assertNotConnected(Iterable<DiscoveryNode> nodes) {
        for (DiscoveryNode node : nodes) {
            assertFalse("still connected to " + node, transport.connectedNodes.contains(node));
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool(getClass().getName());
        this.transport = new MockTransport();
        transportService = new TransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(Settings.EMPTY, buildNewFakeTransportAddress(), UUIDs.randomBase64UUID()), null);
        transportService.start();
        transportService.acceptIncomingRequests();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        transportService.stop();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        super.tearDown();
    }

    final class MockTransport implements Transport {
        private final AtomicLong requestId = new AtomicLong();
        Set<DiscoveryNode> connectedNodes = ConcurrentCollections.newConcurrentSet();
        volatile boolean randomConnectionExceptions = false;

        @Override
        public void setTransportService(TransportService service) {
        }

        @Override
        public BoundTransportAddress boundAddress() {
            return null;
        }

        @Override
        public Map<String, BoundTransportAddress> profileBoundAddresses() {
            return null;
        }

        @Override
        public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
            return new TransportAddress[0];
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            return connectedNodes.contains(node);
        }

        @Override
        public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                                  CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator)
            throws ConnectTransportException {
            if (connectionProfile == null) {
                if (connectedNodes.contains(node) == false && randomConnectionExceptions && randomBoolean()) {
                    throw new ConnectTransportException(node, "simulated");
                }
                connectedNodes.add(node);
            }
        }

        @Override
        public void disconnectFromNode(DiscoveryNode node) {
            connectedNodes.remove(node);
        }

        @Override
        public Connection getConnection(DiscoveryNode node) {
            return new Connection() {
                @Override
                public DiscoveryNode getNode() {
                    return node;
                }

                @Override
                public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                    throws IOException, TransportException {

                }

                @Override
                public void close() throws IOException {

                }
            };
        }

        @Override
        public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
            return getConnection(node);
        }

        @Override
        public List<String> getLocalAddresses() {
            return null;
        }

        @Override
        public long newRequestId() {
            return requestId.incrementAndGet();
        }

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {
        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}

        @Override
        public void close() {}

        @Override
        public TransportStats getStats() {
            throw new UnsupportedOperationException();
        }
    }
}
