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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportStats;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

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
                transportService.disconnectFromNode(node);
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
        assertThat(transportService.getConnectionManager().size(), equalTo(state.nodes().getSize()));
    }

    private void assertConnected(Iterable<DiscoveryNode> nodes) {
        for (DiscoveryNode node : nodes) {
            assertTrue("not connected to " + node, transportService.nodeConnected(node));
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool(getClass().getName());
        this.transport = new MockTransport();
        transportService = new NoHandshakeTransportService(Settings.EMPTY, transport, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(Settings.EMPTY, buildNewFakeTransportAddress(), UUIDs.randomBase64UUID()), null,
            Collections.emptySet());
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

    private final class NoHandshakeTransportService extends TransportService {

        private NoHandshakeTransportService(Settings settings,
                                            Transport transport,
                                            ThreadPool threadPool,
                                            TransportInterceptor transportInterceptor,
                                            Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                            ClusterSettings clusterSettings,
                                            Set<String> taskHeaders) {
            super(settings, transport, threadPool, transportInterceptor, localNodeFactory, clusterSettings, taskHeaders);
        }

        @Override
        public HandshakeResponse handshake(Transport.Connection connection, long timeout, Predicate<ClusterName> clusterNamePredicate) {
            return new HandshakeResponse(connection.getNode(), new ClusterName(""), Version.CURRENT);
        }
    }

    private final class MockTransport implements Transport {
        private ResponseHandlers responseHandlers = new ResponseHandlers();
        private volatile boolean randomConnectionExceptions = false;

        @Override
        public <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        }

        @Override
        public RequestHandlerRegistry getRequestHandler(String action) {
            return null;
        }

        @Override
        public void addMessageListener(TransportMessageListener listener) {
        }

        @Override
        public boolean removeMessageListener(TransportMessageListener listener) {
            throw new UnsupportedOperationException();
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
        public TransportAddress[] addressesFromString(String address, int perAddressLimit) {
            return new TransportAddress[0];
        }

        @Override
        public Releasable openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> listener) {
            if (profile == null) {
                if (randomConnectionExceptions && randomBoolean()) {
                    listener.onFailure(new ConnectTransportException(node, "simulated"));
                    return () -> {};
                }
            }
            listener.onResponse(new Connection() {
                @Override
                public DiscoveryNode getNode() {
                    return node;
                }

                @Override
                public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                    throws TransportException {

                }

                @Override
                public void addCloseListener(ActionListener<Void> listener) {

                }

                @Override
                public void close() {

                }

                @Override
                public boolean isClosed() {
                    return false;
                }
            });
            return () -> {};
        }

        @Override
        public List<String> getLocalAddresses() {
            return null;
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
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void close() {
        }

        @Override
        public TransportStats getStats() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResponseHandlers getResponseHandlers() {
            return responseHandlers;
        }
    }
}
