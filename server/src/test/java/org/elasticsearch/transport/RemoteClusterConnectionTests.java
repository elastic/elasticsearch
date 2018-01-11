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
package org.elasticsearch.transport;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class RemoteClusterConnectionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes, Version version) {
        return startTransport(id, knownNodes, version, threadPool);
    }

    public static MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes, Version version, ThreadPool threadPool) {
        return startTransport(id, knownNodes, version, threadPool, Settings.EMPTY);
    }

    public static MockTransportService startTransport(
            final String id,
            final List<DiscoveryNode> knownNodes,
            final Version version,
            final ThreadPool threadPool,
            final Settings settings) {
        boolean success = false;
        final Settings s = Settings.builder().put(settings).put("node.name", id).build();
        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(s);
        MockTransportService newService = MockTransportService.createNewService(s, version, threadPool, null);
        try {
            newService.registerRequestHandler(ClusterSearchShardsAction.NAME, ClusterSearchShardsRequest::new, ThreadPool.Names.SAME,
                    (request, channel) -> {
                        channel.sendResponse(new ClusterSearchShardsResponse(new ClusterSearchShardsGroup[0],
                                knownNodes.toArray(new DiscoveryNode[0]), Collections.emptyMap()));
                    });
            newService.registerRequestHandler(ClusterStateAction.NAME, ClusterStateRequest::new, ThreadPool.Names.SAME,
                    (request, channel) -> {
                        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
                        for (DiscoveryNode node : knownNodes) {
                            builder.add(node);
                        }
                        ClusterState build = ClusterState.builder(clusterName).nodes(builder.build()).build();
                        channel.sendResponse(new ClusterStateResponse(clusterName, build, 0L));
                    });
            newService.start();
            newService.acceptIncomingRequests();
            success = true;
            return newService;
        } finally {
            if (success == false) {
                newService.close();
            }
        }
    }

    public void testDiscoverSingleNode() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    updateSeedNodes(connection, Arrays.asList(seedNode));
                    assertTrue(service.nodeConnected(seedNode));
                    assertTrue(service.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }

    public void testDiscoverSingleNodeWithIncompatibleSeed() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService incompatibleTransport = startTransport("incompat_seed_node", knownNodes, Version.fromString("2.0.0"));
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            DiscoveryNode incompatibleSeedNode = incompatibleTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            knownNodes.add(incompatibleTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            List<DiscoveryNode> seedNodes = Arrays.asList(incompatibleSeedNode, seedNode);
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, Integer.MAX_VALUE, n -> true)) {
                    updateSeedNodes(connection, seedNodes);
                    assertTrue(service.nodeConnected(seedNode));
                    assertTrue(service.nodeConnected(discoverableNode));
                    assertFalse(service.nodeConnected(incompatibleSeedNode));
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }

    public void testNodeDisconnected() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT);
             MockTransportService spareTransport = startTransport("spare_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            DiscoveryNode spareNode = spareTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    updateSeedNodes(connection, Arrays.asList(seedNode));
                    assertTrue(service.nodeConnected(seedNode));
                    assertTrue(service.nodeConnected(discoverableNode));
                    assertFalse(service.nodeConnected(spareNode));
                    knownNodes.add(spareNode);
                    CountDownLatch latchDisconnect = new CountDownLatch(1);
                    CountDownLatch latchConnected = new CountDownLatch(1);
                    service.addConnectionListener(new TransportConnectionListener() {
                        @Override
                        public void onNodeDisconnected(DiscoveryNode node) {
                            if (node.equals(discoverableNode)) {
                                latchDisconnect.countDown();
                            }
                        }

                        @Override
                        public void onNodeConnected(DiscoveryNode node) {
                            if (node.equals(spareNode)) {
                                latchConnected.countDown();
                            }
                        }
                    });

                    discoverableTransport.close();
                    // now make sure we try to connect again to other nodes once we got disconnected
                    assertTrue(latchDisconnect.await(10, TimeUnit.SECONDS));
                    assertTrue(latchConnected.await(10, TimeUnit.SECONDS));
                    assertTrue(service.nodeConnected(spareNode));
                }
            }
        }
    }

    public void testFilterDiscoveredNodes() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            DiscoveryNode rejectedNode = randomBoolean() ? seedNode : discoverableNode;
            Collections.shuffle(knownNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> n.equals(rejectedNode) == false)) {
                    updateSeedNodes(connection, Arrays.asList(seedNode));
                    if (rejectedNode.equals(seedNode)) {
                        assertFalse(service.nodeConnected(seedNode));
                        assertTrue(service.nodeConnected(discoverableNode));
                    } else {
                        assertTrue(service.nodeConnected(seedNode));
                        assertFalse(service.nodeConnected(discoverableNode));
                    }
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }

    private void updateSeedNodes(RemoteClusterConnection connection, List<DiscoveryNode> seedNodes) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        ActionListener<Void> listener = ActionListener.wrap(x -> latch.countDown(), x -> {
            exceptionAtomicReference.set(x);
            latch.countDown();
        });
        connection.updateSeedNodes(seedNodes, listener);
        latch.await();
        if (exceptionAtomicReference.get() != null) {
            throw exceptionAtomicReference.get();
        }
    }

    public void testConnectWithIncompatibleTransports() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.fromString("2.0.0"))) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    expectThrows(Exception.class, () -> updateSeedNodes(connection, Arrays.asList(seedNode)));
                    assertFalse(service.nodeConnected(seedNode));
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }

    public void testRemoteConnectionVersionMatchesTransportConnectionVersion() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Version previousVersion = VersionUtils.getPreviousVersion();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, previousVersion);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {

            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            assertThat(seedNode, notNullValue());
            knownNodes.add(seedNode);

            DiscoveryNode oldVersionNode = discoverableTransport.getLocalDiscoNode();
            assertThat(oldVersionNode, notNullValue());
            knownNodes.add(oldVersionNode);

            assertThat(seedNode.getVersion(), not(equalTo(oldVersionNode.getVersion())));
            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                final Transport.Connection seedConnection = new Transport.Connection() {
                    @Override
                    public DiscoveryNode getNode() {
                        return seedNode;
                    }

                    @Override
                    public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                        throws IOException, TransportException {
                        // no-op
                    }

                    @Override
                    public void close() throws IOException {
                        // no-op
                    }
                };
                service.addDelegate(seedNode.getAddress(), new MockTransportService.DelegateTransport(service.getOriginalTransport()) {
                    @Override
                    public Connection getConnection(DiscoveryNode node) {
                        if (node == seedNode) {
                            return seedConnection;
                        }
                        return super.getConnection(node);
                    }
                });
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    connection.addConnectedNode(seedNode);
                    for (DiscoveryNode node : knownNodes) {
                        final Transport.Connection transportConnection = connection.getConnection(node);
                        assertThat(transportConnection.getVersion(), equalTo(previousVersion));
                    }
                    assertThat(knownNodes, iterableWithSize(2));
                }
            }
        }
    }

    @SuppressForbidden(reason = "calls getLocalHost here but it's fine in this case")
    public void testSlowNodeCanBeCanceled() throws IOException, InterruptedException {
        try (ServerSocket socket = new MockServerSocket()) {
            socket.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0), 1);
            socket.setReuseAddress(true);
            DiscoveryNode seedNode = new DiscoveryNode("TEST", new TransportAddress(socket.getInetAddress(),
                socket.getLocalPort()), emptyMap(),
                emptySet(), Version.CURRENT);
            CountDownLatch acceptedLatch = new CountDownLatch(1);
            CountDownLatch closeRemote = new CountDownLatch(1);
            Thread t = new Thread() {
                @Override
                public void run() {
                    try (Socket accept = socket.accept()) {
                        acceptedLatch.countDown();
                        closeRemote.await();
                    } catch (IOException e) {
                        // that's fine we might close
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
            t.start();

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                CountDownLatch listenerCalled = new CountDownLatch(1);
                AtomicReference<Exception> exceptionReference = new AtomicReference<>();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    ActionListener<Void> listener = ActionListener.wrap(x -> {
                        listenerCalled.countDown();
                        fail("expected exception");
                    }, x -> {
                        exceptionReference.set(x);
                        listenerCalled.countDown();
                    });
                    connection.updateSeedNodes(Arrays.asList(seedNode), listener);
                    acceptedLatch.await();
                    connection.close(); // now close it, this should trigger an interrupt on the socket and we can move on
                    assertTrue(connection.assertNoRunningConnections());
                }
                closeRemote.countDown();
                listenerCalled.await();
                assertNotNull(exceptionReference.get());
                expectThrows(CancellableThreads.ExecutionCancelledException.class, () -> {throw exceptionReference.get();});

            }
        }
    }

    public void testFetchShards() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                List<DiscoveryNode> nodes = Collections.singletonList(seedNode);
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    nodes, service, Integer.MAX_VALUE, n -> true)) {
                    if (randomBoolean()) {
                        updateSeedNodes(connection, nodes);
                    }
                    if (randomBoolean()) {
                        connection.updateSkipUnavailable(randomBoolean());
                    }
                    SearchRequest request = new SearchRequest("test-index");
                    CountDownLatch responseLatch = new CountDownLatch(1);
                    AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                    AtomicReference<Exception> failReference = new AtomicReference<>();
                    ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest("test-index")
                        .indicesOptions(request.indicesOptions()).local(true).preference(request.preference())
                        .routing(request.routing());
                    connection.fetchSearchShards(searchShardsRequest,
                            new LatchedActionListener<>(ActionListener.wrap(reference::set, failReference::set), responseLatch));
                    responseLatch.await();
                    assertNull(failReference.get());
                    assertNotNull(reference.get());
                    ClusterSearchShardsResponse clusterSearchShardsResponse = reference.get();
                    assertEquals(knownNodes, Arrays.asList(clusterSearchShardsResponse.getNodes()));
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }

    public void testFetchShardsSkipUnavailable() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            knownNodes.add(seedNode);
            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                        Collections.singletonList(seedNode), service, Integer.MAX_VALUE, n -> true)) {

                    SearchRequest request = new SearchRequest("test-index");
                    ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest("test-index")
                            .indicesOptions(request.indicesOptions()).local(true).preference(request.preference())
                            .routing(request.routing());
                    {
                        CountDownLatch responseLatch = new CountDownLatch(1);
                        AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                        AtomicReference<Exception> failReference = new AtomicReference<>();
                        connection.fetchSearchShards(searchShardsRequest,
                                new LatchedActionListener<>(ActionListener.wrap(reference::set, failReference::set), responseLatch));
                        assertTrue(responseLatch.await(1, TimeUnit.SECONDS));
                        assertNull(failReference.get());
                        assertNotNull(reference.get());
                        ClusterSearchShardsResponse response = reference.get();
                        assertTrue(response != ClusterSearchShardsResponse.EMPTY);
                        assertEquals(knownNodes, Arrays.asList(response.getNodes()));
                    }

                    CountDownLatch disconnectedLatch = new CountDownLatch(1);
                    service.addConnectionListener(new TransportConnectionListener() {
                        @Override
                        public void onNodeDisconnected(DiscoveryNode node) {
                            if (node.equals(seedNode)) {
                                disconnectedLatch.countDown();
                            }
                        }
                    });

                    service.addFailToSendNoConnectRule(seedTransport);

                    if (randomBoolean()) {
                        connection.updateSkipUnavailable(false);
                    }
                    {
                        CountDownLatch responseLatch = new CountDownLatch(1);
                        AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                        AtomicReference<Exception> failReference = new AtomicReference<>();
                        connection.fetchSearchShards(searchShardsRequest,
                                new LatchedActionListener<>(ActionListener.wrap(reference::set, failReference::set), responseLatch));
                        assertTrue(responseLatch.await(1, TimeUnit.SECONDS));
                        assertNotNull(failReference.get());
                        assertNull(reference.get());
                        assertThat(failReference.get(), instanceOf(TransportException.class));
                    }

                    connection.updateSkipUnavailable(true);
                    {
                        CountDownLatch responseLatch = new CountDownLatch(1);
                        AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                        AtomicReference<Exception> failReference = new AtomicReference<>();
                        connection.fetchSearchShards(searchShardsRequest,
                                new LatchedActionListener<>(ActionListener.wrap(reference::set, failReference::set), responseLatch));
                        assertTrue(responseLatch.await(1, TimeUnit.SECONDS));
                        assertNull(failReference.get());
                        assertNotNull(reference.get());
                        ClusterSearchShardsResponse response = reference.get();
                        assertTrue(response == ClusterSearchShardsResponse.EMPTY);
                    }

                    //give transport service enough time to realize that the node is down, and to notify the connection listeners
                    //so that RemoteClusterConnection is left with no connected nodes, hence it will retry connecting next
                    assertTrue(disconnectedLatch.await(1, TimeUnit.SECONDS));

                    if (randomBoolean()) {
                        connection.updateSkipUnavailable(false);
                    }

                    service.clearAllRules();
                    //check that we reconnect once the node is back up
                    {
                        CountDownLatch responseLatch = new CountDownLatch(1);
                        AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                        AtomicReference<Exception> failReference = new AtomicReference<>();
                        connection.fetchSearchShards(searchShardsRequest,
                                new LatchedActionListener<>(ActionListener.wrap(reference::set, failReference::set), responseLatch));
                        assertTrue(responseLatch.await(1, TimeUnit.SECONDS));
                        assertNull(failReference.get());
                        assertNotNull(reference.get());
                        ClusterSearchShardsResponse response = reference.get();
                        assertTrue(response != ClusterSearchShardsResponse.EMPTY);
                        assertEquals(knownNodes, Arrays.asList(response.getNodes()));
                    }
                }
            }
        }
    }

    public void testTriggerUpdatesConcurrently() throws IOException, InterruptedException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService seedTransport1 = startTransport("seed_node_1", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            DiscoveryNode seedNode1 = seedTransport1.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            knownNodes.add(seedTransport1.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            List<DiscoveryNode> seedNodes = Arrays.asList(seedNode1, seedNode);
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, Integer.MAX_VALUE, n -> true)) {
                    int numThreads = randomIntBetween(4, 10);
                    Thread[] threads = new Thread[numThreads];
                    CyclicBarrier barrier = new CyclicBarrier(numThreads);
                    for (int i = 0; i < threads.length; i++) {
                        final int numConnectionAttempts = randomIntBetween(10, 200);
                        threads[i] = new Thread() {
                            @Override
                            public void run() {
                                try {
                                    barrier.await();
                                    CountDownLatch latch = new CountDownLatch(numConnectionAttempts);
                                    for (int i = 0; i < numConnectionAttempts; i++) {
                                        AtomicBoolean executed = new AtomicBoolean(false);
                                        ActionListener<Void> listener = ActionListener.wrap(x -> {
                                            assertTrue(executed.compareAndSet(false, true));
                                            latch.countDown();}, x -> {
                                            assertTrue(executed.compareAndSet(false, true));
                                            latch.countDown();
                                            if (x instanceof RejectedExecutionException) {
                                                // that's fine
                                            } else {
                                                throw new AssertionError(x);
                                            }
                                        });
                                        connection.updateSeedNodes(seedNodes, listener);
                                    }
                                    latch.await();
                                } catch (Exception ex) {
                                    throw new AssertionError(ex);
                                }
                            }
                        };
                        threads[i].start();
                    }

                    for (int i = 0; i < threads.length; i++) {
                        threads[i].join();
                    }
                    assertTrue(service.nodeConnected(seedNode));
                    assertTrue(service.nodeConnected(discoverableNode));
                    assertTrue(service.nodeConnected(seedNode1));
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }

    public void testCloseWhileConcurrentlyConnecting() throws IOException, InterruptedException, BrokenBarrierException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService seedTransport1 = startTransport("seed_node_1", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode seedNode1 = seedTransport1.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            knownNodes.add(seedTransport1.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            List<DiscoveryNode> seedNodes = Arrays.asList(seedNode1, seedNode);
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, Integer.MAX_VALUE, n -> true)) {
                    int numThreads = randomIntBetween(4, 10);
                    Thread[] threads = new Thread[numThreads];
                    CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
                    for (int i = 0; i < threads.length; i++) {
                        final int numConnectionAttempts = randomIntBetween(10, 100);
                        threads[i] = new Thread() {
                            @Override
                            public void run() {
                                try {
                                    barrier.await();
                                    CountDownLatch latch = new CountDownLatch(numConnectionAttempts);
                                    for (int i = 0; i < numConnectionAttempts; i++) {
                                        AtomicReference<Exception> executed = new AtomicReference<>();
                                        ActionListener<Void> listener = ActionListener.wrap(
                                            x -> {
                                                if (executed.compareAndSet(null, new RuntimeException())) {
                                                    latch.countDown();
                                                } else {
                                                    throw new AssertionError("shit's been called twice", executed.get());
                                                }
                                            },
                                            x -> {
                                                if (executed.compareAndSet(null, x)) {
                                                    latch.countDown();
                                                } else {
                                                    final String message = x.getMessage();
                                                    if ((executed.get().getClass() == x.getClass()
                                                        && "operation was cancelled reason [connect handler is closed]".equals(message)
                                                        && message.equals(executed.get().getMessage())) == false) {
                                                        // we do cancel the operation and that means that if timing allows it, the caller
                                                        // of a blocking call as well as the handler will get the exception from the
                                                        // ExecutionCancelledException concurrently. unless that is the case we fail
                                                        // if we get called more than once!
                                                        AssertionError assertionError = new AssertionError("shit's been called twice", x);
                                                        assertionError.addSuppressed(executed.get());
                                                        throw assertionError;
                                                    }
                                                }
                                                if (x instanceof RejectedExecutionException || x instanceof AlreadyClosedException
                                                    || x instanceof CancellableThreads.ExecutionCancelledException) {
                                                    // that's fine
                                                } else {
                                                    throw new AssertionError(x);
                                                }
                                        });
                                        connection.updateSeedNodes(seedNodes, listener);
                                    }
                                    latch.await();
                                } catch (Exception ex) {
                                    throw new AssertionError(ex);
                                }
                            }
                        };
                        threads[i].start();
                    }
                    barrier.await();
                    connection.close();
                }
            }
        }
    }

    private static void installNodeStatsHandler(TransportService service, DiscoveryNode...nodes) {
        service.registerRequestHandler(NodesInfoAction.NAME, NodesInfoRequest::new, ThreadPool.Names.SAME, false, false,
            (request, channel) -> {
                List<NodeInfo> nodeInfos = new ArrayList<>();
                int port = 80;
                for (DiscoveryNode node : nodes) {
                    HttpInfo http = new HttpInfo(new BoundTransportAddress(new TransportAddress[]{node.getAddress()},
                        new TransportAddress(node.getAddress().address().getAddress(), port++)), 100);
                    nodeInfos.add(new NodeInfo(node.getVersion(), Build.CURRENT, node, null, null, null, null, null, null, http, null,
                        null, null));
                }
                channel.sendResponse(new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, Collections.emptyList()));
            });

    }

    public void testGetConnectionInfo() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService transport1 = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService transport2 = startTransport("seed_node_1", knownNodes, Version.CURRENT);
             MockTransportService transport3 = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode node1 = transport1.getLocalDiscoNode();
            DiscoveryNode node2 = transport3.getLocalDiscoNode();
            DiscoveryNode node3 = transport2.getLocalDiscoNode();
            knownNodes.add(transport1.getLocalDiscoNode());
            knownNodes.add(transport3.getLocalDiscoNode());
            knownNodes.add(transport2.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            List<DiscoveryNode> seedNodes = Arrays.asList(node3, node1, node2);
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                int maxNumConnections = randomIntBetween(1, 5);
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, maxNumConnections, n -> true)) {
                    // test no nodes connected
                    RemoteConnectionInfo remoteConnectionInfo = assertSerialization(getRemoteConnectionInfo(connection));
                    assertNotNull(remoteConnectionInfo);
                    assertEquals(0, remoteConnectionInfo.numNodesConnected);
                    assertEquals(0, remoteConnectionInfo.seedNodes.size());
                    assertEquals(0, remoteConnectionInfo.httpAddresses.size());
                    assertEquals(maxNumConnections, remoteConnectionInfo.connectionsPerCluster);
                    assertEquals("test-cluster", remoteConnectionInfo.clusterAlias);
                    updateSeedNodes(connection, seedNodes);
                    expectThrows(RemoteTransportException.class, () -> getRemoteConnectionInfo(connection));

                    for (MockTransportService s : Arrays.asList(transport1, transport2, transport3)) {
                        installNodeStatsHandler(s, node1, node2, node3);
                    }

                    remoteConnectionInfo = getRemoteConnectionInfo(connection);
                    remoteConnectionInfo = assertSerialization(remoteConnectionInfo);
                    assertNotNull(remoteConnectionInfo);
                    assertEquals(connection.getNumNodesConnected(), remoteConnectionInfo.numNodesConnected);
                    assertEquals(Math.min(3, maxNumConnections), connection.getNumNodesConnected());
                    assertEquals(3, remoteConnectionInfo.seedNodes.size());
                    assertEquals(remoteConnectionInfo.httpAddresses.size(), Math.min(3, maxNumConnections));
                    assertEquals(maxNumConnections, remoteConnectionInfo.connectionsPerCluster);
                    assertEquals("test-cluster", remoteConnectionInfo.clusterAlias);
                    for (TransportAddress address : remoteConnectionInfo.httpAddresses) {
                        assertTrue("port range mismatch: " + address.getPort(), address.getPort() >= 80 && address.getPort() <= 90);
                    }
                }
            }
        }
    }

    public void testRemoteConnectionInfo() throws IOException {
        RemoteConnectionInfo stats = new RemoteConnectionInfo("test_cluster",
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 1)),
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 80)),
            4, 3, TimeValue.timeValueMinutes(30), false);
        assertSerialization(stats);

        RemoteConnectionInfo stats1 = new RemoteConnectionInfo("test_cluster",
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 1)),
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 80)),
            4, 4, TimeValue.timeValueMinutes(30), true);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster_1",
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 1)),
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 80)),
            4, 3, TimeValue.timeValueMinutes(30), false);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster",
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 15)),
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 80)),
            4, 3, TimeValue.timeValueMinutes(30), false);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster",
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 1)),
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 87)),
            4, 3, TimeValue.timeValueMinutes(30), true);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster",
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 1)),
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 80)),
            4, 3, TimeValue.timeValueMinutes(325), true);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster",
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 1)),
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS, 80)),
            5, 3, TimeValue.timeValueMinutes(30), false);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);
    }

    private static RemoteConnectionInfo assertSerialization(RemoteConnectionInfo info) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.CURRENT);
            info.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.CURRENT);
            RemoteConnectionInfo remoteConnectionInfo = new RemoteConnectionInfo(in);
            assertEquals(info, remoteConnectionInfo);
            assertEquals(info.hashCode(), remoteConnectionInfo.hashCode());
            return  randomBoolean() ? info : remoteConnectionInfo;
        }
    }

    public void testRemoteConnectionInfoBwComp() throws IOException {
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_5_6_5, Version.V_6_0_0);
        RemoteConnectionInfo expected = new RemoteConnectionInfo("test_cluster",
                Collections.singletonList(new TransportAddress(TransportAddress.META_ADDRESS, 1)),
                Collections.singletonList(new TransportAddress(TransportAddress.META_ADDRESS, 80)),
                4, 4, new TimeValue(30, TimeUnit.MINUTES), false);

        final byte[] data;
        if (version.before(Version.V_6_0_0_alpha1)) {
            String encoded = "AQABBAAAAAAHMC4wLjAuMAAAAAEBAAEEAAAAAAcwLjAuMC4wAAAAUAQ8BAQMdGVzdF9jbHVzdGVyAAAAAAAAAA==";
            data = Base64.getDecoder().decode(encoded);
        } else {
            String encoded = "AQQAAAAABzAuMC4wLjAAAAABAQQAAAAABzAuMC4wLjAAAABQBDwEBAx0ZXN0X2NsdXN0ZXIAAAAAAAAAAAAAAA==";
            data = Base64.getDecoder().decode(encoded);
        }

        try (StreamInput in = StreamInput.wrap(data)) {
            in.setVersion(version);
            RemoteConnectionInfo deserialized = new RemoteConnectionInfo(in);
            assertEquals(expected, deserialized);

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                deserialized.writeTo(out);
                try (StreamInput in2 = StreamInput.wrap(out.bytes().toBytesRef().bytes)) {
                    in2.setVersion(version);
                    RemoteConnectionInfo deserialized2 = new RemoteConnectionInfo(in2);
                    assertEquals(expected, deserialized2);
                }
            }
        }
    }

    public void testRenderConnectionInfoXContent() throws IOException {
        RemoteConnectionInfo stats = new RemoteConnectionInfo("test_cluster",
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS,1)),
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS,80)),
            4, 3, TimeValue.timeValueMinutes(30), true);
        stats = assertSerialization(stats);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();
        assertEquals("{\"test_cluster\":{\"seeds\":[\"0.0.0.0:1\"],\"http_addresses\":[\"0.0.0.0:80\"],\"connected\":true," +
            "\"num_nodes_connected\":3,\"max_connections_per_cluster\":4,\"initial_connect_timeout\":\"30m\"," +
                "\"skip_unavailable\":true}}", builder.string());

        stats = new RemoteConnectionInfo("some_other_cluster",
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS,1), new TransportAddress(TransportAddress.META_ADDRESS,2)),
            Arrays.asList(new TransportAddress(TransportAddress.META_ADDRESS,80), new TransportAddress(TransportAddress.META_ADDRESS,81)),
            2, 0, TimeValue.timeValueSeconds(30), false);
        stats = assertSerialization(stats);
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();
        assertEquals("{\"some_other_cluster\":{\"seeds\":[\"0.0.0.0:1\",\"0.0.0.0:2\"],\"http_addresses\":[\"0.0.0.0:80\",\"0.0.0.0:81\"],"
                + "\"connected\":false,\"num_nodes_connected\":0,\"max_connections_per_cluster\":2,\"initial_connect_timeout\":\"30s\"," +
                "\"skip_unavailable\":false}}", builder.string());
    }

    private RemoteConnectionInfo getRemoteConnectionInfo(RemoteClusterConnection connection) throws Exception {
        AtomicReference<RemoteConnectionInfo> statsRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        connection.getConnectionInfo(new ActionListener<RemoteConnectionInfo>() {
            @Override
            public void onResponse(RemoteConnectionInfo remoteConnectionInfo) {
                statsRef.set(remoteConnectionInfo);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });
        latch.await();
        if (exceptionRef.get() != null) {
            throw exceptionRef.get();
        }
        return statsRef.get();
    }

    public void testEnsureConnected() throws IOException, InterruptedException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
            MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    assertFalse(service.nodeConnected(seedNode));
                    assertFalse(service.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());
                    CountDownLatch latch = new CountDownLatch(1);
                    connection.ensureConnected(new LatchedActionListener<>(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void aVoid) {
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new AssertionError(e);
                        }
                    }, latch));
                    latch.await();
                    assertTrue(service.nodeConnected(seedNode));
                    assertTrue(service.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());

                    // exec again we are already connected
                    connection.ensureConnected(new LatchedActionListener<>(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void aVoid) {
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new AssertionError(e);
                        }
                    }, latch));
                    latch.await();
                    assertTrue(service.nodeConnected(seedNode));
                    assertTrue(service.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }

    public void testCollectNodes() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    if (randomBoolean()) {
                        updateSeedNodes(connection, Arrays.asList(seedNode));
                    }
                    CountDownLatch responseLatch = new CountDownLatch(1);
                    AtomicReference<Function<String, DiscoveryNode>> reference = new AtomicReference<>();
                    AtomicReference<Exception> failReference = new AtomicReference<>();
                    ActionListener<Function<String, DiscoveryNode>> shardsListener = ActionListener.wrap(
                        x -> {
                            reference.set(x);
                            responseLatch.countDown();
                        },
                        x -> {
                            failReference.set(x);
                            responseLatch.countDown();
                        });
                    connection.collectNodes(shardsListener);
                    responseLatch.await();
                    assertNull(failReference.get());
                    assertNotNull(reference.get());
                    Function<String, DiscoveryNode> function = reference.get();
                    assertEquals(seedNode, function.apply(seedNode.getId()));
                    assertNull(function.apply(seedNode.getId() + "foo"));
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }

    public void testConnectedNodesConcurrentAccess() throws IOException, InterruptedException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        List<MockTransportService> discoverableTransports = new CopyOnWriteArrayList<>();
        try {
            final int numDiscoverableNodes = randomIntBetween(5, 20);
            List<DiscoveryNode> discoverableNodes = new ArrayList<>(numDiscoverableNodes);
            for (int i = 0; i < numDiscoverableNodes; i++ ) {
                MockTransportService transportService = startTransport("discoverable_node" + i, knownNodes, Version.CURRENT);
                discoverableNodes.add(transportService.getLocalDiscoNode());
                discoverableTransports.add(transportService);
            }

            List<DiscoveryNode> seedNodes = randomSubsetOf(discoverableNodes);
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, Integer.MAX_VALUE, n -> true)) {
                    final int numGetThreads = randomIntBetween(4, 10);
                    final Thread[] getThreads = new Thread[numGetThreads];
                    final int numModifyingThreads = randomIntBetween(4, 10);
                    final Thread[] modifyingThreads = new Thread[numModifyingThreads];
                    CyclicBarrier barrier = new CyclicBarrier(numGetThreads + numModifyingThreads);
                    for (int i = 0; i < getThreads.length; i++) {
                        final int numGetCalls = randomIntBetween(1000, 10000);
                        getThreads[i] = new Thread(() -> {
                            try {
                                barrier.await();
                                for (int j = 0; j < numGetCalls; j++) {
                                    try {
                                        DiscoveryNode node = connection.getConnectedNode();
                                        assertNotNull(node);
                                    } catch (IllegalStateException e) {
                                        if (e.getMessage().startsWith("No node available for cluster:") == false) {
                                            throw e;
                                        }
                                    }
                                }
                            } catch (Exception ex) {
                                throw new AssertionError(ex);
                            }
                        });
                        getThreads[i].start();
                    }

                    final AtomicInteger counter = new AtomicInteger();
                    for (int i = 0; i < modifyingThreads.length; i++) {
                        final int numDisconnects = randomIntBetween(5, 10);
                        modifyingThreads[i] = new Thread(() -> {
                            try {
                                barrier.await();
                                for (int j = 0; j < numDisconnects; j++) {
                                    if (randomBoolean()) {
                                        MockTransportService transportService =
                                            startTransport("discoverable_node_added" + counter.incrementAndGet(), knownNodes,
                                                Version.CURRENT);
                                        discoverableTransports.add(transportService);
                                        connection.addConnectedNode(transportService.getLocalDiscoNode());
                                    } else {
                                        DiscoveryNode node = randomFrom(discoverableNodes);
                                        connection.onNodeDisconnected(node);
                                    }
                                }
                            } catch (Exception ex) {
                                throw new AssertionError(ex);
                            }
                        });
                        modifyingThreads[i].start();
                    }

                    for (Thread thread : getThreads) {
                        thread.join();
                    }
                    for (Thread thread : modifyingThreads) {
                        thread.join();
                    }
                }
            }
        } finally {
            IOUtils.closeWhileHandlingException(discoverableTransports);
        }
    }

    public void testClusterNameIsChecked() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> otherClusterKnownNodes = new CopyOnWriteArrayList<>();

        Settings settings = Settings.builder().put("cluster.name", "testClusterNameIsChecked").build();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, threadPool, settings);
            MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT, threadPool,
                settings);
             MockTransportService otherClusterTransport = startTransport("other_cluster_discoverable_node", otherClusterKnownNodes,
                 Version.CURRENT, threadPool, Settings.builder().put("cluster.name", "otherCluster").build());
         MockTransportService otherClusterDiscoverable= startTransport("other_cluster_discoverable_node", otherClusterKnownNodes,
             Version.CURRENT, threadPool, Settings.builder().put("cluster.name", "otherCluster").build())) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            otherClusterKnownNodes.add(otherClusterDiscoverable.getLocalDiscoNode());
            otherClusterKnownNodes.add(otherClusterTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    updateSeedNodes(connection, Arrays.asList(seedNode));
                    assertTrue(service.nodeConnected(seedNode));
                    assertTrue(service.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());
                    List<DiscoveryNode> discoveryNodes = Arrays.asList(otherClusterTransport.getLocalDiscoNode(), seedNode);
                    Collections.shuffle(discoveryNodes, random());
                    updateSeedNodes(connection, discoveryNodes);
                    assertTrue(service.nodeConnected(seedNode));
                    for (DiscoveryNode otherClusterNode : otherClusterKnownNodes) {
                        assertFalse(service.nodeConnected(otherClusterNode));
                    }
                    assertFalse(service.nodeConnected(otherClusterTransport.getLocalDiscoNode()));
                    assertTrue(service.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());
                    IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, () ->
                        updateSeedNodes(connection, Arrays.asList(otherClusterTransport.getLocalDiscoNode())));
                    assertThat(illegalStateException.getMessage(),
                        startsWith("handshake failed, mismatched cluster name [Cluster [otherCluster]]" +
                            " - {other_cluster_discoverable_node}"));
                }
            }
        }
    }
}
