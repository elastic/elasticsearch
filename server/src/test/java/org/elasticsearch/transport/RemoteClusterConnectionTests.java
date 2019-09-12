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

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableConnectionManager;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class RemoteClusterConnectionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final ConnectionProfile profile = RemoteClusterService.buildConnectionProfileFromSettings(Settings.EMPTY, "cluster");

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
            newService.registerRequestHandler(ClusterSearchShardsAction.NAME, ThreadPool.Names.SAME, ClusterSearchShardsRequest::new,
                (request, channel, task) -> {
                    if ("index_not_found".equals(request.preference())) {
                        channel.sendResponse(new IndexNotFoundException("index"));
                    } else {
                        channel.sendResponse(new ClusterSearchShardsResponse(new ClusterSearchShardsGroup[0],
                            knownNodes.toArray(new DiscoveryNode[0]), Collections.emptyMap()));
                    }
                });
            newService.registerRequestHandler(SearchAction.NAME, ThreadPool.Names.SAME, SearchRequest::new,
                (request, channel, task) -> {
                    if ("index_not_found".equals(request.preference())) {
                        channel.sendResponse(new IndexNotFoundException("index"));
                        return;
                    }
                    SearchHits searchHits;
                    if ("null_target".equals(request.preference())) {
                        searchHits = new SearchHits(new SearchHit[] {new SearchHit(0)}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1F);
                    } else {
                        searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
                    }
                    InternalSearchResponse response = new InternalSearchResponse(searchHits,
                        InternalAggregations.EMPTY, null, null, false, null, 1);
                    SearchResponse searchResponse = new SearchResponse(response, null, 1, 1, 0, 100, ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY);
                    channel.sendResponse(searchResponse);
                });
            newService.registerRequestHandler(ClusterStateAction.NAME, ThreadPool.Names.SAME, ClusterStateRequest::new,
                (request, channel, task) -> {
                    DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
                    for (DiscoveryNode node : knownNodes) {
                        builder.add(node);
                    }
                    ClusterState build = ClusterState.builder(clusterName).nodes(builder.build()).build();
                    channel.sendResponse(new ClusterStateResponse(clusterName, build, false));
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

    public void testRemoteProfileIsUsedForLocalCluster() throws Exception {
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
                    Arrays.asList(Tuple.tuple(seedNode.toString(), () -> seedNode)), service, Integer.MAX_VALUE, n -> true, null,
                    profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
                    updateSeedNodes(connection, seedNodes(seedNode));
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());
                    TransportFuture<ClusterSearchShardsResponse> futureHandler = transportFuture(ClusterSearchShardsResponse::new);
                    TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.BULK)
                        .build();
                    IllegalStateException ise = (IllegalStateException) expectThrows(SendRequestTransportException.class, () -> {
                        service.sendRequest(connectionManager.getConnection(discoverableNode),
                            ClusterSearchShardsAction.NAME, new ClusterSearchShardsRequest(), options, futureHandler);
                        futureHandler.txGet();
                    }).getCause();
                    assertEquals(ise.getMessage(), "can't select channel size is 0 for types: [RECOVERY, BULK, STATE]");
                }
            }
        }
    }

    public void testRemoteProfileIsUsedForRemoteCluster() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, threadPool,
            Settings.builder().put("cluster.name", "foobar").build());
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT,
                 threadPool, Settings.builder().put("cluster.name", "foobar").build())) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(Tuple.tuple(seedNode.toString(), () -> seedNode)), service, Integer.MAX_VALUE, n -> true, null,
                    profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
                    updateSeedNodes(connection, seedNodes(seedNode));
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());
                    TransportFuture<ClusterSearchShardsResponse> futureHandler = transportFuture(ClusterSearchShardsResponse::new);
                    TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.BULK)
                        .build();
                    IllegalStateException ise = (IllegalStateException) expectThrows(SendRequestTransportException.class, () -> {
                        service.sendRequest(connectionManager.getConnection(discoverableNode),
                            ClusterSearchShardsAction.NAME, new ClusterSearchShardsRequest(), options, futureHandler);
                        futureHandler.txGet();
                    }).getCause();
                    assertEquals(ise.getMessage(), "can't select channel size is 0 for types: [RECOVERY, BULK, STATE]");

                    TransportFuture<ClusterSearchShardsResponse> handler = transportFuture(ClusterSearchShardsResponse::new);
                    TransportRequestOptions ops = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.REG)
                        .build();
                    service.sendRequest(connection.getConnection(), ClusterSearchShardsAction.NAME, new ClusterSearchShardsRequest(),
                        ops, handler);
                    handler.txGet();
                }
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
                    Arrays.asList(Tuple.tuple(seedNode.toString(), () -> seedNode)), service, Integer.MAX_VALUE, n -> true, null,
                    profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
                    updateSeedNodes(connection, seedNodes(seedNode));
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
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
            List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes = Arrays.asList(
                    Tuple.tuple(incompatibleSeedNode.toString(), () -> incompatibleSeedNode),
                    Tuple.tuple(seedNode.toString(), () -> seedNode));
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, Integer.MAX_VALUE, n -> true, null, profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
                    updateSeedNodes(connection, seedNodes);
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertFalse(connectionManager.nodeConnected(incompatibleSeedNode));
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
                    seedNodes(seedNode), service, Integer.MAX_VALUE, n -> true, null, profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
                    updateSeedNodes(connection, seedNodes(seedNode));
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertFalse(connectionManager.nodeConnected(spareNode));
                    knownNodes.add(spareNode);
                    CountDownLatch latchDisconnect = new CountDownLatch(1);
                    CountDownLatch latchConnected = new CountDownLatch(1);
                    connectionManager.addListener(new TransportConnectionListener() {
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
                    assertTrue(connectionManager.nodeConnected(spareNode));
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
                    seedNodes(seedNode), service, Integer.MAX_VALUE, n -> n.equals(rejectedNode) == false, null, profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
                    updateSeedNodes(connection, seedNodes(seedNode));
                    if (rejectedNode.equals(seedNode)) {
                        assertFalse(connectionManager.nodeConnected(seedNode));
                        assertTrue(connectionManager.nodeConnected(discoverableNode));
                    } else {
                        assertTrue(connectionManager.nodeConnected(seedNode));
                        assertFalse(connectionManager.nodeConnected(discoverableNode));
                    }
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }
    private void updateSeedNodes(
            final RemoteClusterConnection connection, final List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes) throws Exception {
        updateSeedNodes(connection, seedNodes, null);
    }

    private void updateSeedNodes(
            final RemoteClusterConnection connection,
            final List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes,
            final String proxyAddress)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        ActionListener<Void> listener = ActionListener.wrap(
            x -> latch.countDown(),
            x -> {
                exceptionAtomicReference.set(x);
                latch.countDown();
            }
        );
        connection.updateSeedNodes(proxyAddress, seedNodes, listener);
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
                    Arrays.asList(Tuple.tuple(seedNode.toString(), () -> seedNode)), service, Integer.MAX_VALUE, n -> true, null,
                    profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
                    expectThrows(
                            Exception.class,
                            () -> updateSeedNodes(connection, Arrays.asList(Tuple.tuple(seedNode.toString(), () -> seedNode))));
                    assertFalse(connectionManager.nodeConnected(seedNode));
                    assertTrue(connection.assertNoRunningConnections());
                }
            }
        }
    }

    public void testRemoteConnectionVersionMatchesTransportConnectionVersion() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Version previousVersion = randomValueOtherThan(Version.CURRENT, () -> VersionUtils.randomVersionBetween(random(),
            Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT));
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, previousVersion)) {

            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            assertThat(seedNode, notNullValue());
            knownNodes.add(seedNode);

            DiscoveryNode oldVersionNode = discoverableTransport.getLocalDiscoNode();
            assertThat(oldVersionNode, notNullValue());
            knownNodes.add(oldVersionNode);

            assertThat(seedNode.getVersion(), not(equalTo(oldVersionNode.getVersion())));
            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                final Transport.Connection seedConnection = new CloseableConnection() {
                    @Override
                    public DiscoveryNode getNode() {
                        return seedNode;
                    }

                    @Override
                    public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                        throws IOException, TransportException {
                        // no-op
                    }
                };

                ConnectionManager delegate = new ConnectionManager(Settings.EMPTY, service.transport);
                StubbableConnectionManager connectionManager = new StubbableConnectionManager(delegate, Settings.EMPTY, service.transport);

                connectionManager.addConnectBehavior(seedNode.getAddress(), (cm, discoveryNode) -> {
                    if (discoveryNode == seedNode) {
                        return seedConnection;
                    }
                    return cm.getConnection(discoveryNode);
                });

                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                        seedNodes(seedNode), service, Integer.MAX_VALUE, n -> true, null, connectionManager)) {
                    PlainActionFuture.get(fut -> connection.ensureConnected(ActionListener.map(fut, x -> null)));
                    assertThat(knownNodes, iterableWithSize(2));
                    assertThat(connection.getConnection(seedNode).getVersion(), equalTo(Version.CURRENT));
                    assertThat(connection.getConnection(oldVersionNode).getVersion(), equalTo(previousVersion));
                }
            }
        }
    }

    @SuppressForbidden(reason = "calls getLocalHost here but it's fine in this case")
    public void testSlowNodeCanBeCancelled() throws IOException, InterruptedException {
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
                        seedNodes(seedNode), service, Integer.MAX_VALUE, n -> true, null, profile)) {
                    ActionListener<Void> listener = ActionListener.wrap(x -> {
                        listenerCalled.countDown();
                        fail("expected exception");
                    }, x -> {
                        exceptionReference.set(x);
                        listenerCalled.countDown();
                    });
                    connection.updateSeedNodes(null, seedNodes(seedNode), listener);
                    acceptedLatch.await();
                    connection.close(); // now close it, this should trigger an interrupt on the socket and we can move on
                    assertTrue(connection.assertNoRunningConnections());
                }
                closeRemote.countDown();
                listenerCalled.await();
                assertNotNull(exceptionReference.get());
                expectThrows(AlreadyClosedException.class, () -> {
                    throw exceptionReference.get();
                });

            }
        }
    }

    private static List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes(final DiscoveryNode... seedNodes) {
        if (seedNodes.length == 0) {
            return Collections.emptyList();
        } else if (seedNodes.length == 1) {
            return Collections.singletonList(Tuple.tuple(seedNodes[0].toString(), () -> seedNodes[0]));
        } else {
            return Arrays.stream(seedNodes)
                    .map(s -> Tuple.tuple(s.toString(), (Supplier<DiscoveryNode>)() -> s))
                    .collect(Collectors.toList());
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
            List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes = seedNodes(seedNode1, seedNode);
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, Integer.MAX_VALUE, n -> true, null, profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
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
                                        ActionListener<Void> listener = ActionListener.wrap(
                                            x -> {
                                                assertTrue(executed.compareAndSet(false, true));
                                                latch.countDown();
                                            },
                                            x -> {
                                                assertTrue(executed.compareAndSet(false, true));
                                                latch.countDown();

                                                if (!(x instanceof RejectedExecutionException)) {
                                                    throw new AssertionError(x);
                                                }
                                            });
                                        connection.updateSeedNodes(null, seedNodes, listener);
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
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(connectionManager.nodeConnected(seedNode1));
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
            List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes = seedNodes(seedNode1, seedNode);
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, Integer.MAX_VALUE, n -> true, null, profile)) {
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
                                                if (x instanceof RejectedExecutionException || x instanceof AlreadyClosedException) {
                                                    // that's fine
                                                } else {
                                                    throw new AssertionError(x);
                                                }
                                            });
                                        try {
                                            connection.updateSeedNodes(null, seedNodes, listener);
                                        } catch (Exception e) {
                                            // it's ok if we're shutting down
                                            assertThat(e.getMessage(), containsString("threadcontext is already closed"));
                                            latch.countDown();
                                        }
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
                }
            }
        }
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
            List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes = seedNodes(node3, node1, node2);
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                int maxNumConnections = randomIntBetween(1, 5);
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, maxNumConnections, n -> true, null, profile)) {
                    // test no nodes connected
                    RemoteConnectionInfo remoteConnectionInfo = assertSerialization(connection.getConnectionInfo());
                    assertNotNull(remoteConnectionInfo);
                    assertEquals(0, remoteConnectionInfo.numNodesConnected);
                    assertEquals(3, remoteConnectionInfo.seedNodes.size());
                    assertEquals(maxNumConnections, remoteConnectionInfo.connectionsPerCluster);
                    assertEquals("test-cluster", remoteConnectionInfo.clusterAlias);

                    // Connect some nodes
                    updateSeedNodes(connection, seedNodes);
                    remoteConnectionInfo = assertSerialization(connection.getConnectionInfo());
                    assertNotNull(remoteConnectionInfo);
                    assertEquals(connection.getNumNodesConnected(), remoteConnectionInfo.numNodesConnected);
                    assertEquals(Math.min(3, maxNumConnections), connection.getNumNodesConnected());
                    assertEquals(3, remoteConnectionInfo.seedNodes.size());
                    assertEquals(maxNumConnections, remoteConnectionInfo.connectionsPerCluster);
                    assertEquals("test-cluster", remoteConnectionInfo.clusterAlias);
                }
            }
        }
    }

    public void testRemoteConnectionInfo() throws IOException {
        RemoteConnectionInfo stats =
                new RemoteConnectionInfo("test_cluster", Arrays.asList("seed:1"), 4, 3, TimeValue.timeValueMinutes(30), false);
        assertSerialization(stats);

        RemoteConnectionInfo stats1 =
                new RemoteConnectionInfo("test_cluster", Arrays.asList("seed:1"), 4, 4, TimeValue.timeValueMinutes(30), true);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster_1", Arrays.asList("seed:1"), 4, 3, TimeValue.timeValueMinutes(30), false);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster", Arrays.asList("seed:15"), 4, 3, TimeValue.timeValueMinutes(30), false);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster", Arrays.asList("seed:1"), 4, 3, TimeValue.timeValueMinutes(30), true);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster", Arrays.asList("seed:1"), 4, 3, TimeValue.timeValueMinutes(325), true);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster", Arrays.asList("seed:1"), 5, 3, TimeValue.timeValueMinutes(30), false);
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
            return randomBoolean() ? info : remoteConnectionInfo;
        }
    }

    public void testRenderConnectionInfoXContent() throws IOException {
        RemoteConnectionInfo stats =
                new RemoteConnectionInfo("test_cluster", Arrays.asList("seed:1"), 4, 3, TimeValue.timeValueMinutes(30), true);
        stats = assertSerialization(stats);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();
        assertEquals("{\"test_cluster\":{\"seeds\":[\"seed:1\"],\"connected\":true," +
            "\"num_nodes_connected\":3,\"max_connections_per_cluster\":4,\"initial_connect_timeout\":\"30m\"," +
            "\"skip_unavailable\":true}}", Strings.toString(builder));

        stats = new RemoteConnectionInfo(
                "some_other_cluster", Arrays.asList("seed:1", "seed:2"), 2, 0, TimeValue.timeValueSeconds(30), false);
        stats = assertSerialization(stats);
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();
        assertEquals("{\"some_other_cluster\":{\"seeds\":[\"seed:1\",\"seed:2\"],"
            + "\"connected\":false,\"num_nodes_connected\":0,\"max_connections_per_cluster\":2,\"initial_connect_timeout\":\"30s\"," +
            "\"skip_unavailable\":false}}", Strings.toString(builder));
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
                    seedNodes(seedNode), service, Integer.MAX_VALUE, n -> true, null, profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
                    assertFalse(connectionManager.nodeConnected(seedNode));
                    assertFalse(connectionManager.nodeConnected(discoverableNode));
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
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
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
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
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
                    seedNodes(seedNode), service, Integer.MAX_VALUE, n -> true, null, profile)) {
                    if (randomBoolean()) {
                        updateSeedNodes(connection, seedNodes(seedNode));
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
            List<Tuple<String, Supplier<DiscoveryNode>>> discoverableNodes = new ArrayList<>(numDiscoverableNodes);
            for (int i = 0; i < numDiscoverableNodes; i++ ) {
                MockTransportService transportService = startTransport("discoverable_node" + i, knownNodes, Version.CURRENT);
                discoverableNodes.add(Tuple.tuple("discoverable_node" + i, transportService::getLocalDiscoNode));
                discoverableTransports.add(transportService);
            }

            List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes = new CopyOnWriteArrayList<>(randomSubsetOf(discoverableNodes));
            Collections.shuffle(seedNodes, random());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes, service, Integer.MAX_VALUE, n -> true, null, profile)) {
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
                                        DiscoveryNode node = connection.getAnyConnectedNode();
                                        assertNotNull(node);
                                    } catch (NoSuchRemoteClusterException e) {
                                        // ignore, this is an expected exception
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
                                        String node = "discoverable_node_added" + counter.incrementAndGet();
                                        MockTransportService transportService =
                                            startTransport(node, knownNodes,
                                                Version.CURRENT);
                                        discoverableTransports.add(transportService);
                                        seedNodes.add(Tuple.tuple(node, () -> transportService.getLocalDiscoNode()));
                                        PlainActionFuture.get(fut -> connection.updateSeedNodes(null, seedNodes,
                                            ActionListener.map(fut, x -> null)));
                                    } else {
                                        DiscoveryNode node = randomFrom(discoverableNodes).v2().get();
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
             MockTransportService otherClusterDiscoverable = startTransport("other_cluster_discoverable_node", otherClusterKnownNodes,
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
                    seedNodes(seedNode), service, Integer.MAX_VALUE, n -> true, null, profile)) {
                    ConnectionManager connectionManager = connection.getConnectionManager();
                    updateSeedNodes(connection, seedNodes(seedNode));
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());
                    List<Tuple<String, Supplier<DiscoveryNode>>> discoveryNodes =
                            Arrays.asList(
                                    Tuple.tuple("other", otherClusterTransport::getLocalDiscoNode),
                                    Tuple.tuple(seedNode.toString(), () -> seedNode));
                    Collections.shuffle(discoveryNodes, random());
                    updateSeedNodes(connection, discoveryNodes);
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    for (DiscoveryNode otherClusterNode : otherClusterKnownNodes) {
                        assertFalse(connectionManager.nodeConnected(otherClusterNode));
                    }
                    assertFalse(connectionManager.nodeConnected(otherClusterTransport.getLocalDiscoNode()));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(connection.assertNoRunningConnections());
                    IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, () ->
                        updateSeedNodes(connection, Arrays.asList(Tuple.tuple("other", otherClusterTransport::getLocalDiscoNode))));
                    assertThat(illegalStateException.getMessage(), allOf(
                        startsWith("handshake with [{other_cluster_discoverable_node}"),
                        containsString(otherClusterTransport.getLocalDiscoNode().toString()),
                        endsWith(" failed: remote cluster name [otherCluster] " +
                            "does not match expected remote cluster name [testClusterNameIsChecked]")));
                }
            }
        }
    }

    public void testGetConnection() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {

            DiscoveryNode connectedNode = seedTransport.getLocalDiscoNode();
            assertThat(connectedNode, notNullValue());
            knownNodes.add(connectedNode);

            DiscoveryNode disconnectedNode = discoverableTransport.getLocalDiscoNode();
            assertThat(disconnectedNode, notNullValue());
            knownNodes.add(disconnectedNode);

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                Transport.Connection seedConnection = new CloseableConnection() {
                    @Override
                    public DiscoveryNode getNode() {
                        return connectedNode;
                    }

                    @Override
                    public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                        throws TransportException {
                        // no-op
                    }
                };

                ConnectionManager delegate = new ConnectionManager(Settings.EMPTY, service.transport);
                StubbableConnectionManager connectionManager = new StubbableConnectionManager(delegate, Settings.EMPTY, service.transport);

                connectionManager.setDefaultNodeConnectedBehavior(cm -> Collections.singleton(connectedNode));

                connectionManager.addConnectBehavior(connectedNode.getAddress(), (cm, discoveryNode) -> {
                    if (discoveryNode == connectedNode) {
                        return seedConnection;
                    }
                    return cm.getConnection(discoveryNode);
                });
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    seedNodes(connectedNode), service, Integer.MAX_VALUE, n -> true, null, connectionManager)) {
                    PlainActionFuture.get(fut -> connection.ensureConnected(ActionListener.map(fut, x -> null)));
                    for (int i = 0; i < 10; i++) {
                        //always a direct connection as the remote node is already connected
                        Transport.Connection remoteConnection = connection.getConnection(connectedNode);
                        assertSame(seedConnection, remoteConnection);
                    }
                    for (int i = 0; i < 10; i++) {
                        // we don't use the transport service connection manager so we will get a proxy connection for the local node
                        Transport.Connection remoteConnection = connection.getConnection(service.getLocalNode());
                        assertThat(remoteConnection, instanceOf(RemoteClusterConnection.ProxyConnection.class));
                        assertThat(remoteConnection.getNode(), equalTo(service.getLocalNode()));
                    }
                    for (int i = 0; i < 10; i++) {
                        //always a proxy connection as the target node is not connected
                        Transport.Connection remoteConnection = connection.getConnection(disconnectedNode);
                        assertThat(remoteConnection, instanceOf(RemoteClusterConnection.ProxyConnection.class));
                        assertThat(remoteConnection.getNode(), sameInstance(disconnectedNode));
                    }
                }
            }
        }
    }

    public void testLazyResolveTransportAddress() throws Exception {
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
                CountDownLatch multipleResolveLatch = new CountDownLatch(2);
                Tuple<String, Supplier<DiscoveryNode>> seedSupplier = Tuple.tuple(seedNode.toString(), () -> {
                    multipleResolveLatch.countDown();
                    return seedNode;
                });
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedSupplier), service, Integer.MAX_VALUE, n -> true, null, profile)) {
                    updateSeedNodes(connection, Arrays.asList(seedSupplier));
                    // Closing connections leads to RemoteClusterConnection.ConnectHandler.collectRemoteNodes
                    // being called again so we try to resolve the same seed node's host twice
                    discoverableTransport.close();
                    seedTransport.close();
                    assertTrue(multipleResolveLatch.await(30L, TimeUnit.SECONDS));
                }
            }
        }
    }

    public void testProxyMode() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("node_0", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("node_1", knownNodes, Version.CURRENT)) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            final String proxyAddress = "1.1.1.1:99";
            Map<String, DiscoveryNode> nodes = new HashMap<>();
            nodes.put("node_0", seedTransport.getLocalDiscoNode());
            nodes.put("node_1", discoverableTransport.getLocalDiscoNode());
            Transport mockTcpTransport = getProxyTransport(threadPool, Collections.singletonMap(proxyAddress, nodes));
            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, mockTcpTransport, Version.CURRENT,
                threadPool, null, Collections.emptySet())) {
                service.start();
                service.acceptIncomingRequests();
                Tuple<String, Supplier<DiscoveryNode>> seedSupplier = Tuple.tuple("node_0", () ->
                    RemoteClusterAware.buildSeedNode("some-remote-cluster", "node_0:" + randomIntBetween(1, 10000), true));
                assertEquals("node_0", seedSupplier.v2().get().getAttributes().get("server_name"));
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedSupplier), service, Integer.MAX_VALUE, n -> true, proxyAddress, profile)) {
                    updateSeedNodes(connection, Arrays.asList(seedSupplier), proxyAddress);
                    assertEquals(2, connection.getNumNodesConnected());
                    assertNotNull(connection.getConnection(discoverableTransport.getLocalDiscoNode()));
                    assertNotNull(connection.getConnection(seedTransport.getLocalDiscoNode()));
                    assertEquals(proxyAddress, connection.getConnection(seedTransport.getLocalDiscoNode())
                        .getNode().getAddress().toString());
                    assertEquals(proxyAddress, connection.getConnection(discoverableTransport.getLocalDiscoNode())
                        .getNode().getAddress().toString());
                    connection.getConnectionManager().disconnectFromNode(knownNodes.get(0));
                    // ensure we reconnect
                    assertBusy(() -> {
                        assertEquals(2, connection.getNumNodesConnected());
                    });
                    discoverableTransport.close();
                    seedTransport.close();
                }
            }
        }
    }

    public static Transport getProxyTransport(ThreadPool threadPool, Map<String, Map<String, DiscoveryNode>> nodeMap) {
        if (nodeMap.isEmpty()) {
            throw new IllegalArgumentException("nodeMap must be non-empty");
        }

        StubbableTransport stubbableTransport = new StubbableTransport(MockTransportService.newMockTransport(Settings.EMPTY,
            Version.CURRENT, threadPool));
        stubbableTransport.setDefaultConnectBehavior((t, node,  profile, listener) -> {
                Map<String, DiscoveryNode> proxyMapping = nodeMap.get(node.getAddress().toString());
                if (proxyMapping == null) {
                    throw new IllegalStateException("no proxy mapping for node: " + node);
                }
                DiscoveryNode proxyNode = proxyMapping.get(node.getName());
                if (proxyNode == null) {
                    // this is a seednode - lets pick one randomly
                    assertEquals("seed node must not have a port in the hostname: " + node.getHostName(),
                        -1, node.getHostName().lastIndexOf(':'));
                    assertTrue("missing hostname: " + node, proxyMapping.containsKey(node.getHostName()));
                    // route by seed hostname
                    proxyNode = proxyMapping.get(node.getHostName());
                }
                t.openConnection(proxyNode, profile, ActionListener.delegateFailure(listener,
                    (delegatedListener, connection) -> delegatedListener.onResponse(
                        new Transport.Connection() {
                            @Override
                            public DiscoveryNode getNode() {
                                return node;
                            }

                            @Override
                            public void sendRequest(long requestId, String action, TransportRequest request,
                                                    TransportRequestOptions options) throws IOException {
                                connection.sendRequest(requestId, action, request, options);
                            }

                            @Override
                            public void addCloseListener(ActionListener<Void> listener) {
                                connection.addCloseListener(listener);
                            }

                            @Override
                            public boolean isClosed() {
                                return connection.isClosed();
                            }

                            @Override
                            public void close() {
                                connection.close();
                            }
                        })));
        });
        return stubbableTransport;
    }

    private static <V extends TransportResponse> TransportFuture<V> transportFuture(Writeable.Reader<V> reader) {
        return new TransportFuture<>(new TransportResponseHandler<>() {
            @Override
            public void handleResponse(V response) {
            }

            @Override
            public void handleException(final TransportException exp) {
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public V read(StreamInput in) throws IOException {
                return reader.read(in);
            }
        });
    }
}
