/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.remote.RemoteClusterNodesAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.ReleasableRef;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;

public class RemoteClusterConnectionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(
        String id,
        List<DiscoveryNode> knownNodes,
        VersionInformation version,
        TransportVersion transportVersion
    ) {
        return startTransport(id, knownNodes, version, transportVersion, threadPool);
    }

    public static MockTransportService startTransport(
        String id,
        List<DiscoveryNode> knownNodes,
        VersionInformation version,
        TransportVersion transportVersion,
        ThreadPool threadPool
    ) {
        return startTransport(id, knownNodes, version, transportVersion, threadPool, Settings.EMPTY);
    }

    public static MockTransportService startTransport(
        final String id,
        final List<DiscoveryNode> knownNodes,
        final VersionInformation version,
        final TransportVersion transportVersion,
        final ThreadPool threadPool,
        final Settings settings
    ) {
        boolean success = false;
        final Settings s = Settings.builder().put(settings).put("node.name", id).build();
        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(s);
        MockTransportService newService = MockTransportService.createNewService(s, version, transportVersion, threadPool, null);
        try {
            newService.registerRequestHandler(
                TransportSearchShardsAction.TYPE.name(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                SearchShardsRequest::new,
                (request, channel, task) -> {
                    if ("index_not_found".equals(request.preference())) {
                        channel.sendResponse(new IndexNotFoundException("index"));
                    } else {
                        channel.sendResponse(new SearchShardsResponse(List.of(), knownNodes, Collections.emptyMap()));
                    }
                }
            );
            newService.registerRequestHandler(
                TransportSearchAction.TYPE.name(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                SearchRequest::new,
                (request, channel, task) -> {
                    if ("index_not_found".equals(request.preference())) {
                        channel.sendResponse(new IndexNotFoundException("index"));
                        return;
                    }
                    SearchHits searchHits;
                    if ("null_target".equals(request.preference())) {
                        searchHits = SearchHits.unpooled(
                            new SearchHit[] { SearchHit.unpooled(0) },
                            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                            1F
                        );
                    } else {
                        searchHits = SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
                    }
                    try (
                        var searchResponseRef = ReleasableRef.of(
                            new SearchResponse(
                                searchHits,
                                InternalAggregations.EMPTY,
                                null,
                                false,
                                null,
                                null,
                                1,
                                null,
                                1,
                                1,
                                0,
                                100,
                                ShardSearchFailure.EMPTY_ARRAY,
                                SearchResponse.Clusters.EMPTY
                            )
                        )
                    ) {
                        channel.sendResponse(searchResponseRef.get());
                    }
                }
            );
            newService.registerRequestHandler(
                ClusterStateAction.NAME,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                ClusterStateRequest::new,
                (request, channel, task) -> {
                    DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
                    for (DiscoveryNode node : knownNodes) {
                        builder.add(node);
                    }
                    ClusterState build = ClusterState.builder(clusterName).nodes(builder.build()).build();
                    channel.sendResponse(new ClusterStateResponse(clusterName, build, false));
                }
            );
            if (RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.get(s)) {
                newService.registerRequestHandler(
                    RemoteClusterNodesAction.TYPE.name(),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    RemoteClusterNodesAction.Request::new,
                    (request, channel, task) -> channel.sendResponse(new RemoteClusterNodesAction.Response(knownNodes))
                );
            }
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

    @SuppressForbidden(reason = "calls getLocalHost here but it's fine in this case")
    public void testSlowNodeCanBeCancelled() throws IOException, InterruptedException {
        try (ServerSocket socket = new MockServerSocket()) {
            socket.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0), 1);
            socket.setReuseAddress(true);
            DiscoveryNode seedNode = DiscoveryNodeUtils.create(
                "TEST",
                new TransportAddress(socket.getInetAddress(), socket.getLocalPort()),
                emptyMap(),
                emptySet()
            );
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

            try (
                MockTransportService service = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();
                CountDownLatch listenerCalled = new CountDownLatch(1);
                AtomicReference<Exception> exceptionReference = new AtomicReference<>();
                String clusterAlias = "test-cluster";
                Settings settings = buildRandomSettings(clusterAlias, addresses(seedNode));
                try (
                    RemoteClusterConnection connection = new RemoteClusterConnection(
                        settings,
                        clusterAlias,
                        service,
                        randomFrom(RemoteClusterCredentialsManager.EMPTY, buildCredentialsManager(clusterAlias))
                    )
                ) {
                    ActionListener<Void> listener = ActionListener.wrap(x -> {
                        listenerCalled.countDown();
                        fail("expected exception");
                    }, x -> {
                        exceptionReference.set(x);
                        listenerCalled.countDown();
                    });
                    connection.ensureConnected(listener);
                    acceptedLatch.await();
                    connection.close(); // now close it, this should trigger an interrupt on the socket and we can move on
                    assertTrue(connection.assertNoRunningConnections());
                }
                closeRemote.countDown();
                listenerCalled.await();
                Exception e = exceptionReference.get();
                assertNotNull(e);
                assertThat(e, either(instanceOf(AlreadyClosedException.class)).or(instanceOf(ConnectTransportException.class)));
            }
        }
    }

    private static List<String> addresses(final DiscoveryNode... seedNodes) {
        return Arrays.stream(seedNodes).map(s -> s.getAddress().toString()).collect(Collectors.toCollection(ArrayList::new));
    }

    public void testCloseWhileConcurrentlyConnecting() throws IOException, InterruptedException, BrokenBarrierException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport(
                "seed_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService seedTransport1 = startTransport(
                "seed_node_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService discoverableTransport = startTransport(
                "discoverable_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode seedNode1 = seedTransport1.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            knownNodes.add(seedTransport1.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            List<String> seedNodes = addresses(seedNode1, seedNode);
            Collections.shuffle(seedNodes, random());

            try (
                MockTransportService service = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();
                String clusterAlias = "test-cluster";
                Settings settings = buildRandomSettings(clusterAlias, seedNodes);
                try (
                    RemoteClusterConnection connection = new RemoteClusterConnection(
                        settings,
                        clusterAlias,
                        service,
                        RemoteClusterCredentialsManager.EMPTY
                    )
                ) {
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
                                        ActionListener<Void> listener = ActionListener.wrap(x -> {
                                            if (executed.compareAndSet(null, new RuntimeException())) {
                                                latch.countDown();
                                            } else {
                                                throw new AssertionError("shit's been called twice", executed.get());
                                            }
                                        }, x -> {
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
                                            connection.ensureConnected(listener);
                                        } catch (Exception e) {
                                            // it's ok if we're shutting down
                                            assertThat(e.getMessage(), containsString("threadcontext is already closed"));
                                            latch.countDown();
                                        }
                                    }
                                    safeAwait(latch);
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
        doTestGetConnectionInfo(false);
        doTestGetConnectionInfo(true);
    }

    private void doTestGetConnectionInfo(boolean hasClusterCredentials) throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Settings seedTransportSettings;
        if (hasClusterCredentials) {
            seedTransportSettings = Settings.builder()
                .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
                .put(RemoteClusterPortSettings.PORT.getKey(), "0")
                .build();
        } else {
            seedTransportSettings = Settings.EMPTY;
        }
        try (
            MockTransportService transport1 = startTransport(
                "seed_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                seedTransportSettings
            );
            MockTransportService transport2 = startTransport(
                "seed_node_1",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                seedTransportSettings
            );
            MockTransportService transport3 = startTransport(
                "discoverable_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                seedTransportSettings
            )
        ) {
            DiscoveryNode node1 = transport1.getLocalDiscoNode();
            DiscoveryNode node2 = transport3.getLocalDiscoNode();
            DiscoveryNode node3 = transport2.getLocalDiscoNode();
            if (hasClusterCredentials) {
                node1 = node1.withTransportAddress(transport1.boundRemoteAccessAddress().publishAddress());
                node2 = node2.withTransportAddress(transport3.boundRemoteAccessAddress().publishAddress());
                node3 = node3.withTransportAddress(transport2.boundRemoteAccessAddress().publishAddress());
            }
            knownNodes.add(node1);
            knownNodes.add(node2);
            knownNodes.add(node3);
            Collections.shuffle(knownNodes, random());
            List<String> seedNodes = addresses(node3, node1, node2);
            Collections.shuffle(seedNodes, random());

            try (
                MockTransportService service = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();
                int maxNumConnections = randomIntBetween(1, 5);
                String clusterAlias = "test-cluster";
                Settings settings = Settings.builder()
                    .put(buildSniffSettings(clusterAlias, seedNodes))
                    .put(SniffConnectionStrategy.REMOTE_CONNECTIONS_PER_CLUSTER.getKey(), maxNumConnections)
                    .build();
                if (hasClusterCredentials) {
                    final MockSecureSettings secureSettings = new MockSecureSettings();
                    secureSettings.setString(
                        RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS.getConcreteSettingForNamespace(clusterAlias).getKey(),
                        randomAlphaOfLength(20)
                    );
                    settings = Settings.builder().put(settings).setSecureSettings(secureSettings).build();
                }
                try (
                    RemoteClusterConnection connection = new RemoteClusterConnection(
                        settings,
                        clusterAlias,
                        service,
                        hasClusterCredentials ? buildCredentialsManager(clusterAlias) : RemoteClusterCredentialsManager.EMPTY
                    )
                ) {
                    // test no nodes connected
                    RemoteConnectionInfo remoteConnectionInfo = assertSerialization(connection.getConnectionInfo());
                    assertNotNull(remoteConnectionInfo);
                    SniffConnectionStrategy.SniffModeInfo sniffInfo = (SniffConnectionStrategy.SniffModeInfo) remoteConnectionInfo.modeInfo;
                    assertEquals(0, sniffInfo.numNodesConnected);
                    assertEquals(3, sniffInfo.seedNodes.size());
                    assertEquals(maxNumConnections, sniffInfo.maxConnectionsPerCluster);
                    assertEquals(clusterAlias, remoteConnectionInfo.clusterAlias);
                    assertEquals(hasClusterCredentials, remoteConnectionInfo.hasClusterCredentials);
                }
            }
        }
    }

    public void testRemoteConnectionInfo() throws IOException {
        List<String> remoteAddresses = Collections.singletonList("seed:1");
        String serverName = "the_server_name";

        RemoteConnectionInfo.ModeInfo modeInfo1;
        RemoteConnectionInfo.ModeInfo modeInfo2;

        if (randomBoolean()) {
            modeInfo1 = new SniffConnectionStrategy.SniffModeInfo(remoteAddresses, 4, 4);
            modeInfo2 = new SniffConnectionStrategy.SniffModeInfo(remoteAddresses, 4, 3);
        } else {
            modeInfo1 = new ProxyConnectionStrategy.ProxyModeInfo(remoteAddresses.get(0), serverName, 18, 18);
            modeInfo2 = new ProxyConnectionStrategy.ProxyModeInfo(remoteAddresses.get(0), serverName, 18, 17);
        }

        RemoteConnectionInfo stats = new RemoteConnectionInfo("test_cluster", modeInfo1, TimeValue.timeValueMinutes(30), false, false);
        assertSerialization(stats);

        RemoteConnectionInfo stats1 = new RemoteConnectionInfo("test_cluster", modeInfo1, TimeValue.timeValueMinutes(30), true, false);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster_1", modeInfo1, TimeValue.timeValueMinutes(30), false, false);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster", modeInfo1, TimeValue.timeValueMinutes(325), false, false);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster", modeInfo2, TimeValue.timeValueMinutes(30), false, false);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);

        stats1 = new RemoteConnectionInfo("test_cluster", modeInfo1, TimeValue.timeValueMinutes(30), false, true);
        assertSerialization(stats1);
        assertNotEquals(stats, stats1);
    }

    private static RemoteConnectionInfo assertSerialization(RemoteConnectionInfo info) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            info.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setTransportVersion(TransportVersion.current());
            RemoteConnectionInfo remoteConnectionInfo = new RemoteConnectionInfo(in);
            assertEquals(info, remoteConnectionInfo);
            assertEquals(info.hashCode(), remoteConnectionInfo.hashCode());
            return randomBoolean() ? info : remoteConnectionInfo;
        }
    }

    public void testRenderConnectionInfoXContent() throws IOException {
        List<String> remoteAddresses = Arrays.asList("seed:1", "seed:2");
        String serverName = "the_server_name";

        RemoteConnectionInfo.ModeInfo modeInfo;

        boolean sniff = randomBoolean();
        if (sniff) {
            modeInfo = new SniffConnectionStrategy.SniffModeInfo(remoteAddresses, 3, 2);
        } else {
            modeInfo = new ProxyConnectionStrategy.ProxyModeInfo(remoteAddresses.get(0), serverName, 18, 16);
        }
        final boolean hasClusterCredentials = randomBoolean();

        RemoteConnectionInfo stats = new RemoteConnectionInfo(
            "test_cluster",
            modeInfo,
            TimeValue.timeValueMinutes(30),
            true,
            hasClusterCredentials
        );
        stats = assertSerialization(stats);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();

        if (sniff) {
            assertEquals(XContentHelper.stripWhitespace(Strings.format("""
                {
                  "test_cluster": {
                    "connected": true,
                    "mode": "sniff",
                    "seeds": [ "seed:1", "seed:2" ],
                    "num_nodes_connected": 2,
                    "max_connections_per_cluster": 3,
                    "initial_connect_timeout": "30m",
                    "skip_unavailable": true%s
                  }
                }""", hasClusterCredentials ? ",\"cluster_credentials\":\"::es_redacted::\"" : "")), Strings.toString(builder));
        } else {
            assertEquals(XContentHelper.stripWhitespace(Strings.format("""
                {
                  "test_cluster": {
                    "connected": true,
                    "mode": "proxy",
                    "proxy_address": "seed:1",
                    "server_name": "the_server_name",
                    "num_proxy_sockets_connected": 16,
                    "max_proxy_socket_connections": 18,
                    "initial_connect_timeout": "30m",
                    "skip_unavailable": true%s
                  }
                }""", hasClusterCredentials ? ",\"cluster_credentials\":\"::es_redacted::\"" : "")), Strings.toString(builder));
        }
    }

    public void testCollectNodes() throws Exception {
        doTestCollectNodes(false);
        doTestCollectNodes(true);
    }

    private void doTestCollectNodes(boolean hasClusterCredentials) throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Settings seedTransportSettings;
        if (hasClusterCredentials) {
            seedTransportSettings = Settings.builder()
                .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
                .put(RemoteClusterPortSettings.PORT.getKey(), "0")
                .build();
        } else {
            seedTransportSettings = Settings.EMPTY;
        }

        try (
            MockTransportService seedTransport = startTransport(
                "seed_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                seedTransportSettings
            )
        ) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            if (hasClusterCredentials) {
                seedNode = seedNode.withTransportAddress(seedTransport.boundRemoteAccessAddress().publishAddress());
            }
            knownNodes.add(seedNode);
            try (
                MockTransportService service = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();
                service.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (hasClusterCredentials) {
                        assertThat(
                            action,
                            oneOf(RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME, RemoteClusterNodesAction.TYPE.name())
                        );
                    } else {
                        assertThat(action, oneOf(TransportService.HANDSHAKE_ACTION_NAME, ClusterStateAction.NAME));
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
                String clusterAlias = "test-cluster";
                Settings settings = buildRandomSettings(clusterAlias, addresses(seedNode));

                if (hasClusterCredentials) {
                    final MockSecureSettings secureSettings = new MockSecureSettings();
                    secureSettings.setString(
                        RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS.getConcreteSettingForNamespace(clusterAlias).getKey(),
                        randomAlphaOfLength(20)
                    );
                    settings = Settings.builder().put(settings).setSecureSettings(secureSettings).build();
                }

                try (
                    RemoteClusterConnection connection = new RemoteClusterConnection(
                        settings,
                        clusterAlias,
                        service,
                        hasClusterCredentials ? buildCredentialsManager(clusterAlias) : RemoteClusterCredentialsManager.EMPTY
                    )
                ) {
                    CountDownLatch responseLatch = new CountDownLatch(1);
                    AtomicReference<Function<String, DiscoveryNode>> reference = new AtomicReference<>();
                    AtomicReference<Exception> failReference = new AtomicReference<>();
                    ActionListener<Function<String, DiscoveryNode>> shardsListener = ActionListener.wrap(x -> {
                        reference.set(x);
                        responseLatch.countDown();
                    }, x -> {
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

    public void testNoChannelsExceptREG() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport(
                "seed_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            try (
                MockTransportService service = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();
                String clusterAlias = "test-cluster";
                Settings settings = buildRandomSettings(clusterAlias, addresses(seedNode));

                try (
                    RemoteClusterConnection connection = new RemoteClusterConnection(
                        settings,
                        clusterAlias,
                        service,
                        RemoteClusterCredentialsManager.EMPTY
                    )
                ) {
                    PlainActionFuture<Void> plainActionFuture = new PlainActionFuture<>();
                    connection.ensureConnected(plainActionFuture);
                    plainActionFuture.get(10, TimeUnit.SECONDS);

                    for (TransportRequestOptions.Type type : TransportRequestOptions.Type.values()) {
                        if (type != TransportRequestOptions.Type.REG) {
                            assertThat(
                                expectThrows(
                                    IllegalStateException.class,
                                    () -> connection.getConnection()
                                        .sendRequest(
                                            randomNonNegativeLong(),
                                            "arbitrary",
                                            new EmptyRequest(),
                                            TransportRequestOptions.of(null, type)
                                        )
                                ).getMessage(),
                                allOf(containsString("can't select"), containsString(type.toString()))
                            );
                        }
                    }
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
            for (int i = 0; i < numDiscoverableNodes; i++) {
                MockTransportService transportService = startTransport(
                    "discoverable_node" + i,
                    knownNodes,
                    VersionInformation.CURRENT,
                    TransportVersion.current()
                );
                discoverableNodes.add(transportService.getLocalNode());
                discoverableTransports.add(transportService);
            }

            List<String> seedNodes = new CopyOnWriteArrayList<>(
                randomSubsetOf(
                    randomIntBetween(1, discoverableNodes.size()),
                    discoverableNodes.stream().map(d -> d.getAddress().toString()).toList()
                )
            );
            Collections.shuffle(seedNodes, random());

            try (
                MockTransportService service = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();

                String clusterAlias = "test-cluster";
                Settings settings = buildRandomSettings(clusterAlias, seedNodes);
                try (
                    RemoteClusterConnection connection = new RemoteClusterConnection(
                        settings,
                        clusterAlias,
                        service,
                        randomFrom(RemoteClusterCredentialsManager.EMPTY, buildCredentialsManager(clusterAlias))
                    )
                ) {
                    final int numGetThreads = randomIntBetween(4, 10);
                    final Thread[] getThreads = new Thread[numGetThreads];
                    final int numModifyingThreads = randomIntBetween(4, 10);
                    final Thread[] modifyingThreads = new Thread[numModifyingThreads];
                    CyclicBarrier barrier = new CyclicBarrier(numGetThreads + numModifyingThreads);
                    for (int i = 0; i < getThreads.length; i++) {
                        final int numGetCalls = randomIntBetween(1000, 10000);
                        getThreads[i] = new Thread(() -> {
                            try {
                                safeAwait(barrier);
                                for (int j = 0; j < numGetCalls; j++) {
                                    try {
                                        Transport.Connection lowLevelConnection = connection.getConnection();
                                        assertNotNull(lowLevelConnection);
                                    } catch (ConnectTransportException e) {
                                        // ignore, this is an expected exception
                                    }
                                }
                            } catch (Exception ex) {
                                throw new AssertionError(ex);
                            }
                        });
                        getThreads[i].start();
                    }

                    for (int i = 0; i < modifyingThreads.length; i++) {
                        final int numDisconnects = randomIntBetween(5, 10);
                        modifyingThreads[i] = new Thread(() -> {
                            try {
                                safeAwait(barrier);
                                for (int j = 0; j < numDisconnects; j++) {
                                    DiscoveryNode node = randomFrom(discoverableNodes);
                                    try {
                                        connection.getConnectionManager().getConnection(node);
                                    } catch (ConnectTransportException e) {
                                        // Ignore
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

    public void testGetConnection() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport(
                "seed_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            );
            MockTransportService disconnectedTransport = startTransport(
                "disconnected_node",
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current()
            )
        ) {

            DiscoveryNode seedNode = seedTransport.getLocalNode();
            knownNodes.add(seedNode);

            DiscoveryNode disconnectedNode = disconnectedTransport.getLocalNode();

            try (
                MockTransportService service = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();
                String clusterAlias = "test-cluster";
                Settings settings = buildRandomSettings(clusterAlias, addresses(seedNode));
                try (
                    RemoteClusterConnection connection = new RemoteClusterConnection(
                        settings,
                        clusterAlias,
                        service,
                        RemoteClusterCredentialsManager.EMPTY
                    )
                ) {
                    safeAwait(listener -> connection.ensureConnected(listener.map(x -> null)));
                    for (int i = 0; i < 10; i++) {
                        // always a direct connection as the remote node is already connected
                        Transport.Connection remoteConnection = connection.getConnection(seedNode);
                        assertEquals(seedNode, remoteConnection.getNode());
                    }
                    for (int i = 0; i < 10; i++) {
                        // we don't use the transport service connection manager so we will get a proxy connection for the local node
                        Transport.Connection remoteConnection = connection.getConnection(service.getLocalNode());
                        assertThat(remoteConnection, instanceOf(RemoteConnectionManager.ProxyConnection.class));
                        assertThat(remoteConnection.getNode(), equalTo(service.getLocalNode()));
                    }
                    for (int i = 0; i < 10; i++) {
                        // always a proxy connection as the target node is not connected
                        Transport.Connection remoteConnection = connection.getConnection(disconnectedNode);
                        assertThat(remoteConnection, instanceOf(RemoteConnectionManager.ProxyConnection.class));
                        assertThat(remoteConnection.getNode(), sameInstance(disconnectedNode));
                    }
                }
            }
        }
    }

    private Settings buildRandomSettings(String clusterAlias, List<String> addresses) {
        if (randomBoolean()) {
            return buildProxySettings(clusterAlias, addresses);
        } else {
            return buildSniffSettings(clusterAlias, addresses);
        }
    }

    private static Settings buildProxySettings(String clusterAlias, List<String> addresses) {
        Settings.Builder builder = Settings.builder();
        builder.put(ProxyConnectionStrategy.PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias).getKey(), addresses.get(0));
        builder.put(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).getKey(), "proxy");
        return builder.build();
    }

    private static Settings buildSniffSettings(String clusterAlias, List<String> seedNodes) {
        Settings.Builder builder = Settings.builder();
        builder.put(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).getKey(), "sniff");
        builder.put(
            SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).getKey(),
            Strings.collectionToCommaDelimitedString(seedNodes)
        );
        return builder.build();
    }

    private static RemoteClusterCredentialsManager buildCredentialsManager(String clusterAlias) {
        Objects.requireNonNull(clusterAlias);
        final Settings.Builder builder = Settings.builder();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote." + clusterAlias + ".credentials", randomAlphaOfLength(20));
        builder.setSecureSettings(secureSettings);
        return new RemoteClusterCredentialsManager(builder.build());
    }
}
