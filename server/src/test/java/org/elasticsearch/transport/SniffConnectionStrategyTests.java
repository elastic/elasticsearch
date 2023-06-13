/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.remote.RemoteClusterNodesAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

public class SniffConnectionStrategyTests extends ESTestCase {

    private final String clusterAlias = "cluster-alias";
    private final String modeKey = RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).getKey();
    private Settings clientSettings;
    private ConnectionProfile profile;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private boolean hasClusterCredentials;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        hasClusterCredentials = randomBoolean();
        final Settings.Builder builder = Settings.builder().put(modeKey, "sniff");
        if (hasClusterCredentials) {
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(
                RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS.getConcreteSettingForNamespace(clusterAlias).getKey(),
                randomAlphaOfLength(20)
            );
            builder.setSecureSettings(secureSettings);
        }
        clientSettings = builder.build();
        profile = RemoteConnectionStrategy.buildConnectionProfile(clusterAlias, clientSettings, hasClusterCredentials);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(
        String id,
        List<DiscoveryNode> knownNodes,
        Version version,
        TransportVersion transportVersion
    ) {
        return startTransport(id, knownNodes, version, transportVersion, Settings.EMPTY);
    }

    public MockTransportService startTransport(
        final String id,
        final List<DiscoveryNode> knownNodes,
        final Version version,
        final TransportVersion transportVersion,
        final Settings settings
    ) {
        boolean success = false;
        final Settings s = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterAlias)
            .put("node.name", id)
            .put(settings)
            .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), hasClusterCredentials)
            .put(RemoteClusterPortSettings.PORT.getKey(), "0")
            .build();
        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(s);
        MockTransportService newService = MockTransportService.createNewService(s, version, transportVersion, threadPool);
        try {
            newService.registerRequestHandler(
                ClusterStateAction.NAME,
                ThreadPool.Names.SAME,
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
            if (hasClusterCredentials) {
                newService.registerRequestHandler(
                    RemoteClusterNodesAction.NAME,
                    ThreadPool.Names.SAME,
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

    public void testSniffStrategyWillConnectToAndDiscoverNodes() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService discoverableTransport = startTransport(
                "discoverable_node",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT
            )
        ) {
            DiscoveryNode seedNode = getLocalNode(seedTransport);
            DiscoveryNode discoverableNode = getLocalNode(discoverableTransport);
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    clientSettings,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                addSendBehaviour(localService);
                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        clientSettings,
                        3,
                        n -> true,
                        seedNodes(seedNode)
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(strategy.assertNoRunningConnections());
                }

            }
        }
    }

    public void testSniffStrategyWillResolveDiscoveryNodesEachConnect() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService discoverableTransport = startTransport(
                "discoverable_node",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT
            )
        ) {
            DiscoveryNode seedNode = getLocalNode(seedTransport);
            DiscoveryNode discoverableNode = getLocalNode(discoverableTransport);
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());

            CountDownLatch multipleResolveLatch = new CountDownLatch(2);
            Supplier<DiscoveryNode> seedNodeSupplier = () -> {
                multipleResolveLatch.countDown();
                return seedNode;
            };

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        Settings.EMPTY,
                        3,
                        n -> true,
                        seedNodes(seedNode),
                        Collections.singletonList(seedNodeSupplier)
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    // Closing connections leads to RemoteClusterConnection.ConnectHandler.collectRemoteNodes
                    connectionManager.getConnection(seedNode).close();

                    assertTrue(multipleResolveLatch.await(30L, TimeUnit.SECONDS));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                }
            }
        }
    }

    public void testSniffStrategyWillConnectToMaxAllowedNodesAndOpenNewConnectionsOnDisconnect() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService discoverableTransport1 = startTransport(
                "discoverable_node",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT
            );
            MockTransportService discoverableTransport2 = startTransport(
                "discoverable_node",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT
            )
        ) {
            DiscoveryNode seedNode = getLocalNode(seedTransport);
            DiscoveryNode discoverableNode1 = getLocalNode(discoverableTransport1);
            DiscoveryNode discoverableNode2 = getLocalNode(discoverableTransport2);
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode1);
            knownNodes.add(discoverableNode2);
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        Settings.EMPTY,
                        2,
                        n -> true,
                        seedNodes(seedNode)
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertEquals(2, connectionManager.size());
                    assertTrue(connectionManager.nodeConnected(seedNode));

                    // Assert that one of the discovered nodes is connected. After that, disconnect the connected
                    // discovered node and ensure the other discovered node is eventually connected
                    if (connectionManager.nodeConnected(discoverableNode1)) {
                        assertTrue(connectionManager.nodeConnected(discoverableNode1));
                        discoverableTransport1.close();
                        assertBusy(() -> assertTrue(connectionManager.nodeConnected(discoverableNode2)));
                    } else {
                        assertTrue(connectionManager.nodeConnected(discoverableNode2));
                        discoverableTransport2.close();
                        assertBusy(() -> assertTrue(connectionManager.nodeConnected(discoverableNode1)));
                    }
                }
            }
        }
    }

    public void testDiscoverWithSingleIncompatibleSeedNode() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        Version incompatibleVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        TransportVersion incompatibleTransportVersion = TransportVersionUtils.getPreviousVersion(TransportVersion.MINIMUM_COMPATIBLE);
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService incompatibleSeedTransport = startTransport(
                "discoverable_node",
                knownNodes,
                incompatibleVersion,
                incompatibleTransportVersion
            );
            MockTransportService discoverableTransport = startTransport(
                "discoverable_node",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT
            )
        ) {
            DiscoveryNode seedNode = getLocalNode(seedTransport);
            DiscoveryNode incompatibleSeedNode = getLocalNode(incompatibleSeedTransport);
            DiscoveryNode discoverableNode = getLocalNode(discoverableTransport);
            knownNodes.add(seedNode);
            knownNodes.add(incompatibleSeedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        Settings.EMPTY,
                        3,
                        n -> true,
                        seedNodes(seedNode)
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertEquals(2, connectionManager.size());
                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertFalse(connectionManager.nodeConnected(incompatibleSeedNode));
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testConnectFailsWithIncompatibleNodes() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        Version incompatibleVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        TransportVersion incompatibleTransportVersion = TransportVersionUtils.getPreviousVersion(TransportVersion.MINIMUM_COMPATIBLE);
        try (
            MockTransportService incompatibleSeedTransport = startTransport(
                "seed_node",
                knownNodes,
                incompatibleVersion,
                incompatibleTransportVersion
            )
        ) {
            DiscoveryNode incompatibleSeedNode = getLocalNode(incompatibleSeedTransport);
            knownNodes.add(incompatibleSeedNode);

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        Settings.EMPTY,
                        3,
                        n -> true,
                        seedNodes(incompatibleSeedNode)
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);

                    expectThrows(Exception.class, connectFuture::actionGet);
                    assertFalse(connectionManager.nodeConnected(incompatibleSeedNode));
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testFilterNodesWithNodePredicate() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService discoverableTransport = startTransport(
                "discoverable_node",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT
            )
        ) {
            DiscoveryNode seedNode = getLocalNode(seedTransport);
            DiscoveryNode discoverableNode = getLocalNode(discoverableTransport);
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            DiscoveryNode rejectedNode = randomBoolean() ? seedNode : discoverableNode;
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        Settings.EMPTY,
                        3,
                        n -> n.equals(rejectedNode) == false,
                        seedNodes(seedNode)
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    if (rejectedNode.equals(seedNode)) {
                        assertFalse(connectionManager.nodeConnected(seedNode));
                        assertTrue(connectionManager.nodeConnected(discoverableNode));
                    } else {
                        assertTrue(connectionManager.nodeConnected(seedNode));
                        assertFalse(connectionManager.nodeConnected(discoverableNode));
                    }
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testConnectFailsIfNoConnectionsOpened() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService closedTransport = startTransport(
                "discoverable_node",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT
            )
        ) {
            DiscoveryNode seedNode = getLocalNode(seedTransport);
            DiscoveryNode discoverableNode = getLocalNode(closedTransport);
            knownNodes.add(discoverableNode);
            closedTransport.close();

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                // Predicate excludes seed node as a possible connection
                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        Settings.EMPTY,
                        3,
                        n -> n.equals(seedNode) == false,
                        seedNodes(seedNode)
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    final IllegalStateException ise = expectThrows(IllegalStateException.class, connectFuture::actionGet);
                    assertEquals("Unable to open any connections to remote cluster [cluster-alias]", ise.getMessage());
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testClusterNameValidationPreventConnectingToDifferentClusters() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> otherKnownNodes = new CopyOnWriteArrayList<>();

        Settings otherSettings = Settings.builder().put("cluster.name", "otherCluster").build();

        try (
            MockTransportService seed = startTransport("other_seed", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService discoverable = startTransport("other_discoverable", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService otherSeed = startTransport(
                "other_seed",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT,
                otherSettings
            );
            MockTransportService otherDiscoverable = startTransport(
                "other_discoverable",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT,
                otherSettings
            )
        ) {
            DiscoveryNode seedNode = getLocalNode(seed);
            DiscoveryNode discoverableNode = getLocalNode(discoverable);
            DiscoveryNode otherSeedNode = getLocalNode(otherSeed);
            DiscoveryNode otherDiscoverableNode = getLocalNode(otherDiscoverable);
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());
            Collections.shuffle(otherKnownNodes, random());

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        Settings.EMPTY,
                        3,
                        n -> true,
                        seedNodes(seedNode, otherSeedNode)
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(strategy.assertNoRunningConnections());

                    seed.close();

                    assertBusy(strategy::assertNoRunningConnections);

                    PlainActionFuture<Void> newConnect = PlainActionFuture.newFuture();
                    strategy.connect(newConnect);
                    IllegalStateException ise = expectThrows(IllegalStateException.class, newConnect::actionGet);
                    assertThat(
                        ise.getMessage(),
                        allOf(
                            startsWith("handshake with [{cluster-alias#"),
                            endsWith(
                                " failed: remote cluster name [otherCluster] "
                                    + "does not match expected remote cluster name ["
                                    + clusterAlias
                                    + "]"
                            )
                        )
                    );

                    assertFalse(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertFalse(connectionManager.nodeConnected(otherSeedNode));
                    assertFalse(connectionManager.nodeConnected(otherDiscoverableNode));
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testMultipleCallsToConnectEnsuresConnection() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService discoverableTransport = startTransport(
                "discoverable_node",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT
            )
        ) {
            DiscoveryNode seedNode = getLocalNode(seedTransport);
            DiscoveryNode discoverableNode = getLocalNode(discoverableTransport);
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        Settings.EMPTY,
                        3,
                        n -> true,
                        seedNodes(seedNode)
                    )
                ) {
                    assertFalse(connectionManager.nodeConnected(seedNode));
                    assertFalse(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(strategy.assertNoRunningConnections());

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(strategy.assertNoRunningConnections());

                    // exec again we are already connected
                    PlainActionFuture<Void> ensureConnectFuture = PlainActionFuture.newFuture();
                    strategy.connect(ensureConnectFuture);
                    ensureConnectFuture.actionGet();

                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testConfiguredProxyAddressModeWillReplaceNodeAddress() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService accessible = startTransport("seed_node", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService unresponsive1 = MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                TransportVersion.CURRENT,
                threadPool
            );
            MockTransportService unresponsive2 = MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                TransportVersion.CURRENT,
                threadPool
            )
        ) {
            // We start in order to get a valid address + port, but do not start accepting connections as we
            // will not actually connect to these transports
            unresponsive1.start();
            unresponsive2.start();
            DiscoveryNode accessibleNode = getLocalNode(accessible);
            DiscoveryNode discoverableNode = getLocalNode(unresponsive2);

            // Use the address for the node that will not respond
            DiscoveryNode unaddressableSeedNode = new DiscoveryNode(
                accessibleNode.getName(),
                accessibleNode.getId(),
                accessibleNode.getEphemeralId(),
                accessibleNode.getHostName(),
                accessibleNode.getHostAddress(),
                getLocalNode(unresponsive1).getAddress(),
                accessibleNode.getAttributes(),
                accessibleNode.getRoles(),
                accessibleNode.getVersion()
            );

            knownNodes.add(unaddressableSeedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                StubbableTransport transport = new StubbableTransport(localService.transport);
                AtomicReference<TransportAddress> discoverableNodeAddress = new AtomicReference<>();
                transport.setDefaultConnectBehavior((delegate, node, profile, listener) -> {
                    if (node.equals(discoverableNode)) {
                        // Do not actually try to connect because the node will not respond. Just capture the
                        // address for later assertion
                        discoverableNodeAddress.set(node.getAddress());
                        listener.onFailure(new ConnectTransportException(node, "general failure"));
                    } else {
                        delegate.openConnection(node, profile, listener);
                    }
                });

                List<String> seedNodes = Collections.singletonList(accessibleNode.toString());
                TransportAddress proxyAddress = accessibleNode.getAddress();
                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        proxyAddress.toString(),
                        Settings.EMPTY,
                        3,
                        n -> true,
                        seedNodes
                    )
                ) {
                    assertFalse(connectionManager.nodeConnected(unaddressableSeedNode));
                    assertFalse(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(strategy.assertNoRunningConnections());

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.nodeConnected(unaddressableSeedNode));
                    // Connection to discoverable will fail due to the stubbable transport
                    assertFalse(connectionManager.nodeConnected(discoverableNode));
                    assertEquals(proxyAddress, discoverableNodeAddress.get());
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testSniffStrategyWillNeedToBeRebuiltIfNumOfConnectionsOrSeedsOrProxyChange() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT, TransportVersion.CURRENT);
            MockTransportService discoverableTransport = startTransport(
                "discoverable_node",
                knownNodes,
                Version.CURRENT,
                TransportVersion.CURRENT
            )
        ) {
            DiscoveryNode seedNode = getLocalNode(seedTransport);
            DiscoveryNode discoverableNode = getLocalNode(discoverableTransport);
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    Version.CURRENT,
                    TransportVersion.CURRENT,
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    SniffConnectionStrategy strategy = new SniffConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        null,
                        Settings.EMPTY,
                        3,
                        n -> true,
                        seedNodes(seedNode)
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.nodeConnected(seedNode));
                    assertTrue(connectionManager.nodeConnected(discoverableNode));
                    assertTrue(strategy.assertNoRunningConnections());

                    Setting<?> seedSetting = SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("cluster-alias");
                    Setting<?> proxySetting = SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace("cluster-alias");
                    Setting<?> numConnections = SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS.getConcreteSettingForNamespace(
                        "cluster-alias"
                    );

                    Settings noChange = Settings.builder()
                        .put(seedSetting.getKey(), Strings.arrayToCommaDelimitedString(seedNodes(seedNode).toArray()))
                        .put(numConnections.getKey(), 3)
                        .build();
                    assertFalse(strategy.shouldRebuildConnection(noChange));
                    Settings seedsChanged = Settings.builder()
                        .put(seedSetting.getKey(), Strings.arrayToCommaDelimitedString(seedNodes(discoverableNode).toArray()))
                        .build();
                    assertTrue(strategy.shouldRebuildConnection(seedsChanged));
                    Settings proxyChanged = Settings.builder()
                        .put(seedSetting.getKey(), Strings.arrayToCommaDelimitedString(seedNodes(seedNode).toArray()))
                        .put(proxySetting.getKey(), "proxy_address:9300")
                        .build();
                    assertTrue(strategy.shouldRebuildConnection(proxyChanged));
                    Settings connectionsChanged = Settings.builder()
                        .put(seedSetting.getKey(), Strings.arrayToCommaDelimitedString(seedNodes(seedNode).toArray()))
                        .put(numConnections.getKey(), 4)
                        .build();
                    assertTrue(strategy.shouldRebuildConnection(connectionsChanged));
                }
            }
        }
    }

    public void testGetNodePredicateNodeRoles() {
        TransportAddress address = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        Predicate<DiscoveryNode> nodePredicate = SniffConnectionStrategy.getNodePredicate(Settings.EMPTY);
        {
            DiscoveryNode all = DiscoveryNodeUtils.create("id", address);
            assertTrue(nodePredicate.test(all));
        }
        {
            DiscoveryNode dataMaster = DiscoveryNodeUtils.create(
                "id",
                address,
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)
            );
            assertTrue(nodePredicate.test(dataMaster));
        }
        {
            DiscoveryNode dedicatedMaster = DiscoveryNodeUtils.create(
                "id",
                address,
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE)
            );
            assertFalse(nodePredicate.test(dedicatedMaster));
        }
        {
            DiscoveryNode dedicatedIngest = DiscoveryNodeUtils.create(
                "id",
                address,
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.INGEST_ROLE)
            );
            assertTrue(nodePredicate.test(dedicatedIngest));
        }
        {
            DiscoveryNode masterIngest = DiscoveryNodeUtils.create(
                "id",
                address,
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.MASTER_ROLE)
            );
            assertTrue(nodePredicate.test(masterIngest));
        }
        {
            DiscoveryNode dedicatedData = DiscoveryNodeUtils.create(
                "id",
                address,
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.DATA_ROLE)
            );
            assertTrue(nodePredicate.test(dedicatedData));
        }
        {
            DiscoveryNode ingestData = DiscoveryNodeUtils.create(
                "id",
                address,
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE)
            );
            assertTrue(nodePredicate.test(ingestData));
        }
        {
            DiscoveryNode coordOnly = DiscoveryNodeUtils.create("id", address, Collections.emptyMap(), Set.of());
            assertTrue(nodePredicate.test(coordOnly));
        }
    }

    public void testGetNodePredicateNodeVersion() {
        TransportAddress address = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        Predicate<DiscoveryNode> nodePredicate = SniffConnectionStrategy.getNodePredicate(Settings.EMPTY);
        Version version = VersionUtils.randomVersion(random());
        DiscoveryNode node = DiscoveryNodeUtils.builder("id").address(address).version(version).build();
        assertThat(nodePredicate.test(node), equalTo(Version.CURRENT.isCompatible(version)));
    }

    public void testGetNodePredicateNodeAttrs() {
        TransportAddress address = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        Set<DiscoveryNodeRole> roles = DiscoveryNodeRole.roles();
        Settings settings = Settings.builder().put("cluster.remote.node.attr", "gateway").build();
        Predicate<DiscoveryNode> nodePredicate = SniffConnectionStrategy.getNodePredicate(settings);
        {
            DiscoveryNode nonGatewayNode = DiscoveryNodeUtils.create("id", address, Collections.singletonMap("gateway", "false"), roles);
            assertFalse(nodePredicate.test(nonGatewayNode));
            assertTrue(SniffConnectionStrategy.getNodePredicate(Settings.EMPTY).test(nonGatewayNode));
        }
        {
            DiscoveryNode gatewayNode = DiscoveryNodeUtils.create("id", address, Collections.singletonMap("gateway", "true"), roles);
            assertTrue(nodePredicate.test(gatewayNode));
            assertTrue(SniffConnectionStrategy.getNodePredicate(Settings.EMPTY).test(gatewayNode));
        }
        {
            DiscoveryNode noAttrNode = DiscoveryNodeUtils.create("id", address, Collections.emptyMap(), roles);
            assertFalse(nodePredicate.test(noAttrNode));
            assertTrue(SniffConnectionStrategy.getNodePredicate(Settings.EMPTY).test(noAttrNode));
        }
    }

    public void testGetNodePredicatesCombination() {
        TransportAddress address = new TransportAddress(TransportAddress.META_ADDRESS, 0);
        Settings settings = Settings.builder().put("cluster.remote.node.attr", "gateway").build();
        Predicate<DiscoveryNode> nodePredicate = SniffConnectionStrategy.getNodePredicate(settings);
        Set<DiscoveryNodeRole> allRoles = DiscoveryNodeRole.roles();
        Set<DiscoveryNodeRole> dedicatedMasterRoles = Set.of(DiscoveryNodeRole.MASTER_ROLE);
        {
            DiscoveryNode node = DiscoveryNodeUtils.create(
                "id",
                address,
                Collections.singletonMap("gateway", "true"),
                dedicatedMasterRoles
            );
            assertFalse(nodePredicate.test(node));
        }
        {
            DiscoveryNode node = DiscoveryNodeUtils.create(
                "id",
                address,
                Collections.singletonMap("gateway", "false"),
                dedicatedMasterRoles
            );
            assertFalse(nodePredicate.test(node));
        }
        {
            DiscoveryNode node = DiscoveryNodeUtils.create(
                "id",
                address,
                Collections.singletonMap("gateway", "false"),
                dedicatedMasterRoles
            );
            assertFalse(nodePredicate.test(node));
        }
        {
            DiscoveryNode node = DiscoveryNodeUtils.create("id", address, Collections.singletonMap("gateway", "true"), allRoles);
            assertTrue(nodePredicate.test(node));
        }
    }

    public void testModeSettingsCannotBeUsedWhenInDifferentMode() {
        List<Tuple<Setting.AffixSetting<?>, String>> restrictedSettings = Arrays.asList(
            new Tuple<>(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS, "192.168.0.1:8080"),
            new Tuple<>(SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS, "2")
        );

        RemoteConnectionStrategy.ConnectionStrategy proxy = RemoteConnectionStrategy.ConnectionStrategy.PROXY;

        String clusterName = "cluster_name";
        Settings settings = Settings.builder()
            .put(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterName).getKey(), proxy.name())
            .build();

        Set<Setting<?>> clusterSettings = new HashSet<>();
        clusterSettings.add(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE);
        clusterSettings.addAll(restrictedSettings.stream().map(Tuple::v1).toList());
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, clusterSettings);

        // Should validate successfully
        service.validate(settings, true);

        for (Tuple<Setting.AffixSetting<?>, String> restrictedSetting : restrictedSettings) {
            Setting<?> concreteSetting = restrictedSetting.v1().getConcreteSettingForNamespace(clusterName);
            Settings invalid = Settings.builder().put(settings).put(concreteSetting.getKey(), restrictedSetting.v2()).build();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> service.validate(invalid, true));
            String expected = Strings.format("""
                Setting "%s" cannot be used with the configured "cluster.remote.cluster_name.mode" \
                [required=SNIFF, configured=PROXY]\
                """, concreteSetting.getKey());
            assertEquals(expected, iae.getMessage());
        }
    }

    private void addSendBehaviour(MockTransportService transportService) {
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (hasClusterCredentials) {
                assertThat(action, oneOf(RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME, RemoteClusterNodesAction.NAME));
            } else {
                assertThat(action, oneOf(TransportService.HANDSHAKE_ACTION_NAME, ClusterStateAction.NAME));
            }
            connection.sendRequest(requestId, action, request, options);
        });
    }

    private static List<String> seedNodes(final DiscoveryNode... seedNodes) {
        return Arrays.stream(seedNodes).map(s -> s.getAddress().toString()).toList();
    }

    private static DiscoveryNode getLocalNode(MockTransportService transportService) {
        final BoundTransportAddress remoteClusterServerAddress = transportService.boundRemoteAccessAddress();
        if (remoteClusterServerAddress != null) {
            return transportService.getLocalNode().withTransportAddress(remoteClusterServerAddress.publishAddress());
        } else {
            return transportService.getLocalNode();
        }
    }
}
