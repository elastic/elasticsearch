/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class ProxyConnectionStrategyTests extends ESTestCase {

    private final String clusterAlias = "cluster-alias";
    private final String modeKey = RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).getKey();
    private final Settings settings = Settings.builder().put(modeKey, "proxy").build();
    private final ConnectionProfile profile = RemoteConnectionStrategy.buildConnectionProfile("cluster", settings, false);
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, VersionInformation version, TransportVersion transportVersion) {
        return startTransport(id, version, transportVersion, Settings.EMPTY);
    }

    public MockTransportService startTransport(
        String id,
        VersionInformation version,
        TransportVersion transportVersion,
        Settings settings
    ) {
        boolean success = false;
        final Settings s = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterAlias)
            .put("node.name", id)
            .put(settings)
            .build();
        MockTransportService newService = MockTransportService.createNewService(s, version, transportVersion, threadPool);
        try {
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

    public void testProxyStrategyWillOpenExpectedNumberOfConnectionsToAddress() {
        try (MockTransportService transport1 = startTransport("node1", VersionInformation.CURRENT, TransportVersion.current())) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();

            try (
                MockTransportService localService = spy(
                    MockTransportService.createNewService(
                        Settings.EMPTY,
                        VersionInformation.CURRENT,
                        TransportVersion.current(),
                        threadPool
                    )
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                // Handshake (as part of cluster name validation) should go through the internal remote connection
                // So it can be intercepted accordingly
                doAnswer(invocation -> {
                    final var connection = (Transport.Connection) invocation.getArgument(0);
                    final Optional<String> optionalClusterAlias = RemoteConnectionManager.resolveRemoteClusterAlias(connection);
                    assertTrue(optionalClusterAlias.isPresent());
                    assertEquals(clusterAlias, optionalClusterAlias.get());
                    invocation.callRealMethod();
                    return null;
                }).when(localService).handshake(any(), any(), any(), any());

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                int numOfConnections = randomIntBetween(4, 8);
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    ProxyConnectionStrategy strategy = new ProxyConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        Settings.EMPTY,
                        numOfConnections,
                        address1.toString()
                    )
                ) {
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertEquals(numOfConnections, connectionManager.size());
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    // This test has failed once or twice in the past. This is enabled in case it were to fail again.
    @TestLogging(
        value = "org.elasticsearch.transport.ClusterConnectionManager:TRACE,org.elasticsearch.transport.ProxyConnectionStrategy:TRACE",
        reason = "to ensure that connections are logged"
    )
    public void testProxyStrategyWillOpenNewConnectionsOnDisconnect() throws Exception {
        try (
            MockTransportService transport1 = startTransport("node1", VersionInformation.CURRENT, TransportVersion.current());
            MockTransportService transport2 = startTransport("node2", VersionInformation.CURRENT, TransportVersion.current())
        ) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();
            TransportAddress address2 = transport2.boundAddress().publishAddress();

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
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
                int numOfConnections = randomIntBetween(4, 8);

                AtomicBoolean useAddress1 = new AtomicBoolean(true);

                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    ProxyConnectionStrategy strategy = new ProxyConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        Settings.EMPTY,
                        numOfConnections,
                        address1.toString(),
                        alternatingResolver(address1, address2, useAddress1),
                        null
                    )
                ) {
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    long initialConnectionsToTransport2 = connectionManager.getAllConnectedNodes()
                        .stream()
                        .filter(n -> n.getAddress().equals(address2))
                        .count();
                    assertEquals(0, initialConnectionsToTransport2);
                    assertEquals(numOfConnections, connectionManager.size());
                    assertTrue(strategy.assertNoRunningConnections());
                    useAddress1.set(false);

                    transport1.close();

                    assertBusy(() -> {
                        assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                        // Connections now pointing to transport2
                        long finalConnectionsToTransport2 = connectionManager.getAllConnectedNodes()
                            .stream()
                            .filter(n -> n.getAddress().equals(address2))
                            .count();
                        assertNotEquals(0, finalConnectionsToTransport2);
                        assertEquals(numOfConnections, connectionManager.size());
                        assertTrue(strategy.assertNoRunningConnections());
                    });
                }
            }
        }
    }

    public void testConnectFailsWithIncompatibleNodes() {
        VersionInformation incompatibleVersion = new VersionInformation(
            Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion(),
            IndexVersion.MINIMUM_COMPATIBLE,
            IndexVersion.current()
        );
        TransportVersion incompatibleTransportVersion = TransportVersionUtils.getPreviousVersion(TransportVersion.MINIMUM_COMPATIBLE);
        try (MockTransportService transport1 = startTransport("incompatible-node", incompatibleVersion, incompatibleTransportVersion)) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
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
                int numOfConnections = randomIntBetween(4, 8);
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    ProxyConnectionStrategy strategy = new ProxyConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        Settings.EMPTY,
                        numOfConnections,
                        address1.toString()
                    )
                ) {

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    final NoSeedNodeLeftException exception = expectThrows(NoSeedNodeLeftException.class, connectFuture::actionGet);
                    assertThat(
                        exception.getMessage(),
                        allOf(
                            containsString("Unable to open any proxy connections"),
                            containsString('[' + clusterAlias + ']'),
                            containsString("at address [" + address1 + "]")
                        )
                    );
                    assertThat(exception.getSuppressed(), hasItemInArray(instanceOf(ConnectTransportException.class)));

                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertEquals(0, connectionManager.size());
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testConnectFailsWithNonRetryableException() {
        try (MockTransportService transport1 = startTransport("remote", VersionInformation.CURRENT, TransportVersion.current())) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool
                )
            ) {
                if (randomBoolean()) {
                    transport1.addRequestHandlingBehavior(
                        TransportService.HANDSHAKE_ACTION_NAME,
                        (handler, request, channel, task) -> channel.sendResponse(new ElasticsearchException("non-retryable"))
                    );
                } else {
                    localService.addSendBehavior(address1, (connection, requestId, action, request, options) -> {
                        throw new ElasticsearchException("non-retryable");
                    });
                }

                localService.start();
                localService.acceptIncomingRequests();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                int numOfConnections = randomIntBetween(4, 8);
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    ProxyConnectionStrategy strategy = new ProxyConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        Settings.EMPTY,
                        numOfConnections,
                        address1.toString()
                    )
                ) {

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    final ElasticsearchException exception = expectThrows(ElasticsearchException.class, connectFuture::actionGet);
                    assertThat(exception.getMessage(), containsString("non-retryable"));

                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertEquals(0, connectionManager.size());
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testClusterNameValidationPreventConnectingToDifferentClusters() throws Exception {
        Settings otherSettings = Settings.builder().put("cluster.name", "otherCluster").build();

        try (
            MockTransportService transport1 = startTransport("cluster1", VersionInformation.CURRENT, TransportVersion.current());
            MockTransportService transport2 = startTransport(
                "cluster2",
                VersionInformation.CURRENT,
                TransportVersion.current(),
                otherSettings
            )
        ) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();
            TransportAddress address2 = transport2.boundAddress().publishAddress();

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
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
                int numOfConnections = randomIntBetween(4, 8);

                AtomicBoolean useAddress1 = new AtomicBoolean(true);

                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    ProxyConnectionStrategy strategy = new ProxyConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        Settings.EMPTY,
                        numOfConnections,
                        address1.toString(),
                        alternatingResolver(address1, address2, useAddress1),
                        null
                    )
                ) {
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));
                    useAddress1.set(false);

                    transport1.close();

                    assertBusy(() -> {
                        assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                        assertTrue(strategy.assertNoRunningConnections());

                        long finalConnectionsToTransport2 = connectionManager.getAllConnectedNodes()
                            .stream()
                            .filter(n -> n.getAddress().equals(address2))
                            .count();

                        // Connections not pointing to transport2 because the cluster name is different
                        assertEquals(0, finalConnectionsToTransport2);
                        assertEquals(0, connectionManager.size());
                    });
                }
            }
        }
    }

    public void testProxyStrategyWillResolveAddressesEachConnect() throws Exception {
        try (MockTransportService transport1 = startTransport("seed_node", VersionInformation.CURRENT, TransportVersion.current())) {
            TransportAddress address = transport1.boundAddress().publishAddress();

            CountDownLatch multipleResolveLatch = new CountDownLatch(2);
            Supplier<TransportAddress> addressSupplier = () -> {
                multipleResolveLatch.countDown();
                return address;
            };

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
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
                int numOfConnections = randomIntBetween(4, 8);
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    ProxyConnectionStrategy strategy = new ProxyConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        Settings.EMPTY,
                        numOfConnections,
                        address.toString(),
                        addressSupplier,
                        null
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    remoteConnectionManager.getAnyRemoteConnection().close();

                    assertTrue(multipleResolveLatch.await(30L, TimeUnit.SECONDS));
                }
            }
        }
    }

    public void testProxyStrategyWillNeedToBeRebuiltIfNumOfSocketsOrAddressesOrServerNameChange() {
        try (MockTransportService remoteTransport = startTransport("node1", VersionInformation.CURRENT, TransportVersion.current())) {
            TransportAddress remoteAddress = remoteTransport.boundAddress().publishAddress();

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
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
                int numOfConnections = randomIntBetween(4, 8);
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    ProxyConnectionStrategy strategy = new ProxyConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        Settings.EMPTY,
                        numOfConnections,
                        remoteAddress.toString(),
                        "server-name"
                    )
                ) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(remoteAddress)));
                    assertEquals(numOfConnections, connectionManager.size());
                    assertTrue(strategy.assertNoRunningConnections());

                    Setting<?> modeSetting = RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(
                        "cluster-alias"
                    );
                    Setting<?> addressesSetting = ProxyConnectionStrategy.PROXY_ADDRESS.getConcreteSettingForNamespace("cluster-alias");
                    Setting<?> socketConnections = ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(
                        "cluster-alias"
                    );
                    Setting<?> serverName = ProxyConnectionStrategy.SERVER_NAME.getConcreteSettingForNamespace("cluster-alias");

                    Settings noChange = Settings.builder()
                        .put(modeSetting.getKey(), "proxy")
                        .put(addressesSetting.getKey(), remoteAddress.toString())
                        .put(socketConnections.getKey(), numOfConnections)
                        .put(serverName.getKey(), "server-name")
                        .build();
                    assertFalse(strategy.shouldRebuildConnection(noChange));
                    Settings addressesChanged = Settings.builder()
                        .put(modeSetting.getKey(), "proxy")
                        .put(addressesSetting.getKey(), remoteAddress.toString())
                        .build();
                    assertTrue(strategy.shouldRebuildConnection(addressesChanged));
                    Settings socketsChanged = Settings.builder()
                        .put(modeSetting.getKey(), "proxy")
                        .put(addressesSetting.getKey(), remoteAddress.toString())
                        .put(socketConnections.getKey(), numOfConnections + 1)
                        .build();
                    assertTrue(strategy.shouldRebuildConnection(socketsChanged));
                    Settings serverNameChange = Settings.builder()
                        .put(modeSetting.getKey(), "proxy")
                        .put(addressesSetting.getKey(), remoteAddress.toString())
                        .put(socketConnections.getKey(), numOfConnections)
                        .put(serverName.getKey(), "server-name2")
                        .build();
                    assertTrue(strategy.shouldRebuildConnection(serverNameChange));
                }
            }
        }
    }

    public void testModeSettingsCannotBeUsedWhenInDifferentMode() {
        List<Tuple<Setting.AffixSetting<?>, String>> restrictedSettings = Arrays.asList(
            new Tuple<>(ProxyConnectionStrategy.PROXY_ADDRESS, "192.168.0.1:8080"),
            new Tuple<>(ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS, "3")
        );

        RemoteConnectionStrategy.ConnectionStrategy sniff = RemoteConnectionStrategy.ConnectionStrategy.SNIFF;

        String clusterName = "cluster_name";
        Settings settings = Settings.builder()
            .put(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterName).getKey(), sniff.name())
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
                [required=PROXY, configured=SNIFF]\
                """, concreteSetting.getKey());
            assertEquals(expected, iae.getMessage());
        }
    }

    public void testServerNameAttributes() {
        Settings bindSettings = Settings.builder().put(TransportSettings.BIND_HOST.getKey(), "localhost").build();
        try (
            MockTransportService transport1 = startTransport("node1", VersionInformation.CURRENT, TransportVersion.current(), bindSettings)
        ) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();

            try (
                MockTransportService localService = MockTransportService.createNewService(
                    Settings.EMPTY,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool
                )
            ) {
                localService.start();
                localService.acceptIncomingRequests();

                String address = "localhost:" + address1.getPort();

                final ClusterConnectionManager connectionManager = new ClusterConnectionManager(
                    profile,
                    localService.transport,
                    threadPool.getThreadContext()
                );
                int numOfConnections = randomIntBetween(4, 8);
                try (
                    RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                    ProxyConnectionStrategy strategy = new ProxyConnectionStrategy(
                        clusterAlias,
                        localService,
                        remoteConnectionManager,
                        Settings.EMPTY,
                        numOfConnections,
                        address,
                        "localhost"
                    )
                ) {
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertTrue(strategy.assertNoRunningConnections());

                    DiscoveryNode discoveryNode = connectionManager.getAllConnectedNodes().stream().findFirst().get();
                    assertEquals("localhost", discoveryNode.getAttributes().get("server_name"));
                }
            }
        }
    }

    private Supplier<TransportAddress> alternatingResolver(
        TransportAddress address1,
        TransportAddress address2,
        AtomicBoolean useAddress1
    ) {
        return () -> {
            if (useAddress1.get()) {
                return address1;
            } else {
                return address2;
            }
        };
    }
}
