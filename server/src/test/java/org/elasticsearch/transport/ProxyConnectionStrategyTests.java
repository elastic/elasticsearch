/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class ProxyConnectionStrategyTests extends ESTestCase {

    private final String clusterAlias = "cluster-alias";
    private final String modeKey = RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).getKey();
    private final Settings settings = Settings.builder().put(modeKey, "proxy").build();
    private final ConnectionProfile profile = RemoteConnectionStrategy.buildConnectionProfile("cluster", settings);
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, Version version) {
        return startTransport(id, version, Settings.EMPTY);
    }

    public MockTransportService startTransport(final String id, final Version version, final Settings settings) {
        boolean success = false;
        final Settings s = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterAlias)
            .put("node.name", id)
            .put(settings)
            .build();
        MockTransportService newService = MockTransportService.createNewService(s, version, threadPool);
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
        try (MockTransportService transport1 = startTransport("node1", Version.CURRENT)) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
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
            MockTransportService transport1 = startTransport("node1", Version.CURRENT);
            MockTransportService transport2 = startTransport("node2", Version.CURRENT)
        ) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();
            TransportAddress address2 = transport2.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
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
        Version incompatibleVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        try (MockTransportService transport1 = startTransport("incompatible-node", incompatibleVersion)) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
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
                    assertThat(
                        expectThrows(NoSeedNodeLeftException.class, connectFuture::actionGet).getMessage(),
                        allOf(containsString("Unable to open any proxy connections"), containsString('[' + clusterAlias + ']'))
                    );

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
            MockTransportService transport1 = startTransport("cluster1", Version.CURRENT);
            MockTransportService transport2 = startTransport("cluster2", Version.CURRENT, otherSettings)
        ) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();
            TransportAddress address2 = transport2.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
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
        try (MockTransportService transport1 = startTransport("seed_node", Version.CURRENT)) {
            TransportAddress address = transport1.boundAddress().publishAddress();

            CountDownLatch multipleResolveLatch = new CountDownLatch(2);
            Supplier<TransportAddress> addressSupplier = () -> {
                multipleResolveLatch.countDown();
                return address;
            };

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
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
        try (MockTransportService remoteTransport = startTransport("node1", Version.CURRENT)) {
            TransportAddress remoteAddress = remoteTransport.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
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
        clusterSettings.addAll(restrictedSettings.stream().map(Tuple::v1).collect(Collectors.toList()));
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, clusterSettings);

        // Should validate successfully
        service.validate(settings, true);

        for (Tuple<Setting.AffixSetting<?>, String> restrictedSetting : restrictedSettings) {
            Setting<?> concreteSetting = restrictedSetting.v1().getConcreteSettingForNamespace(clusterName);
            Settings invalid = Settings.builder().put(settings).put(concreteSetting.getKey(), restrictedSetting.v2()).build();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> service.validate(invalid, true));
            String expected = "Setting \""
                + concreteSetting.getKey()
                + "\" cannot be used with the configured "
                + "\"cluster.remote.cluster_name.mode\" [required=PROXY, configured=SNIFF]";
            assertEquals(expected, iae.getMessage());
        }
    }

    public void testServerNameAttributes() {
        Settings bindSettings = Settings.builder().put(TransportSettings.BIND_HOST.getKey(), "localhost").build();
        try (MockTransportService transport1 = startTransport("node1", Version.CURRENT, bindSettings)) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
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
