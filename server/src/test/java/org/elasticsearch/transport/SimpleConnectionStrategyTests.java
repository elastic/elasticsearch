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

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SimpleConnectionStrategyTests extends ESTestCase {

    private final String clusterAlias = "cluster-alias";
    private final String modeKey = RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).getKey();
    private final Settings settings = Settings.builder().put(modeKey, "simple").build();
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

    public void testSimpleStrategyWillOpenExpectedNumberOfConnectionsToAddresses() {
        try (MockTransportService transport1 = startTransport("node1", Version.CURRENT);
             MockTransportService transport2 = startTransport("node2", Version.CURRENT)) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();
            TransportAddress address2 = transport2.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                int numOfConnections = randomIntBetween(4, 8);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SimpleConnectionStrategy strategy = new SimpleConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         numOfConnections, addresses(address1, address2))) {
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertTrue(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));
                    assertEquals(numOfConnections, connectionManager.size());
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testSimpleStrategyWillOpenNewConnectionsOnDisconnect() throws Exception {
        try (MockTransportService transport1 = startTransport("node1", Version.CURRENT);
             MockTransportService transport2 = startTransport("node2", Version.CURRENT)) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();
            TransportAddress address2 = transport2.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                int numOfConnections = randomIntBetween(4, 8);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SimpleConnectionStrategy strategy = new SimpleConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         numOfConnections, addresses(address1, address2))) {
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertTrue(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    long initialConnectionsToTransport2 = connectionManager.getAllConnectedNodes().stream()
                        .filter(n -> n.getAddress().equals(address2))
                        .count();
                    assertNotEquals(0, initialConnectionsToTransport2);
                    assertEquals(numOfConnections, connectionManager.size());
                    assertTrue(strategy.assertNoRunningConnections());

                    transport1.close();

                    assertBusy(() -> {
                        assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                        // More connections now pointing to transport2
                        long finalConnectionsToTransport2 = connectionManager.getAllConnectedNodes().stream()
                            .filter(n -> n.getAddress().equals(address2))
                            .count();
                        assertTrue(finalConnectionsToTransport2 > initialConnectionsToTransport2);
                        assertTrue(strategy.assertNoRunningConnections());
                    });
                }
            }
        }
    }

    public void testConnectWithSingleIncompatibleNode() {
        Version incompatibleVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        try (MockTransportService transport1 = startTransport("compatible-node", Version.CURRENT);
             MockTransportService transport2 = startTransport("incompatible-node", incompatibleVersion)) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();
            TransportAddress address2 = transport2.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                StubbableTransport stubbableTransport = new StubbableTransport(localService.transport);
                ConnectionManager connectionManager = new ConnectionManager(profile, stubbableTransport);
                AtomicInteger address1Attempts = new AtomicInteger(0);
                AtomicInteger address2Attempts = new AtomicInteger(0);
                stubbableTransport.setDefaultConnectBehavior((transport, discoveryNode, profile, listener) -> {
                    if (discoveryNode.getAddress().equals(address1)) {
                        address1Attempts.incrementAndGet();
                        transport.openConnection(discoveryNode, profile, listener);
                    } else if (discoveryNode.getAddress().equals(address2)) {
                        address2Attempts.incrementAndGet();
                        transport.openConnection(discoveryNode, profile, listener);
                    } else {
                        throw new AssertionError("Unexpected address");
                    }
                });
                int numOfConnections = 5;
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SimpleConnectionStrategy strategy = new SimpleConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         numOfConnections, addresses(address1, address2))) {
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    assertEquals(4 ,connectionManager.size());
                    assertEquals(4 ,connectionManager.getAllConnectedNodes().stream().map(n -> n.getAddress().equals(address1)).count());
                    // Three attempts on first round, one attempts on second round, zero attempts on third round
                    assertEquals(4, address1Attempts.get());
                    // Two attempts on first round, one attempt on second round, one attempt on third round
                    assertEquals(4, address2Attempts.get());
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));
                    assertTrue(strategy.assertNoRunningConnections());
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

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                int numOfConnections = randomIntBetween(4, 8);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SimpleConnectionStrategy strategy = new SimpleConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         numOfConnections, addresses(address1))) {

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    expectThrows(Exception.class, connectFuture::actionGet);

                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertEquals(0, connectionManager.size());
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testClusterNameValidationPreventConnectingToDifferentClusters() throws Exception {
        Settings otherSettings = Settings.builder().put("cluster.name", "otherCluster").build();

        try (MockTransportService transport1 = startTransport("cluster1", Version.CURRENT);
             MockTransportService transport2 = startTransport("cluster2", Version.CURRENT, otherSettings)) {
            TransportAddress address1 = transport1.boundAddress().publishAddress();
            TransportAddress address2 = transport2.boundAddress().publishAddress();

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                int numOfConnections = randomIntBetween(4, 8);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SimpleConnectionStrategy strategy = new SimpleConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         numOfConnections, addresses(address1, address2))) {
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1)));
                    assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));

                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    if (connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address1))) {
                        assertFalse(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));
                    } else {
                        assertTrue(connectionManager.getAllConnectedNodes().stream().anyMatch(n -> n.getAddress().equals(address2)));
                    }
                    assertTrue(strategy.assertNoRunningConnections());
                }
            }
        }
    }

    public void testSimpleStrategyWillResolveAddressesEachConnect() throws Exception {
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

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                int numOfConnections = randomIntBetween(4, 8);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SimpleConnectionStrategy strategy = new SimpleConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         numOfConnections,  addresses(address), Collections.singletonList(addressSupplier))) {
                    PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                    strategy.connect(connectFuture);
                    connectFuture.actionGet();

                    remoteConnectionManager.getAnyRemoteConnection().close();

                    assertTrue(multipleResolveLatch.await(30L, TimeUnit.SECONDS));
                }
            }
        }
    }

    private static List<String> addresses(final TransportAddress... addresses) {
        return Arrays.stream(addresses).map(TransportAddress::toString).collect(Collectors.toList());
    }
}
