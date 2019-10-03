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
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Tuple;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

public class SniffConnectionStrategyTests extends ESTestCase {

    private final String clusterAlias = "cluster-alias";
    private final ConnectionProfile profile = RemoteClusterService.buildConnectionProfileFromSettings(Settings.EMPTY, "cluster");
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes, Version version) {
        return startTransport(id, knownNodes, version, Settings.EMPTY);
    }

    public MockTransportService startTransport(final String id, final List<DiscoveryNode> knownNodes, final Version version,
                                               final Settings settings) {
        boolean success = false;
        final Settings s = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterAlias)
            .put("node.name", id)
            .put(settings)
            .build();
        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(s);
        MockTransportService newService = MockTransportService.createNewService(s, version, threadPool);
        try {
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

    public void testSniffStrategyWillConnectToAndDiscoverNodes() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalNode();
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());


            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SniffConnectionStrategy strategy = new SniffConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         null, 3, n -> true, seedNodes(seedNode))) {
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

    public void testSniffStrategyWillConnectToMaxAllowedNodesAndOpenNewConnectionsOnDisconnect() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport1 = startTransport("discoverable_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport2 = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalNode();
            DiscoveryNode discoverableNode1 = discoverableTransport1.getLocalNode();
            DiscoveryNode discoverableNode2 = discoverableTransport2.getLocalNode();
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode1);
            knownNodes.add(discoverableNode2);
            Collections.shuffle(knownNodes, random());


            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SniffConnectionStrategy strategy = new SniffConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         null, 2, n -> true, seedNodes(seedNode))) {
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
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService incompatibleSeedTransport = startTransport("discoverable_node", knownNodes, incompatibleVersion);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalNode();
            DiscoveryNode incompatibleSeedNode = incompatibleSeedTransport.getLocalNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalNode();
            knownNodes.add(seedNode);
            knownNodes.add(incompatibleSeedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());


            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SniffConnectionStrategy strategy = new SniffConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         null, 3, n -> true, seedNodes(seedNode))) {
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
        try (MockTransportService incompatibleSeedTransport = startTransport("seed_node", knownNodes, incompatibleVersion)) {
            DiscoveryNode incompatibleSeedNode = incompatibleSeedTransport.getLocalNode();
            knownNodes.add(incompatibleSeedNode);

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SniffConnectionStrategy strategy = new SniffConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         null, 3, n -> true, seedNodes(incompatibleSeedNode))) {
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
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalNode();
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            DiscoveryNode rejectedNode = randomBoolean() ? seedNode : discoverableNode;
            Collections.shuffle(knownNodes, random());

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SniffConnectionStrategy strategy = new SniffConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         null, 3, n -> n.equals(rejectedNode) == false, seedNodes(seedNode))) {
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

    public void testClusterNameValidationPreventConnectingToDifferentClusters() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        List<DiscoveryNode> otherKnownNodes = new CopyOnWriteArrayList<>();

        Settings otherSettings = Settings.builder().put("cluster.name", "otherCluster").build();

        try (MockTransportService seed = startTransport("other_seed", knownNodes, Version.CURRENT);
             MockTransportService discoverable = startTransport("other_discoverable", knownNodes, Version.CURRENT);
             MockTransportService otherSeed = startTransport("other_seed", knownNodes, Version.CURRENT, otherSettings);
             MockTransportService otherDiscoverable = startTransport("other_discoverable", knownNodes, Version.CURRENT, otherSettings)) {
            DiscoveryNode seedNode = seed.getLocalNode();
            DiscoveryNode discoverableNode = discoverable.getLocalNode();
            DiscoveryNode otherSeedNode = otherSeed.getLocalNode();
            DiscoveryNode otherDiscoverableNode = otherDiscoverable.getLocalNode();
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());
            Collections.shuffle(otherKnownNodes, random());

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SniffConnectionStrategy strategy = new SniffConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         null, 3, n -> true, seedNodes(seedNode, otherSeedNode))) {
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
                    assertThat(ise.getMessage(), allOf(
                        startsWith("handshake with [{other_seed}"),
                        containsString(otherSeedNode.toString()),
                        endsWith(" failed: remote cluster name [otherCluster] " +
                            "does not match expected remote cluster name [" + clusterAlias + "]")));

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
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalNode();
            knownNodes.add(seedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
                localService.start();
                localService.acceptIncomingRequests();

                ConnectionManager connectionManager = new ConnectionManager(profile, localService.transport);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SniffConnectionStrategy strategy = new SniffConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         null, 3, n -> true, seedNodes(seedNode))) {
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
        try (MockTransportService accessible = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService unresponsive1 = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool);
             MockTransportService unresponsive2 = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
            // We start in order to get a valid address + port, but do not start accepting connections as we
            // will not actually connect to these transports
            unresponsive1.start();
            unresponsive2.start();
            DiscoveryNode accessibleNode = accessible.getLocalNode();
            DiscoveryNode discoverableNode = unresponsive2.getLocalNode();

            // Use the address for the node that will not respond
            DiscoveryNode unaddressableSeedNode = new DiscoveryNode(accessibleNode.getName(), accessibleNode.getId(),
                accessibleNode.getEphemeralId(), accessibleNode.getHostName(), accessibleNode.getHostAddress(),
                unresponsive1.getLocalNode().getAddress(), accessibleNode.getAttributes(), accessibleNode.getRoles(),
                accessibleNode.getVersion());

            knownNodes.add(unaddressableSeedNode);
            knownNodes.add(discoverableNode);
            Collections.shuffle(knownNodes, random());

            try (MockTransportService localService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool)) {
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

                Tuple<String, Supplier<DiscoveryNode>> tuple = Tuple.tuple(accessibleNode.toString(), () -> unaddressableSeedNode);
                List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes = Collections.singletonList(tuple);
                TransportAddress proxyAddress = accessibleNode.getAddress();
                ConnectionManager connectionManager = new ConnectionManager(profile, transport);
                try (RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager(clusterAlias, connectionManager);
                     SniffConnectionStrategy strategy = new SniffConnectionStrategy(clusterAlias, localService, remoteConnectionManager,
                         proxyAddress.toString(), 3, n -> true, seedNodes)) {
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

    private static List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes(final DiscoveryNode... seedNodes) {
        if (seedNodes.length == 0) {
            return Collections.emptyList();
        } else if (seedNodes.length == 1) {
            return Collections.singletonList(Tuple.tuple(seedNodes[0].toString(), () -> seedNodes[0]));
        } else {
            return Arrays.stream(seedNodes)
                .map(s -> Tuple.tuple(s.toString(), (Supplier<DiscoveryNode>) () -> s))
                .collect(Collectors.toList());
        }
    }
}
