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
package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteClusterConnectionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes, Version version) {
        boolean success = false;
        MockTransportService newService = MockTransportService.createNewService(Settings.EMPTY, version, threadPool, null);
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
                    ClusterState build = ClusterState.builder(ClusterName.DEFAULT).nodes(builder.build()).build();
                    channel.sendResponse(new ClusterStateResponse(ClusterName.DEFAULT, build));
                });
            newService.start();
            newService.setLocalNode(new DiscoveryNode(id, newService.boundAddress().publishAddress(), version));
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
                    assertFalse(connection.isConnectRunning());
                }
            }
        }
    }

    public void testDiscoverSingleNodeWithIncompatibleSeed() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes, Version.CURRENT);
             MockTransportService incomaptibleTransport = startTransport("incompat_seed_node", knownNodes, Version.fromString("2.0.0"));
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            DiscoveryNode incompatibleSeedNode = incomaptibleTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            knownNodes.add(incomaptibleTransport.getLocalDiscoNode());
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
                    assertFalse(connection.isConnectRunning());
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
                    assertFalse(connection.isConnectRunning());
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
                    assertFalse(connection.isConnectRunning());
                }
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
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    CountDownLatch latch = new CountDownLatch(1);
                    String newNode = null;
                    if (randomBoolean()) {
                        updateSeedNodes(connection, Arrays.asList(seedNode));
                    }

                    SearchRequest request = new SearchRequest("test-index");
                    CountDownLatch responseLatch = new CountDownLatch(1);
                    AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                    AtomicReference<Exception> failReference = new AtomicReference<>();

                    ActionListener<ClusterSearchShardsResponse> shardsListener = ActionListener.wrap(
                        x -> {
                            reference.set(x);
                            responseLatch.countDown();
                        },
                        x -> {
                            failReference.set(x);
                            responseLatch.countDown();
                        });
                    connection.fetchSearchShards(request, Arrays.asList("test-index"), shardsListener);
                    responseLatch.await();
                    assertNull(failReference.get());
                    assertNotNull(reference.get());
                    ClusterSearchShardsResponse clusterSearchShardsResponse = reference.get();
                    DiscoveryNode[] nodes = clusterSearchShardsResponse.getNodes();
                    assertTrue(nodes.length != 0);
                    for (DiscoveryNode dataNode : nodes) {
                        assertNotNull(connection.getConnection(dataNode));
                        if (dataNode.getName().equals(newNode)) {
                            assertFalse(service.nodeConnected(dataNode));
                        } else {
                            assertTrue(service.nodeConnected(dataNode));
                        }
                    }
                    assertFalse(connection.isConnectRunning());
                }
            }
        }
    }
}
