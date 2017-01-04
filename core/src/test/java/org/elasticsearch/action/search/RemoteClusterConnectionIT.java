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
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteClusterConnectionIT extends ESIntegTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes) {
        boolean success = false;
        MockTransportService newService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null);
        try {
            newService.registerRequestHandler(ClusterSearchShardsAction.NAME, ClusterSearchShardsRequest::new, ThreadPool.Names.SAME,
                (request, channel) -> {
                    channel.sendResponse(new ClusterSearchShardsResponse());
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
            newService.setLocalNode(new DiscoveryNode(id, newService.boundAddress().publishAddress(), Version.CURRENT));
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
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());

            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                    Arrays.asList(seedNode), service, Integer.MAX_VALUE, n -> true)) {
                    updateSeedNodes(connection, Arrays.asList(seedNode));
                    assertTrue(service.nodeConnected(seedNode));
                    assertTrue(service.nodeConnected(discoverableNode));
                }
            }
        }
    }

    public void testNodeDisconnected() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes);
             MockTransportService spareTransport = startTransport("spare_node", knownNodes)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            DiscoveryNode spareNode = spareTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());

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
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode discoverableNode = discoverableTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            DiscoveryNode rejectedNode = randomBoolean() ? seedNode : discoverableNode;

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


    public void testConnect() throws InterruptedException, IOException {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setNodes(true).get();
        ImmutableOpenMap<String, DiscoveryNode> nodes = clusterStateResponse.getState().getNodes().getDataNodes();
        DiscoveryNode node = nodes.valuesIt().next();
        try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster", Arrays.asList(node),
                service, Integer.MAX_VALUE, n -> true)) {
                CountDownLatch latch = new CountDownLatch(1);
                AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
                ActionListener<Void> listener = ActionListener.wrap(x -> latch.countDown(), x -> {
                    exceptionAtomicReference.set(x);
                    latch.countDown();
                });
                connection.updateSeedNodes(Arrays.asList(node), listener);
                latch.await();
                assertTrue(service.nodeConnected(node));
                Iterable<DiscoveryNode> nodesIterable = nodes::valuesIt;
                for (DiscoveryNode dataNode : nodesIterable) {
                    assertTrue(service.nodeConnected(dataNode));
                }
                assertNull(exceptionAtomicReference.get());
            }
        }
    }

    public void testConnectToSingleSeed() throws InterruptedException, IOException {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setNodes(true).get();
        ImmutableOpenMap<String, DiscoveryNode> nodes = clusterStateResponse.getState().getNodes().getNodes();
        DiscoveryNode node = nodes.valuesIt().next();
        try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster", Arrays.asList(node),
                service, 1, n -> true)) {
                CountDownLatch latch = new CountDownLatch(1);
                AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
                ActionListener<Void> listener = ActionListener.wrap(x -> latch.countDown(), x -> {
                    exceptionAtomicReference.set(x);
                    latch.countDown();
                });
                connection.updateSeedNodes(Arrays.asList(node), listener);
                latch.await();
                assertTrue(service.nodeConnected(node));
                Iterable<DiscoveryNode> nodesIterable = nodes::valuesIt;
                for (DiscoveryNode aNode : nodesIterable) {
                    if (aNode.equals(node)) {
                        assertTrue(service.nodeConnected(aNode));
                    } else {
                        assertFalse(service.nodeConnected(aNode));
                    }
                }
                assertNull(exceptionAtomicReference.get());
            }
        }
    }

    public void testFetchShards() throws Exception {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setNodes(true).get();
        ImmutableOpenMap<String, DiscoveryNode> clusterNodes = clusterStateResponse.getState().getNodes().getNodes();
        DiscoveryNode node = clusterNodes.valuesIt().next();
        try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            final boolean hasInitialNodes = randomBoolean();
            try (RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                hasInitialNodes ? Arrays.asList(node) : Collections.emptyList(), service, Integer.MAX_VALUE, n -> true)) {
                CountDownLatch latch = new CountDownLatch(1);
                String newNode = null;
                if (hasInitialNodes == false) {
                    AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
                    ActionListener<Void> listener = ActionListener.wrap(x -> latch.countDown(), x -> {
                        exceptionAtomicReference.set(x);
                        latch.countDown();
                    });
                    connection.updateSeedNodes(Arrays.asList(node), listener);
                    latch.await();

                    newNode = internalCluster().startDataOnlyNode();
                    createIndex("test-index");
                    assertTrue(service.nodeConnected(node));
                    Iterable<DiscoveryNode> nodesIterable = clusterNodes::valuesIt;
                    for (DiscoveryNode dataNode : nodesIterable) {
                        if (dataNode.getName().equals(newNode)) {
                            assertFalse(service.nodeConnected(dataNode));
                        } else {
                            assertTrue(service.nodeConnected(dataNode));
                        }
                    }
                    assertNull(exceptionAtomicReference.get());
                } else {
                    createIndex("test-index");
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
            }
        }
    }
}
