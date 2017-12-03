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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class RemoteClusterServiceTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes, Version version) {
        return startTransport(id, knownNodes, version, Settings.EMPTY);
    }

    private MockTransportService startTransport(
            final String id,
            final List<DiscoveryNode> knownNodes,
            final Version version,
            final Settings settings) {
        return RemoteClusterConnectionTests.startTransport(
                id, knownNodes, version, threadPool, settings);
    }

    public void testSettingsAreRegistered() {
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterAware.REMOTE_CLUSTERS_SEEDS));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_CONNECTIONS_PER_CLUSTER));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING));
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(RemoteClusterService.REMOTE_NODE_ATTRIBUTE));
    }

    public void testRemoteClusterSeedSetting() {
        // simple validation
        Settings settings = Settings.builder()
            .put("search.remote.foo.seeds", "192.168.0.1:8080")
            .put("search.remote.bar.seed", "[::1]:9090").build();
        RemoteClusterAware.REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));

        Settings brokenSettings = Settings.builder()
            .put("search.remote.foo.seeds", "192.168.0.1").build();
        expectThrows(IllegalArgumentException.class, () ->
        RemoteClusterAware.REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(brokenSettings).forEach(setting -> setting.get(brokenSettings)));
    }

    public void testBuiltRemoteClustersSeeds() throws Exception {
        Map<String, List<DiscoveryNode>> map = RemoteClusterService.buildRemoteClustersSeeds(
            Settings.builder().put("search.remote.foo.seeds", "192.168.0.1:8080").put("search.remote.bar.seeds", "[::1]:9090").build());
        assertEquals(2, map.size());
        assertTrue(map.containsKey("foo"));
        assertTrue(map.containsKey("bar"));
        assertEquals(1, map.get("foo").size());
        assertEquals(1, map.get("bar").size());

        DiscoveryNode foo = map.get("foo").get(0);

        assertEquals(foo.getAddress(), new TransportAddress(new InetSocketAddress(InetAddress.getByName("192.168.0.1"), 8080)));
        assertEquals(foo.getId(), "foo#192.168.0.1:8080");
        assertEquals(foo.getVersion(), Version.CURRENT.minimumCompatibilityVersion());

        DiscoveryNode bar = map.get("bar").get(0);
        assertEquals(bar.getAddress(), new TransportAddress(new InetSocketAddress(InetAddress.getByName("[::1]"), 9090)));
        assertEquals(bar.getId(), "bar#[::1]:9090");
        assertEquals(bar.getVersion(), Version.CURRENT.minimumCompatibilityVersion());
    }


    public void testGroupClusterIndices() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
             MockTransportService otherSeedTransport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode otherSeedNode = otherSeedTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(otherSeedTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool,
                null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("search.remote.cluster_1.seeds", seedNode.getAddress().toString());
                builder.putList("search.remote.cluster_2.seeds", otherSeedNode.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(builder.build(), transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    assertFalse(service.isRemoteClusterRegistered("foo"));
                    Map<String, List<String>> perClusterIndices = service.groupClusterIndices(new String[]{"foo:bar", "cluster_1:bar",
                        "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo", "cluster*:baz", "*:boo", "no*match:boo"},
                        i -> false);
                    String[] localIndices = perClusterIndices.computeIfAbsent(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                        k -> Collections.emptyList()).toArray(new String[0]);
                    assertNotNull(perClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));
                    assertArrayEquals(new String[]{"foo:bar", "foo", "no*match:boo"}, localIndices);
                    assertEquals(2, perClusterIndices.size());
                    assertEquals(Arrays.asList("bar", "test", "baz", "boo"), perClusterIndices.get("cluster_1"));
                    assertEquals(Arrays.asList("foo:bar", "foo*", "baz", "boo"), perClusterIndices.get("cluster_2"));

                    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
                        service.groupClusterIndices(new String[]{"foo:bar", "cluster_1:bar",
                            "cluster_2:foo:bar", "cluster_1:test", "cluster_2:foo*", "foo"}, "cluster_1:bar"::equals));

                    assertEquals("Can not filter indices; index cluster_1:bar exists but there is also a remote cluster named:" +
                            " cluster_1", iae.getMessage());
                }
            }
        }
    }

    public void testIncrementallyAddClusters() throws IOException {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("cluster_1_node", knownNodes, Version.CURRENT);
             MockTransportService otherSeedTransport = startTransport("cluster_2_node", knownNodes, Version.CURRENT)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            DiscoveryNode otherSeedNode = otherSeedTransport.getLocalDiscoNode();
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(otherSeedTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool,
                null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                Settings.Builder builder = Settings.builder();
                builder.putList("search.remote.cluster_1.seeds", seedNode.getAddress().toString());
                builder.putList("search.remote.cluster_2.seeds", otherSeedNode.getAddress().toString());
                try (RemoteClusterService service = new RemoteClusterService(Settings.EMPTY, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.updateRemoteCluster("cluster_1", Collections.singletonList(seedNode.getAddress().address()));
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    service.updateRemoteCluster("cluster_2", Collections.singletonList(otherSeedNode.getAddress().address()));
                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    service.updateRemoteCluster("cluster_2", Collections.emptyList());
                    assertFalse(service.isRemoteClusterRegistered("cluster_2"));
                    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                        () -> service.updateRemoteCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, Collections.emptyList()));
                    assertEquals("remote clusters must not have the empty string as its key", iae.getMessage());
                }
            }
        }
    }

    public void testRemoteNodeAttribute() throws IOException, InterruptedException {
        final Settings settings =
                Settings.builder().put("search.remote.node.attr", "gateway").build();
        final List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        final Settings gateway = Settings.builder().put("node.attr.gateway", true).build();
        try (MockTransportService c1N1 =
                     startTransport("cluster_1_node_1", knownNodes, Version.CURRENT);
             MockTransportService c1N2 =
                     startTransport("cluster_1_node_2", knownNodes, Version.CURRENT, gateway);
             MockTransportService c2N1 =
                     startTransport("cluster_2_node_1", knownNodes, Version.CURRENT);
             MockTransportService c2N2 =
                     startTransport("cluster_2_node_2", knownNodes, Version.CURRENT, gateway)) {
            final DiscoveryNode c1N1Node = c1N1.getLocalDiscoNode();
            final DiscoveryNode c1N2Node = c1N2.getLocalDiscoNode();
            final DiscoveryNode c2N1Node = c2N1.getLocalDiscoNode();
            final DiscoveryNode c2N2Node = c2N2.getLocalDiscoNode();
            knownNodes.add(c1N1Node);
            knownNodes.add(c1N2Node);
            knownNodes.add(c2N1Node);
            knownNodes.add(c2N2Node);
            Collections.shuffle(knownNodes, random());

            try (MockTransportService transportService = MockTransportService.createNewService(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                final Settings.Builder builder = Settings.builder();
                builder.putList(
                        "search.remote.cluster_1.seeds", c1N1Node.getAddress().toString());
                builder.putList(
                        "search.remote.cluster_2.seeds", c2N1Node.getAddress().toString());
                try (RemoteClusterService service =
                             new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());

                    final InetSocketAddress c1N1Address = c1N1Node.getAddress().address();
                    final InetSocketAddress c1N2Address = c1N2Node.getAddress().address();
                    final InetSocketAddress c2N1Address = c2N1Node.getAddress().address();
                    final InetSocketAddress c2N2Address = c2N2Node.getAddress().address();

                    final CountDownLatch firstLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                            "cluster_1",
                            Arrays.asList(c1N1Address, c1N2Address),
                            connectionListener(firstLatch));
                    firstLatch.await();

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                            "cluster_2",
                            Arrays.asList(c2N1Address, c2N2Address),
                            connectionListener(secondLatch));
                    secondLatch.await();

                    assertTrue(service.isCrossClusterSearchEnabled());
                    assertTrue(service.isRemoteClusterRegistered("cluster_1"));
                    assertFalse(service.isRemoteNodeConnected("cluster_1", c1N1Node));
                    assertTrue(service.isRemoteNodeConnected("cluster_1", c1N2Node));
                    assertTrue(service.isRemoteClusterRegistered("cluster_2"));
                    assertFalse(service.isRemoteNodeConnected("cluster_2", c2N1Node));
                    assertTrue(service.isRemoteNodeConnected("cluster_2", c2N2Node));
                }
            }
        }
    }

    private ActionListener<Void> connectionListener(final CountDownLatch latch) {
        return ActionListener.wrap(x -> latch.countDown(), x -> fail());
    }

    public void testCollectNodes() throws InterruptedException, IOException {
        final Settings settings = Settings.EMPTY;
        final List<DiscoveryNode> knownNodes_c1 = new CopyOnWriteArrayList<>();
        final List<DiscoveryNode> knownNodes_c2 = new CopyOnWriteArrayList<>();

        try (MockTransportService c1N1 =
                 startTransport("cluster_1_node_1", knownNodes_c1, Version.CURRENT, settings);
             MockTransportService c1N2 =
                 startTransport("cluster_1_node_2", knownNodes_c1, Version.CURRENT, settings);
             MockTransportService c2N1 =
                 startTransport("cluster_2_node_1", knownNodes_c2, Version.CURRENT, settings);
             MockTransportService c2N2 =
                 startTransport("cluster_2_node_2", knownNodes_c2, Version.CURRENT, settings)) {
            final DiscoveryNode c1N1Node = c1N1.getLocalDiscoNode();
            final DiscoveryNode c1N2Node = c1N2.getLocalDiscoNode();
            final DiscoveryNode c2N1Node = c2N1.getLocalDiscoNode();
            final DiscoveryNode c2N2Node = c2N2.getLocalDiscoNode();
            knownNodes_c1.add(c1N1Node);
            knownNodes_c1.add(c1N2Node);
            knownNodes_c2.add(c2N1Node);
            knownNodes_c2.add(c2N2Node);
            Collections.shuffle(knownNodes_c1, random());
            Collections.shuffle(knownNodes_c2, random());

            try (MockTransportService transportService = MockTransportService.createNewService(
                settings,
                Version.CURRENT,
                threadPool,
                null)) {
                transportService.start();
                transportService.acceptIncomingRequests();
                final Settings.Builder builder = Settings.builder();
                builder.putList(
                    "search.remote.cluster_1.seeds", c1N1Node.getAddress().toString());
                builder.putList(
                    "search.remote.cluster_2.seeds", c2N1Node.getAddress().toString());
                try (RemoteClusterService service =
                         new RemoteClusterService(settings, transportService)) {
                    assertFalse(service.isCrossClusterSearchEnabled());
                    service.initializeRemoteClusters();
                    assertFalse(service.isCrossClusterSearchEnabled());

                    final InetSocketAddress c1N1Address = c1N1Node.getAddress().address();
                    final InetSocketAddress c1N2Address = c1N2Node.getAddress().address();
                    final InetSocketAddress c2N1Address = c2N1Node.getAddress().address();
                    final InetSocketAddress c2N2Address = c2N2Node.getAddress().address();

                    final CountDownLatch firstLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_1",
                        Arrays.asList(c1N1Address, c1N2Address),
                        connectionListener(firstLatch));
                    firstLatch.await();

                    final CountDownLatch secondLatch = new CountDownLatch(1);
                    service.updateRemoteCluster(
                        "cluster_2",
                        Arrays.asList(c2N1Address, c2N2Address),
                        connectionListener(secondLatch));
                    secondLatch.await();
                    CountDownLatch latch = new CountDownLatch(1);
                    service.collectNodes(new HashSet<>(Arrays.asList("cluster_1", "cluster_2")),
                        new ActionListener<BiFunction<String, String, DiscoveryNode>>() {
                        @Override
                        public void onResponse(BiFunction<String, String, DiscoveryNode> func) {
                            try {
                                assertEquals(c1N1Node, func.apply("cluster_1", c1N1Node.getId()));
                                assertEquals(c1N2Node, func.apply("cluster_1", c1N2Node.getId()));
                                assertEquals(c2N1Node, func.apply("cluster_2", c2N1Node.getId()));
                                assertEquals(c2N2Node, func.apply("cluster_2", c2N2Node.getId()));
                            } finally {
                                latch.countDown();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                throw new AssertionError(e);
                            } finally {
                                latch.countDown();
                            }
                        }
                    });
                    latch.await();
                    {
                        CountDownLatch failLatch = new CountDownLatch(1);
                        AtomicReference<Exception> ex = new AtomicReference<>();
                        service.collectNodes(new HashSet<>(Arrays.asList("cluster_1", "cluster_2", "no such cluster")),
                            new ActionListener<BiFunction<String, String, DiscoveryNode>>() {
                                @Override
                                public void onResponse(BiFunction<String, String, DiscoveryNode> stringStringDiscoveryNodeBiFunction) {
                                    try {
                                        fail("should not be called");
                                    } finally {
                                        failLatch.countDown();
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    try {
                                        ex.set(e);
                                    } finally {
                                        failLatch.countDown();
                                    }
                                }
                            });
                        failLatch.await();
                        assertNotNull(ex.get());
                        assertTrue(ex.get() instanceof IllegalArgumentException);
                        assertEquals("no such remote cluster: [no such cluster]", ex.get().getMessage());
                    }
                    {

                        logger.info("closing all source nodes");
                        // close all targets and check for the transport level failure path
                        IOUtils.close(c1N1, c1N2, c2N1, c2N2);
                        logger.info("all source nodes are closed");
                        CountDownLatch failLatch = new CountDownLatch(1);
                        AtomicReference<Exception> ex = new AtomicReference<>();
                        service.collectNodes(new HashSet<>(Arrays.asList("cluster_1", "cluster_2")),
                            new ActionListener<BiFunction<String, String, DiscoveryNode>>() {
                                @Override
                                public void onResponse(BiFunction<String, String, DiscoveryNode> stringStringDiscoveryNodeBiFunction) {
                                    try {
                                        fail("should not be called");
                                    } finally {
                                        failLatch.countDown();
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    try {
                                        ex.set(e);
                                    } finally {
                                        failLatch.countDown();
                                    }
                                }
                            });
                        failLatch.await();
                        assertNotNull(ex.get());
                        if (ex.get() instanceof  IllegalStateException) {
                            assertThat(ex.get().getMessage(), anyOf(equalTo("no seed node left"), startsWith
                                ("No node available for cluster:")));
                        } else {
                            if (ex.get() instanceof TransportException == false) {
                                // we have an issue for this see #25301
                                logger.error("expected TransportException but got a different one see #25301", ex.get());
                            }
                            assertTrue("expected TransportException but got a different one [" + ex.get().getClass().toString() + "]",
                                ex.get() instanceof TransportException);
                        }
                    }
                }
            }
        }
    }

    public void testCollectSearchShards() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        MockTransportService[] mockTransportServices = new MockTransportService[numClusters];
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        for (int i = 0; i < numClusters; i++) {
            List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
            MockTransportService remoteSeedTransport = startTransport("node_remote" + i, knownNodes, Version.CURRENT);
            mockTransportServices[i] = remoteSeedTransport;
            DiscoveryNode remoteSeedNode = remoteSeedTransport.getLocalDiscoNode();
            knownNodes.add(remoteSeedNode);
            nodes[i] = remoteSeedNode;
            builder.put("search.remote.remote" + i + ".seeds", remoteSeedNode.getAddress().toString());
            remoteIndicesByCluster.put("remote" + i, new OriginalIndices(new String[]{"index"}, IndicesOptions.lenientExpandOpen()));
        }
        Settings settings = builder.build();

        try {
            try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                try (RemoteClusterService remoteClusterService = new RemoteClusterService(settings, service)) {
                    assertFalse(remoteClusterService.isCrossClusterSearchEnabled());
                    remoteClusterService.initializeRemoteClusters();
                    assertTrue(remoteClusterService.isCrossClusterSearchEnabled());
                    {
                        final CountDownLatch latch = new CountDownLatch(1);
                        AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                        AtomicReference<Exception> failure = new AtomicReference<>();
                        remoteClusterService.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, remoteIndicesByCluster,
                                new LatchedActionListener<>(ActionListener.wrap(response::set, failure::set), latch));
                        assertTrue(latch.await(1, TimeUnit.SECONDS));
                        assertNull(failure.get());
                        assertNotNull(response.get());
                        Map<String, ClusterSearchShardsResponse> map = response.get();
                        assertEquals(numClusters, map.size());
                        for (int i = 0; i < numClusters; i++) {
                            String clusterAlias = "remote" + i;
                            assertTrue(map.containsKey(clusterAlias));
                            ClusterSearchShardsResponse shardsResponse = map.get(clusterAlias);
                            assertEquals(1, shardsResponse.getNodes().length);
                        }
                    }

                    int numDisconnectedClusters = randomIntBetween(1, numClusters);
                    Set<DiscoveryNode> disconnectedNodes = new HashSet<>(numDisconnectedClusters);
                    Set<Integer> disconnectedNodesIndices = new HashSet<>(numDisconnectedClusters);
                    while(disconnectedNodes.size() < numDisconnectedClusters) {
                        int i = randomIntBetween(0, numClusters - 1);
                        if (disconnectedNodes.add(nodes[i])) {
                            assertTrue(disconnectedNodesIndices.add(i));
                        }
                    }

                    CountDownLatch disconnectedLatch = new CountDownLatch(numDisconnectedClusters);
                    service.addConnectionListener(new TransportConnectionListener() {
                        @Override
                        public void onNodeDisconnected(DiscoveryNode node) {
                            if (disconnectedNodes.remove(node)) {
                                disconnectedLatch.countDown();
                            }
                        }
                    });

                    for (DiscoveryNode disconnectedNode : disconnectedNodes) {
                        service.addFailToSendNoConnectRule(disconnectedNode.getAddress());
                    }

                    {
                        final CountDownLatch latch = new CountDownLatch(1);
                        AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                        AtomicReference<Exception> failure = new AtomicReference<>();
                        remoteClusterService.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, remoteIndicesByCluster,
                                new LatchedActionListener<>(ActionListener.wrap(response::set, failure::set), latch));
                        assertTrue(latch.await(1, TimeUnit.SECONDS));
                        assertNull(response.get());
                        assertNotNull(failure.get());
                        assertThat(failure.get(), instanceOf(TransportException.class));
                        assertThat(failure.get().getMessage(), containsString("unable to communicate with remote cluster"));
                    }

                    //setting skip_unavailable to true for all the disconnected clusters will make the request succeed again
                    for (int i : disconnectedNodesIndices) {
                        remoteClusterService.updateSkipUnavailable("remote" + i, true);
                    }
                    {
                        final CountDownLatch latch = new CountDownLatch(1);
                        AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                        AtomicReference<Exception> failure = new AtomicReference<>();
                        remoteClusterService.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, remoteIndicesByCluster,
                                new LatchedActionListener<>(ActionListener.wrap(response::set, failure::set), latch));
                        assertTrue(latch.await(1, TimeUnit.SECONDS));
                        assertNull(failure.get());
                        assertNotNull(response.get());
                        Map<String, ClusterSearchShardsResponse> map = response.get();
                        assertEquals(numClusters, map.size());
                        for (int i = 0; i < numClusters; i++) {
                            String clusterAlias = "remote" + i;
                            assertTrue(map.containsKey(clusterAlias));
                            ClusterSearchShardsResponse shardsResponse = map.get(clusterAlias);
                            if (disconnectedNodesIndices.contains(i)) {
                                assertTrue(shardsResponse == ClusterSearchShardsResponse.EMPTY);
                            } else {
                                assertTrue(shardsResponse != ClusterSearchShardsResponse.EMPTY);
                            }
                        }
                    }

                    //give transport service enough time to realize that the node is down, and to notify the connection listeners
                    //so that RemoteClusterConnection is left with no connected nodes, hence it will retry connecting next
                    assertTrue(disconnectedLatch.await(1, TimeUnit.SECONDS));

                    service.clearAllRules();
                    if (randomBoolean()) {
                        for (int i : disconnectedNodesIndices) {
                            if (randomBoolean()) {
                                remoteClusterService.updateSkipUnavailable("remote" + i, true);
                            }

                        }
                    }
                    {
                        final CountDownLatch latch = new CountDownLatch(1);
                        AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                        AtomicReference<Exception> failure = new AtomicReference<>();
                        remoteClusterService.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, remoteIndicesByCluster,
                                new LatchedActionListener<>(ActionListener.wrap(response::set, failure::set), latch));
                        assertTrue(latch.await(1, TimeUnit.SECONDS));
                        assertNull(failure.get());
                        assertNotNull(response.get());
                        Map<String, ClusterSearchShardsResponse> map = response.get();
                        assertEquals(numClusters, map.size());
                        for (int i = 0; i < numClusters; i++) {
                            String clusterAlias = "remote" + i;
                            assertTrue(map.containsKey(clusterAlias));
                            ClusterSearchShardsResponse shardsResponse = map.get(clusterAlias);
                            assertTrue(shardsResponse != ClusterSearchShardsResponse.EMPTY);
                        }
                    }
                }
            }
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testRemoteClusterSkipIfDisconnectedSetting() {
        {
            Settings settings = Settings.builder()
                    .put("search.remote.foo.skip_unavailable", true)
                    .put("search.remote.bar.skip_unavailable", false).build();
            RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(settings).forEach(setting -> setting.get(settings));
        }
        {
            Settings brokenSettings = Settings.builder()
                    .put("search.remote.foo.skip_unavailable", "broken").build();
            expectThrows(IllegalArgumentException.class, () ->
                    RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(brokenSettings)
                            .forEach(setting -> setting.get(brokenSettings)));
        }

        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,
                new HashSet<>(Arrays.asList(RemoteClusterAware.REMOTE_CLUSTERS_SEEDS,
                        RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE)));
        {
            Settings settings = Settings.builder().put("search.remote.foo.skip_unavailable", randomBoolean()).build();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> service.validate(settings, true));
            assertEquals("Missing required setting [search.remote.foo.seeds] for setting [search.remote.foo.skip_unavailable]",
                    iae.getMessage());
        }
        {
            try (MockTransportService remoteSeedTransport = startTransport("seed", new CopyOnWriteArrayList<>(), Version.CURRENT)) {
                String seed = remoteSeedTransport.getLocalDiscoNode().getAddress().toString();
                service.validate(Settings.builder().put("search.remote.foo.skip_unavailable", randomBoolean())
                        .put("search.remote.foo.seeds", seed).build(), true);
                service.validate(Settings.builder().put("search.remote.foo.seeds", seed).build(), true);

                AbstractScopedSettings service2 = new ClusterSettings(Settings.builder().put("search.remote.foo.seeds", seed).build(),
                        new HashSet<>(Arrays.asList(RemoteClusterAware.REMOTE_CLUSTERS_SEEDS,
                                RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE)));
                service2.validate(Settings.builder().put("search.remote.foo.skip_unavailable", randomBoolean()).build(), false);
            }
        }
    }
}
