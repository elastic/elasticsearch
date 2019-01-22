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
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.RemoteClusterConnectionTests;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteClusterServiceTests;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;

public class TransportSearchActionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testMergeShardsIterators() {
        List<ShardIterator> localShardIterators = new ArrayList<>();
        {
            ShardId shardId = new ShardId("local_index", "local_index_uuid", 0);
            ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, "local_node", true, STARTED);
            ShardIterator shardIterator = new PlainShardIterator(shardId, Collections.singletonList(shardRouting));
            localShardIterators.add(shardIterator);
        }
        {
            ShardId shardId2 = new ShardId("local_index_2", "local_index_2_uuid", 1);
            ShardRouting shardRouting2 = TestShardRouting.newShardRouting(shardId2, "local_node", true, STARTED);
            ShardIterator shardIterator2 = new PlainShardIterator(shardId2, Collections.singletonList(shardRouting2));
            localShardIterators.add(shardIterator2);
        }
        GroupShardsIterator<ShardIterator> localShardsIterator = new GroupShardsIterator<>(localShardIterators);

        OriginalIndices localIndices = new OriginalIndices(new String[]{"local_alias", "local_index_2"},
                SearchRequest.DEFAULT_INDICES_OPTIONS);

        OriginalIndices remoteIndices = new OriginalIndices(new String[]{"remote_alias", "remote_index_2"},
                IndicesOptions.strictExpandOpen());
        List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
        {
            ShardId remoteShardId = new ShardId("remote_index", "remote_index_uuid", 2);
            ShardRouting remoteShardRouting = TestShardRouting.newShardRouting(remoteShardId, "remote_node", true, STARTED);
            SearchShardIterator remoteShardIterator = new SearchShardIterator("remote", remoteShardId,
                    Collections.singletonList(remoteShardRouting), remoteIndices);
            remoteShardIterators.add(remoteShardIterator);
        }
        {
            ShardId remoteShardId2 = new ShardId("remote_index_2", "remote_index_2_uuid", 3);
            ShardRouting remoteShardRouting2 = TestShardRouting.newShardRouting(remoteShardId2, "remote_node", true, STARTED);
            SearchShardIterator remoteShardIterator2 = new SearchShardIterator("remote", remoteShardId2,
                    Collections.singletonList(remoteShardRouting2), remoteIndices);
            remoteShardIterators.add(remoteShardIterator2);
        }
        OriginalIndices remoteIndices2 = new OriginalIndices(new String[]{"remote_index_3"}, IndicesOptions.strictExpand());

        {
            ShardId remoteShardId3 = new ShardId("remote_index_3", "remote_index_3_uuid", 4);
            ShardRouting remoteShardRouting3 = TestShardRouting.newShardRouting(remoteShardId3, "remote_node", true, STARTED);
            SearchShardIterator remoteShardIterator3 = new SearchShardIterator("remote", remoteShardId3,
                    Collections.singletonList(remoteShardRouting3), remoteIndices2);
            remoteShardIterators.add(remoteShardIterator3);
        }

        String localClusterAlias = randomBoolean() ? null : "local";
        GroupShardsIterator<SearchShardIterator> searchShardIterators = TransportSearchAction.mergeShardsIterators(localShardsIterator,
                localIndices, localClusterAlias, remoteShardIterators);

        assertEquals(searchShardIterators.size(), 5);
        int i = 0;
        for (SearchShardIterator searchShardIterator : searchShardIterators) {
            switch(i++) {
                case 0:
                    assertEquals("local_index", searchShardIterator.shardId().getIndexName());
                    assertEquals(0, searchShardIterator.shardId().getId());
                    assertSame(localIndices, searchShardIterator.getOriginalIndices());
                    assertEquals(localClusterAlias, searchShardIterator.getClusterAlias());
                    break;
                case 1:
                    assertEquals("local_index_2", searchShardIterator.shardId().getIndexName());
                    assertEquals(1, searchShardIterator.shardId().getId());
                    assertSame(localIndices, searchShardIterator.getOriginalIndices());
                    assertEquals(localClusterAlias, searchShardIterator.getClusterAlias());
                    break;
                case 2:
                    assertEquals("remote_index", searchShardIterator.shardId().getIndexName());
                    assertEquals(2, searchShardIterator.shardId().getId());
                    assertSame(remoteIndices, searchShardIterator.getOriginalIndices());
                    assertEquals("remote", searchShardIterator.getClusterAlias());
                    break;
                case 3:
                    assertEquals("remote_index_2", searchShardIterator.shardId().getIndexName());
                    assertEquals(3, searchShardIterator.shardId().getId());
                    assertSame(remoteIndices, searchShardIterator.getOriginalIndices());
                    assertEquals("remote", searchShardIterator.getClusterAlias());
                    break;
                case 4:
                    assertEquals("remote_index_3", searchShardIterator.shardId().getIndexName());
                    assertEquals(4, searchShardIterator.shardId().getId());
                    assertSame(remoteIndices2, searchShardIterator.getOriginalIndices());
                    assertEquals("remote", searchShardIterator.getClusterAlias());
                    break;
            }
        }
    }

    public void testProcessRemoteShards() {
        try (TransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool,
            null)) {
            RemoteClusterService service = transportService.getRemoteClusterService();
            assertFalse(service.isCrossClusterSearchEnabled());
            List<SearchShardIterator> iteratorList = new ArrayList<>();
            Map<String, ClusterSearchShardsResponse> searchShardsResponseMap = new HashMap<>();
            DiscoveryNode[] nodes = new DiscoveryNode[] {
                new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT),
                new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT)
            };
            Map<String, AliasFilter> indicesAndAliases = new HashMap<>();
            indicesAndAliases.put("foo", new AliasFilter(new TermsQueryBuilder("foo", "bar"), "some_alias_for_foo",
                "some_other_foo_alias"));
            indicesAndAliases.put("bar", new AliasFilter(new MatchAllQueryBuilder(), Strings.EMPTY_ARRAY));
            ClusterSearchShardsGroup[] groups = new ClusterSearchShardsGroup[] {
                new ClusterSearchShardsGroup(new ShardId("foo", "foo_id", 0),
                    new ShardRouting[] {TestShardRouting.newShardRouting("foo", 0, "node1", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("foo", 0, "node2", false, ShardRoutingState.STARTED)}),
                new ClusterSearchShardsGroup(new ShardId("foo", "foo_id", 1),
                    new ShardRouting[] {TestShardRouting.newShardRouting("foo", 0, "node1", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("foo", 1, "node2", false, ShardRoutingState.STARTED)}),
                new ClusterSearchShardsGroup(new ShardId("bar", "bar_id", 0),
                    new ShardRouting[] {TestShardRouting.newShardRouting("bar", 0, "node2", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("bar", 0, "node1", false, ShardRoutingState.STARTED)})
            };
            searchShardsResponseMap.put("test_cluster_1", new ClusterSearchShardsResponse(groups, nodes, indicesAndAliases));
            DiscoveryNode[] nodes2 = new DiscoveryNode[] {
                new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT)
            };
            ClusterSearchShardsGroup[] groups2 = new ClusterSearchShardsGroup[] {
                new ClusterSearchShardsGroup(new ShardId("xyz", "xyz_id", 0),
                    new ShardRouting[] {TestShardRouting.newShardRouting("xyz", 0, "node3", true, ShardRoutingState.STARTED)})
            };
            Map<String, AliasFilter> filter = new HashMap<>();
            filter.put("xyz", new AliasFilter(null, "some_alias_for_xyz"));
            searchShardsResponseMap.put("test_cluster_2", new ClusterSearchShardsResponse(groups2, nodes2, filter));

            Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
            remoteIndicesByCluster.put("test_cluster_1",
                new OriginalIndices(new String[]{"fo*", "ba*"}, SearchRequest.DEFAULT_INDICES_OPTIONS));
            remoteIndicesByCluster.put("test_cluster_2",
                new OriginalIndices(new String[]{"x*"}, SearchRequest.DEFAULT_INDICES_OPTIONS));
            Map<String, AliasFilter> remoteAliases = new HashMap<>();
            TransportSearchAction.processRemoteShards(searchShardsResponseMap, remoteIndicesByCluster, iteratorList,
                remoteAliases);
            assertEquals(4, iteratorList.size());
            for (SearchShardIterator iterator : iteratorList) {
                if (iterator.shardId().getIndexName().endsWith("foo")) {
                    assertArrayEquals(new String[]{"some_alias_for_foo", "some_other_foo_alias"},
                        iterator.getOriginalIndices().indices());
                    assertTrue(iterator.shardId().getId() == 0 || iterator.shardId().getId() == 1);
                    assertEquals("test_cluster_1", iterator.getClusterAlias());
                    assertEquals("foo", iterator.shardId().getIndexName());
                    ShardRouting shardRouting = iterator.nextOrNull();
                    assertNotNull(shardRouting);
                    assertEquals(shardRouting.getIndexName(), "foo");
                    shardRouting = iterator.nextOrNull();
                    assertNotNull(shardRouting);
                    assertEquals(shardRouting.getIndexName(), "foo");
                    assertNull(iterator.nextOrNull());
                } else if (iterator.shardId().getIndexName().endsWith("bar")) {
                    assertArrayEquals(new String[]{"bar"}, iterator.getOriginalIndices().indices());
                    assertEquals(0, iterator.shardId().getId());
                    assertEquals("test_cluster_1", iterator.getClusterAlias());
                    assertEquals("bar", iterator.shardId().getIndexName());
                    ShardRouting shardRouting = iterator.nextOrNull();
                    assertNotNull(shardRouting);
                    assertEquals(shardRouting.getIndexName(), "bar");
                    shardRouting = iterator.nextOrNull();
                    assertNotNull(shardRouting);
                    assertEquals(shardRouting.getIndexName(), "bar");
                    assertNull(iterator.nextOrNull());
                } else if (iterator.shardId().getIndexName().endsWith("xyz")) {
                    assertArrayEquals(new String[]{"some_alias_for_xyz"}, iterator.getOriginalIndices().indices());
                    assertEquals(0, iterator.shardId().getId());
                    assertEquals("xyz", iterator.shardId().getIndexName());
                    assertEquals("test_cluster_2", iterator.getClusterAlias());
                    ShardRouting shardRouting = iterator.nextOrNull();
                    assertNotNull(shardRouting);
                    assertEquals(shardRouting.getIndexName(), "xyz");
                    assertNull(iterator.nextOrNull());
                }
            }
            assertEquals(3, remoteAliases.size());
            assertTrue(remoteAliases.toString(), remoteAliases.containsKey("foo_id"));
            assertTrue(remoteAliases.toString(), remoteAliases.containsKey("bar_id"));
            assertTrue(remoteAliases.toString(), remoteAliases.containsKey("xyz_id"));
            assertEquals(new TermsQueryBuilder("foo", "bar"), remoteAliases.get("foo_id").getQueryBuilder());
            assertEquals(new MatchAllQueryBuilder(), remoteAliases.get("bar_id").getQueryBuilder());
            assertNull(remoteAliases.get("xyz_id").getQueryBuilder());
        }
    }

    public void testBuildConnectionLookup() {
        Function<String, DiscoveryNode> localNodes = (nodeId) -> new DiscoveryNode("local-" + nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 1024), Version.CURRENT);
        BiFunction<String, String, DiscoveryNode> remoteNodes = (clusterAlias, nodeId) -> new DiscoveryNode("remote-" + nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 2048), Version.CURRENT);
        BiFunction<String, DiscoveryNode, Transport.Connection> nodeToConnection = (clusterAlias, node) -> new Transport.Connection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws TransportException {
            }

            @Override
            public void addCloseListener(ActionListener<Void> listener) {
            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {
            }
        };

        {
            BiFunction<String, String, Transport.Connection> connectionLookup = TransportSearchAction.buildConnectionLookup(
                null, localNodes, remoteNodes, nodeToConnection);

            Transport.Connection localConnection = connectionLookup.apply(null, randomAlphaOfLengthBetween(5, 10));
            assertThat(localConnection.getNode().getId(), startsWith("local-"));
            Transport.Connection remoteConnection = connectionLookup.apply(randomAlphaOfLengthBetween(5, 10),
                randomAlphaOfLengthBetween(5, 10));
            assertThat(remoteConnection.getNode().getId(), startsWith("remote-"));
        }
        {
            String requestClusterAlias = randomAlphaOfLengthBetween(5, 10);
            BiFunction<String, String, Transport.Connection> connectionLookup = TransportSearchAction.buildConnectionLookup(
                requestClusterAlias, localNodes, remoteNodes, nodeToConnection);

            Transport.Connection localConnection = connectionLookup.apply(requestClusterAlias, randomAlphaOfLengthBetween(5, 10));
            assertThat(localConnection.getNode().getId(), startsWith("local-"));
        }
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes) {
        return RemoteClusterConnectionTests.startTransport(id, knownNodes, Version.CURRENT, threadPool);
    }

    public void testCollectSearchShards() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        MockTransportService[] mockTransportServices = new MockTransportService[numClusters];
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        for (int i = 0; i < numClusters; i++) {
            List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
            MockTransportService remoteSeedTransport = startTransport("node_remote" + i, knownNodes);
            mockTransportServices[i] = remoteSeedTransport;
            DiscoveryNode remoteSeedNode = remoteSeedTransport.getLocalDiscoNode();
            knownNodes.add(remoteSeedNode);
            nodes[i] = remoteSeedNode;
            builder.put("cluster.remote.remote" + i + ".seeds", remoteSeedNode.getAddress().toString());
            remoteIndicesByCluster.put("remote" + i, new OriginalIndices(new String[]{"index"}, IndicesOptions.lenientExpandOpen()));
        }
        Settings settings = builder.build();

        try {
            try (MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();
                RemoteClusterService remoteClusterService = service.getRemoteClusterService();
                {
                    final CountDownLatch latch = new CountDownLatch(1);
                    AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                    AtomicInteger skippedClusters = new AtomicInteger();
                    TransportSearchAction.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, skippedClusters,
                        remoteIndicesByCluster, remoteClusterService, threadPool,
                        new LatchedActionListener<>(ActionListener.wrap(response::set, e -> fail("no failures expected")), latch));
                    awaitLatch(latch, 5, TimeUnit.SECONDS);
                    assertEquals(0, skippedClusters.get());
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
                {
                    final CountDownLatch latch = new CountDownLatch(1);
                    AtomicReference<Exception> failure = new AtomicReference<>();
                    AtomicInteger skippedClusters = new AtomicInteger(0);
                    TransportSearchAction.collectSearchShards(IndicesOptions.lenientExpandOpen(), "index_not_found", null, skippedClusters,
                        remoteIndicesByCluster, remoteClusterService, threadPool,
                        new LatchedActionListener<>(ActionListener.wrap(r -> fail("no response expected"), failure::set), latch));
                    awaitLatch(latch, 5, TimeUnit.SECONDS);
                    assertEquals(0, skippedClusters.get());
                    assertNotNull(failure.get());
                    assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                    RemoteTransportException remoteTransportException = (RemoteTransportException) failure.get();
                    assertEquals(RestStatus.NOT_FOUND, remoteTransportException.status());
                }

                int numDisconnectedClusters = randomIntBetween(1, numClusters);
                Set<DiscoveryNode> disconnectedNodes = new HashSet<>(numDisconnectedClusters);
                Set<Integer> disconnectedNodesIndices = new HashSet<>(numDisconnectedClusters);
                while (disconnectedNodes.size() < numDisconnectedClusters) {
                    int i = randomIntBetween(0, numClusters - 1);
                    if (disconnectedNodes.add(nodes[i])) {
                        assertTrue(disconnectedNodesIndices.add(i));
                    }
                }

                CountDownLatch disconnectedLatch = new CountDownLatch(numDisconnectedClusters);
                RemoteClusterServiceTests.addConnectionListener(remoteClusterService, new TransportConnectionListener() {
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
                    AtomicInteger skippedClusters = new AtomicInteger(0);
                    AtomicReference<Exception> failure = new AtomicReference<>();
                    TransportSearchAction.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, skippedClusters,
                        remoteIndicesByCluster, remoteClusterService, threadPool,
                        new LatchedActionListener<>(ActionListener.wrap(r -> fail("no response expected"), failure::set), latch));
                    awaitLatch(latch, 5, TimeUnit.SECONDS);
                    assertEquals(0, skippedClusters.get());
                    assertNotNull(failure.get());
                    assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                    assertThat(failure.get().getMessage(), containsString("error while communicating with remote cluster ["));
                    assertThat(failure.get().getCause(), instanceOf(NodeDisconnectedException.class));
                }

                //setting skip_unavailable to true for all the disconnected clusters will make the request succeed again
                for (int i : disconnectedNodesIndices) {
                    RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
                }

                {
                    final CountDownLatch latch = new CountDownLatch(1);
                    AtomicInteger skippedClusters = new AtomicInteger(0);
                    AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                    TransportSearchAction.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, skippedClusters,
                        remoteIndicesByCluster, remoteClusterService, threadPool,
                        new LatchedActionListener<>(ActionListener.wrap(response::set, e -> fail("no failures expected")), latch));
                    awaitLatch(latch, 5, TimeUnit.SECONDS);
                    assertNotNull(response.get());
                    Map<String, ClusterSearchShardsResponse> map = response.get();
                    assertEquals(numClusters - disconnectedNodesIndices.size(), map.size());
                    assertEquals(skippedClusters.get(), disconnectedNodesIndices.size());
                    for (int i = 0; i < numClusters; i++) {
                        String clusterAlias = "remote" + i;
                        if (disconnectedNodesIndices.contains(i)) {
                            assertFalse(map.containsKey(clusterAlias));
                        } else {
                            assertNotNull(map.get(clusterAlias));
                        }
                    }
                }

                //give transport service enough time to realize that the node is down, and to notify the connection listeners
                //so that RemoteClusterConnection is left with no connected nodes, hence it will retry connecting next
                assertTrue(disconnectedLatch.await(5, TimeUnit.SECONDS));

                service.clearAllRules();
                if (randomBoolean()) {
                    for (int i : disconnectedNodesIndices) {
                        if (randomBoolean()) {
                            RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
                        }

                    }
                }
                {
                    final CountDownLatch latch = new CountDownLatch(1);
                    AtomicInteger skippedClusters = new AtomicInteger(0);
                    AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                    TransportSearchAction.collectSearchShards(IndicesOptions.lenientExpandOpen(), null, null, skippedClusters,
                        remoteIndicesByCluster, remoteClusterService, threadPool,
                        new LatchedActionListener<>(ActionListener.wrap(response::set, e -> fail("no failures expected")), latch));
                    awaitLatch(latch, 5, TimeUnit.SECONDS);
                    assertEquals(0, skippedClusters.get());
                    assertNotNull(response.get());
                    Map<String, ClusterSearchShardsResponse> map = response.get();
                    assertEquals(numClusters, map.size());
                    for (int i = 0; i < numClusters; i++) {
                        String clusterAlias = "remote" + i;
                        assertTrue(map.containsKey(clusterAlias));
                        assertNotNull(map.get(clusterAlias));
                    }
                }
                assertEquals(0, service.getConnectionManager().size());
            }
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }
}
