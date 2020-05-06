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

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.OriginalIndicesTests;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.GroupShardsIteratorTests;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortBuilders;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.test.InternalAggregationTestCase.emptyReduceContextBuilder;
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

    private static SearchShardIterator createSearchShardIterator(int id, Index index,
                                                                 OriginalIndices originalIndices, String clusterAlias) {
        ShardId shardId = new ShardId(index, id);
        List<ShardRouting> shardRoutings = GroupShardsIteratorTests.randomShardRoutings(shardId);
        return new SearchShardIterator(clusterAlias, shardId, shardRoutings, originalIndices);
    }

    public void testMergeShardsIterators() {
        Index[] indices = new Index[randomIntBetween(1, 10)];
        for (int i = 0; i < indices.length; i++) {
            if (randomBoolean() && i > 0) {
                Index existingIndex = indices[randomIntBetween(0, i - 1)];
                indices[i] = new Index(existingIndex.getName(), randomAlphaOfLength(10));
            } else {
                indices[i] = new Index(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10));
            }
        }
        Arrays.sort(indices, (o1, o2) -> {
            int nameCompareTo = o1.getName().compareTo(o2.getName());
            if (nameCompareTo == 0) {
                return o1.getUUID().compareTo(o2.getUUID());
            }
            return nameCompareTo;
        });
        String[] remoteClusters = new String[randomIntBetween(1, 3)];
        for (int i = 0; i < remoteClusters.length; i++) {
            remoteClusters[i] = randomAlphaOfLengthBetween(5, 10);
        }
        Arrays.sort(remoteClusters);

        List<SearchShardIterator> expected = new ArrayList<>();
        String localClusterAlias = randomAlphaOfLengthBetween(5, 10);
        OriginalIndices localIndices = OriginalIndicesTests.randomOriginalIndices();
        List<ShardIterator> localShardIterators = new ArrayList<>();
        List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
        int numShards = randomIntBetween(0, 10);
        for (int i = 0; i < numShards; i++) {
            int numIndices = randomIntBetween(0, indices.length);
            for (int j = 0; j < numIndices; j++) {
                Index index = indices[j];
                boolean localIndex = randomBoolean();
                if (localIndex) {
                    SearchShardIterator localIterator = createSearchShardIterator(i, index, localIndices, localClusterAlias);
                    localShardIterators.add(new PlainShardIterator(localIterator.shardId(), localIterator.getShardRoutings()));
                    if (rarely()) {
                        String remoteClusterAlias = randomFrom(remoteClusters);
                        //simulate scenario where the local cluster is also registered as a remote one
                        SearchShardIterator remoteIterator = createSearchShardIterator(i, index,
                            OriginalIndicesTests.randomOriginalIndices(), remoteClusterAlias);
                        remoteShardIterators.add(remoteIterator);
                        assert remoteClusterAlias.equals(localClusterAlias) == false;
                        if (remoteClusterAlias.compareTo(localClusterAlias) < 0) {
                            expected.add(remoteIterator);
                            expected.add(localIterator);
                        } else {
                            expected.add(localIterator);
                            expected.add(remoteIterator);
                        }
                    } else {
                        expected.add(localIterator);
                    }
                } else if (rarely()) {
                    int numClusters = randomIntBetween(1, remoteClusters.length);
                    for (int k = 0; k < numClusters; k++) {
                        //simulate scenario where the same cluster is registered multiple times with different aliases
                        String clusterAlias = remoteClusters[k];
                        SearchShardIterator iterator = createSearchShardIterator(i, index, OriginalIndicesTests.randomOriginalIndices(),
                            clusterAlias);
                        expected.add(iterator);
                        remoteShardIterators.add(iterator);
                    }
                } else {
                    SearchShardIterator iterator = createSearchShardIterator(i, index, OriginalIndicesTests.randomOriginalIndices(),
                        randomFrom(remoteClusters));
                    expected.add(iterator);
                    remoteShardIterators.add(iterator);
                }
            }
        }

        Collections.shuffle(localShardIterators, random());
        Collections.shuffle(remoteShardIterators, random());

        GroupShardsIterator<SearchShardIterator> groupShardsIterator = TransportSearchAction.mergeShardsIterators(
            new GroupShardsIterator<>(localShardIterators), localIndices, localClusterAlias, remoteShardIterators);
        List<SearchShardIterator> result = new ArrayList<>();
        for (SearchShardIterator searchShardIterator : groupShardsIterator) {
            result.add(searchShardIterator);
        }
        assertEquals(expected, result);
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

    private MockTransportService[] startTransport(int numClusters, DiscoveryNode[] nodes, Map<String, OriginalIndices> remoteIndices,
                                                  Settings.Builder settingsBuilder) {
        MockTransportService[] mockTransportServices = new MockTransportService[numClusters];
        for (int i = 0; i < numClusters; i++) {
            List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
            MockTransportService remoteSeedTransport = RemoteClusterConnectionTests.startTransport("node_remote" + i, knownNodes,
                Version.CURRENT, threadPool);
            mockTransportServices[i] = remoteSeedTransport;
            DiscoveryNode remoteSeedNode = remoteSeedTransport.getLocalDiscoNode();
            knownNodes.add(remoteSeedNode);
            nodes[i] = remoteSeedNode;
            settingsBuilder.put("cluster.remote.remote" + i + ".seeds", remoteSeedNode.getAddress().toString());
            remoteIndices.put("remote" + i, new OriginalIndices(new String[]{"index"}, IndicesOptions.lenientExpandOpen()));
        }
        return mockTransportServices;
    }

    private static SearchResponse emptySearchResponse() {
        InternalSearchResponse response = new InternalSearchResponse(new SearchHits(new SearchHit[0],
            new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN), InternalAggregations.EMPTY, null, null, false, null, 1);
        return new SearchResponse(response, null, 1, 1, 0, 100, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    public void testCCSRemoteReduceMergeFails() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder);
        Settings settings = builder.build();
        boolean local = randomBoolean();
        OriginalIndices localIndices = local ? new OriginalIndices(new String[]{"index"}, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        Function<Boolean, InternalAggregation.ReduceContext> reduceContext = finalReduce -> null;
        try (MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.preference("null_target");
            final CountDownLatch latch = new CountDownLatch(1);
            SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
            AtomicReference<Exception> failure = new AtomicReference<>();
            LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                ActionListener.wrap(r -> fail("no response expected"), failure::set), latch);
            TransportSearchAction.ccsRemoteReduce(searchRequest, localIndices, remoteIndicesByCluster, timeProvider,
                    emptyReduceContextBuilder(), remoteClusterService, threadPool, listener, (r, l) -> setOnce.set(Tuple.tuple(r, l)));
            if (localIndices == null) {
                assertNull(setOnce.get());
            } else {
                Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                assertEquals("", tuple.v1().getLocalClusterAlias());
                assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                tuple.v2().onResponse(emptySearchResponse());
            }
            awaitLatch(latch, 5, TimeUnit.SECONDS);
            assertNotNull(failure.get());
            //the intention here is not to test that we throw NPE, rather to trigger a situation that makes
            //SearchResponseMerger#getMergedResponse fail unexpectedly and verify that the listener is properly notified with the NPE
            assertThat(failure.get(), instanceOf(NullPointerException.class));
            assertEquals(0, service.getConnectionManager().size());
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testCCSRemoteReduce() throws Exception {
        int numClusters = randomIntBetween(1, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder);
        Settings settings = builder.build();
        boolean local = randomBoolean();
        OriginalIndices localIndices = local ? new OriginalIndices(new String[]{"index"}, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
        int totalClusters = numClusters + (local ? 1 : 0);
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        try (MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<SearchResponse> response = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(response::set, e -> fail("no failures expected")), latch);
                TransportSearchAction.ccsRemoteReduce(searchRequest, localIndices, remoteIndicesByCluster, timeProvider,
                        emptyReduceContextBuilder(), remoteClusterService, threadPool, listener, (r, l) -> setOnce.set(Tuple.tuple(r, l)));
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                assertEquals(0, searchResponse.getClusters().getSkipped());
                assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                assertEquals(totalClusters, searchResponse.getClusters().getSuccessful());
                assertEquals(totalClusters == 1 ? 1 : totalClusters + 1, searchResponse.getNumReducePhases());
            }
            {
                SearchRequest searchRequest = new SearchRequest();
                searchRequest.preference("index_not_found");
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<Exception> failure = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(r -> fail("no response expected"), failure::set), latch);
                TransportSearchAction.ccsRemoteReduce(searchRequest, localIndices, remoteIndicesByCluster, timeProvider,
                        emptyReduceContextBuilder(), remoteClusterService, threadPool, listener, (r, l) -> setOnce.set(Tuple.tuple(r, l)));
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);
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
                public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                    if (disconnectedNodes.remove(node)) {
                        disconnectedLatch.countDown();
                    }
                }
            });
            for (DiscoveryNode disconnectedNode : disconnectedNodes) {
                service.addFailToSendNoConnectRule(disconnectedNode.getAddress());
            }

            {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<Exception> failure = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(r -> fail("no response expected"), failure::set), latch);
                TransportSearchAction.ccsRemoteReduce(searchRequest, localIndices, remoteIndicesByCluster, timeProvider,
                        emptyReduceContextBuilder(), remoteClusterService, threadPool, listener, (r, l) -> setOnce.set(Tuple.tuple(r, l)));
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);
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
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<SearchResponse> response = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(response::set, e -> fail("no failures expected")), latch);
                TransportSearchAction.ccsRemoteReduce(searchRequest, localIndices, remoteIndicesByCluster, timeProvider,
                        emptyReduceContextBuilder(), remoteClusterService, threadPool, listener, (r, l) -> setOnce.set(Tuple.tuple(r, l)));
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                assertEquals(disconnectedNodesIndices.size(), searchResponse.getClusters().getSkipped());
                assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                int successful = totalClusters - disconnectedNodesIndices.size();
                assertEquals(successful, searchResponse.getClusters().getSuccessful());
                assertEquals(successful == 0 ? 0 : successful + 1, searchResponse.getNumReducePhases());
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
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<SearchResponse> response = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(response::set, e -> fail("no failures expected")), latch);
                TransportSearchAction.ccsRemoteReduce(searchRequest, localIndices, remoteIndicesByCluster, timeProvider,
                        emptyReduceContextBuilder(), remoteClusterService, threadPool, listener, (r, l) -> setOnce.set(Tuple.tuple(r, l)));
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                assertEquals(0, searchResponse.getClusters().getSkipped());
                assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                assertEquals(totalClusters, searchResponse.getClusters().getSuccessful());
                assertEquals(totalClusters == 1 ? 1 : totalClusters + 1, searchResponse.getNumReducePhases());
            }
            assertEquals(0, service.getConnectionManager().size());
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testCollectSearchShards() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder);
        Settings settings = builder.build();
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
                public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
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
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testCreateSearchResponseMerger() {
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        {
            SearchSourceBuilder source = new SearchSourceBuilder();
            assertEquals(-1, source.size());
            assertEquals(-1, source.from());
            assertNull(source.trackTotalHitsUpTo());
            SearchResponseMerger merger = TransportSearchAction.createSearchResponseMerger(
                    source, timeProvider, emptyReduceContextBuilder());
            assertEquals(0, merger.from);
            assertEquals(10, merger.size);
            assertEquals(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, merger.trackTotalHitsUpTo);
            assertEquals(0, source.from());
            assertEquals(10, source.size());
            assertNull(source.trackTotalHitsUpTo());
        }
        {
            SearchResponseMerger merger = TransportSearchAction.createSearchResponseMerger(null, timeProvider, emptyReduceContextBuilder());
            assertEquals(0, merger.from);
            assertEquals(10, merger.size);
            assertEquals(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, merger.trackTotalHitsUpTo);
        }
        {
            SearchSourceBuilder source = new SearchSourceBuilder();
            int originalFrom = randomIntBetween(0, 1000);
            source.from(originalFrom);
            int originalSize = randomIntBetween(0, 1000);
            source.size(originalSize);
            int trackTotalHitsUpTo = randomIntBetween(0, Integer.MAX_VALUE);
            source.trackTotalHitsUpTo(trackTotalHitsUpTo);
            SearchResponseMerger merger = TransportSearchAction.createSearchResponseMerger(
                    source, timeProvider, emptyReduceContextBuilder());
            assertEquals(0, source.from());
            assertEquals(originalFrom + originalSize, source.size());
            assertEquals(trackTotalHitsUpTo, (int)source.trackTotalHitsUpTo());
            assertEquals(originalFrom, merger.from);
            assertEquals(originalSize, merger.size);
            assertEquals(trackTotalHitsUpTo, merger.trackTotalHitsUpTo);
        }
    }

    public void testShouldMinimizeRoundtrips() throws Exception {
        {
            SearchRequest searchRequest = new SearchRequest();
            assertTrue(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder());
            assertTrue(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.scroll("5s");
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            SearchSourceBuilder source = new SearchSourceBuilder();
            searchRequest.source(source);
            CollapseBuilder collapseBuilder = new CollapseBuilder("field");
            source.collapse(collapseBuilder);
            collapseBuilder.setInnerHits(new InnerHitBuilder("inner"));
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequestTests searchRequestTests = new SearchRequestTests();
            searchRequestTests.setUp();
            SearchRequest searchRequest = searchRequestTests.createSearchRequest();
            searchRequest.scroll((Scroll)null);
            searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
            SearchSourceBuilder source = searchRequest.source();
            if (source != null) {
                CollapseBuilder collapse = source.collapse();
                if (collapse != null) {
                    collapse.setInnerHits(Collections.emptyList());
                }
            }
            searchRequest.setCcsMinimizeRoundtrips(true);
            assertTrue(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
            searchRequest.setCcsMinimizeRoundtrips(false);
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
    }

    public void testShouldPreFilterSearchShards() {
        int numIndices = randomIntBetween(2, 10);
        Index[] indices = new Index[numIndices];
        for (int i = 0; i < numIndices; i++) {
            String indexName = randomAlphaOfLengthBetween(5, 10);
            indices[i] = new Index(indexName, indexName + "-uuid");
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        {
            SearchRequest searchRequest = new SearchRequest();
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(2, 128)));
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(129, 10000)));
        }
        {
            SearchRequest searchRequest = new SearchRequest()
                .source(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp")));
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(2, 128)));
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(129, 10000)));
        }
        {
            SearchRequest searchRequest = new SearchRequest()
                .source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp")));
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(2, 127)));
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(127, 10000)));
        }
        {
            SearchRequest searchRequest = new SearchRequest()
                .source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp")))
                .scroll("5m");
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(2, 128)));
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(129, 10000)));
        }
    }

    public void testShouldPreFilterSearchShardsWithReadOnly() {
        int numIndices = randomIntBetween(2, 10);
        int numReadOnly = randomIntBetween(1, numIndices);
        Index[] indices = new Index[numIndices];
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder();
        for (int i = 0; i < numIndices; i++) {
            String indexName = randomAlphaOfLengthBetween(5, 10);
            indices[i] = new Index(indexName, indexName + "-uuid");
            if (--numReadOnly >= 0) {
                if (randomBoolean()) {
                    blocksBuilder.addIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);
                } else {
                    blocksBuilder.addIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_BLOCK);
                }
            }
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).blocks(blocksBuilder).build();
        {
            SearchRequest searchRequest = new SearchRequest();
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(2, 127)));
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(127, 10000)));
        }
        {
            SearchRequest searchRequest = new SearchRequest()
                .source(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp")));
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(2, 127)));
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(127, 10000)));
        }
        {
            SearchRequest searchRequest = new SearchRequest()
                .source(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp")));
            searchRequest.scroll("5s");
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(2, 127)));
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(127, 10000)));
        }
        {
            SearchRequest searchRequest = new SearchRequest()
                .source(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp")));
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(2, 127)));
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest,
                indices, randomIntBetween(127, 10000)));
        }
    }
}
