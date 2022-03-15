/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.GroupShardsIteratorTests;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.tasks.TaskId;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.action.search.SearchRequest.DEFAULT_PRE_FILTER_SHARD_SIZE;
import static org.elasticsearch.test.InternalAggregationTestCase.emptyReduceContextBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSearchActionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private static SearchShardIterator createSearchShardIterator(
        int id,
        Index index,
        OriginalIndices originalIndices,
        String clusterAlias
    ) {
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
        List<SearchShardIterator> localShardIterators = new ArrayList<>();
        List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
        int numShards = randomIntBetween(0, 10);
        for (int i = 0; i < numShards; i++) {
            int numIndices = randomIntBetween(0, indices.length);
            for (int j = 0; j < numIndices; j++) {
                Index index = indices[j];
                boolean localIndex = randomBoolean();
                if (localIndex) {
                    SearchShardIterator localIterator = createSearchShardIterator(i, index, localIndices, localClusterAlias);
                    localShardIterators.add(localIterator);
                    if (rarely()) {
                        String remoteClusterAlias = randomFrom(remoteClusters);
                        // simulate scenario where the local cluster is also registered as a remote one
                        SearchShardIterator remoteIterator = createSearchShardIterator(
                            i,
                            index,
                            OriginalIndicesTests.randomOriginalIndices(),
                            remoteClusterAlias
                        );
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
                        // simulate scenario where the same cluster is registered multiple times with different aliases
                        String clusterAlias = remoteClusters[k];
                        SearchShardIterator iterator = createSearchShardIterator(
                            i,
                            index,
                            OriginalIndicesTests.randomOriginalIndices(),
                            clusterAlias
                        );
                        expected.add(iterator);
                        remoteShardIterators.add(iterator);
                    }
                } else {
                    SearchShardIterator iterator = createSearchShardIterator(
                        i,
                        index,
                        OriginalIndicesTests.randomOriginalIndices(),
                        randomFrom(remoteClusters)
                    );
                    expected.add(iterator);
                    remoteShardIterators.add(iterator);
                }
            }
        }

        Collections.shuffle(localShardIterators, random());
        Collections.shuffle(remoteShardIterators, random());

        GroupShardsIterator<SearchShardIterator> groupShardsIterator = TransportSearchAction.mergeShardsIterators(
            localShardIterators,
            remoteShardIterators
        );
        List<SearchShardIterator> result = new ArrayList<>();
        for (SearchShardIterator searchShardIterator : groupShardsIterator) {
            result.add(searchShardIterator);
        }
        assertEquals(expected, result);
    }

    public void testProcessRemoteShards() {
        try (TransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
            RemoteClusterService service = transportService.getRemoteClusterService();
            assertFalse(service.isCrossClusterSearchEnabled());
            Map<String, ClusterSearchShardsResponse> searchShardsResponseMap = new HashMap<>();
            DiscoveryNode[] nodes = new DiscoveryNode[] {
                new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT),
                new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT) };
            Map<String, AliasFilter> indicesAndAliases = new HashMap<>();
            indicesAndAliases.put(
                "foo",
                new AliasFilter(new TermsQueryBuilder("foo", "bar"), "some_alias_for_foo", "some_other_foo_alias")
            );
            indicesAndAliases.put("bar", new AliasFilter(new MatchAllQueryBuilder(), Strings.EMPTY_ARRAY));
            ClusterSearchShardsGroup[] groups = new ClusterSearchShardsGroup[] {
                new ClusterSearchShardsGroup(
                    new ShardId("foo", "foo_id", 0),
                    new ShardRouting[] {
                        TestShardRouting.newShardRouting("foo", 0, "node1", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("foo", 0, "node2", false, ShardRoutingState.STARTED) }
                ),
                new ClusterSearchShardsGroup(
                    new ShardId("foo", "foo_id", 1),
                    new ShardRouting[] {
                        TestShardRouting.newShardRouting("foo", 0, "node1", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("foo", 1, "node2", false, ShardRoutingState.STARTED) }
                ),
                new ClusterSearchShardsGroup(
                    new ShardId("bar", "bar_id", 0),
                    new ShardRouting[] {
                        TestShardRouting.newShardRouting("bar", 0, "node2", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("bar", 0, "node1", false, ShardRoutingState.STARTED) }
                ) };
            searchShardsResponseMap.put("test_cluster_1", new ClusterSearchShardsResponse(groups, nodes, indicesAndAliases));
            DiscoveryNode[] nodes2 = new DiscoveryNode[] { new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT) };
            ClusterSearchShardsGroup[] groups2 = new ClusterSearchShardsGroup[] {
                new ClusterSearchShardsGroup(
                    new ShardId("xyz", "xyz_id", 0),
                    new ShardRouting[] { TestShardRouting.newShardRouting("xyz", 0, "node3", true, ShardRoutingState.STARTED) }
                ) };
            Map<String, AliasFilter> filter = new HashMap<>();
            filter.put("xyz", new AliasFilter(null, "some_alias_for_xyz"));
            searchShardsResponseMap.put("test_cluster_2", new ClusterSearchShardsResponse(groups2, nodes2, filter));

            Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
            remoteIndicesByCluster.put(
                "test_cluster_1",
                new OriginalIndices(new String[] { "fo*", "ba*" }, SearchRequest.DEFAULT_INDICES_OPTIONS)
            );
            remoteIndicesByCluster.put("test_cluster_2", new OriginalIndices(new String[] { "x*" }, SearchRequest.DEFAULT_INDICES_OPTIONS));
            Map<String, AliasFilter> remoteAliases = TransportSearchAction.getRemoteAliasFilters(searchShardsResponseMap);
            List<SearchShardIterator> iteratorList = TransportSearchAction.getRemoteShardsIterator(
                searchShardsResponseMap,
                remoteIndicesByCluster,
                remoteAliases
            );
            assertEquals(4, iteratorList.size());
            for (SearchShardIterator iterator : iteratorList) {
                if (iterator.shardId().getIndexName().endsWith("foo")) {
                    assertArrayEquals(
                        new String[] { "some_alias_for_foo", "some_other_foo_alias" },
                        iterator.getOriginalIndices().indices()
                    );
                    assertTrue(iterator.shardId().getId() == 0 || iterator.shardId().getId() == 1);
                    assertEquals("test_cluster_1", iterator.getClusterAlias());
                    assertEquals("foo", iterator.shardId().getIndexName());
                    SearchShardTarget shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "foo");
                    shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "foo");
                    assertNull(iterator.nextOrNull());
                } else if (iterator.shardId().getIndexName().endsWith("bar")) {
                    assertArrayEquals(new String[] { "bar" }, iterator.getOriginalIndices().indices());
                    assertEquals(0, iterator.shardId().getId());
                    assertEquals("test_cluster_1", iterator.getClusterAlias());
                    assertEquals("bar", iterator.shardId().getIndexName());
                    SearchShardTarget shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "bar");
                    shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "bar");
                    assertNull(iterator.nextOrNull());
                } else if (iterator.shardId().getIndexName().endsWith("xyz")) {
                    assertArrayEquals(new String[] { "some_alias_for_xyz" }, iterator.getOriginalIndices().indices());
                    assertEquals(0, iterator.shardId().getId());
                    assertEquals("xyz", iterator.shardId().getIndexName());
                    assertEquals("test_cluster_2", iterator.getClusterAlias());
                    SearchShardTarget shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "xyz");
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
        Function<String, DiscoveryNode> localNodes = (nodeId) -> new DiscoveryNode(
            "local-" + nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 1024),
            Version.CURRENT
        );
        BiFunction<String, String, DiscoveryNode> remoteNodes = (clusterAlias, nodeId) -> new DiscoveryNode(
            "remote-" + nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 2048),
            Version.CURRENT
        );
        BiFunction<String, DiscoveryNode, Transport.Connection> nodeToConnection = (clusterAlias, node) -> new Transport.Connection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws TransportException {}

            @Override
            public void addCloseListener(ActionListener<Void> listener) {}

            @Override
            public void addRemovedListener(ActionListener<Void> listener) {}

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {}

            @Override
            public void incRef() {}

            @Override
            public boolean tryIncRef() {
                return true;
            }

            @Override
            public boolean decRef() {
                assert false : "shouldn't release a mock connection";
                return false;
            }

            @Override
            public boolean hasReferences() {
                return true;
            }

            @Override
            public void onRemoved() {
                assert false : "shouldn't remove a mock connection";
            }
        };

        {
            BiFunction<String, String, Transport.Connection> connectionLookup = TransportSearchAction.buildConnectionLookup(
                null,
                localNodes,
                remoteNodes,
                nodeToConnection
            );

            Transport.Connection localConnection = connectionLookup.apply(null, randomAlphaOfLengthBetween(5, 10));
            assertThat(localConnection.getNode().getId(), startsWith("local-"));
            Transport.Connection remoteConnection = connectionLookup.apply(
                randomAlphaOfLengthBetween(5, 10),
                randomAlphaOfLengthBetween(5, 10)
            );
            assertThat(remoteConnection.getNode().getId(), startsWith("remote-"));
        }
        {
            String requestClusterAlias = randomAlphaOfLengthBetween(5, 10);
            BiFunction<String, String, Transport.Connection> connectionLookup = TransportSearchAction.buildConnectionLookup(
                requestClusterAlias,
                localNodes,
                remoteNodes,
                nodeToConnection
            );

            Transport.Connection localConnection = connectionLookup.apply(requestClusterAlias, randomAlphaOfLengthBetween(5, 10));
            assertThat(localConnection.getNode().getId(), startsWith("local-"));
        }
    }

    private MockTransportService[] startTransport(
        int numClusters,
        DiscoveryNode[] nodes,
        Map<String, OriginalIndices> remoteIndices,
        Settings.Builder settingsBuilder
    ) {
        MockTransportService[] mockTransportServices = new MockTransportService[numClusters];
        for (int i = 0; i < numClusters; i++) {
            List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
            MockTransportService remoteSeedTransport = RemoteClusterConnectionTests.startTransport(
                "node_remote" + i,
                knownNodes,
                Version.CURRENT,
                threadPool
            );
            mockTransportServices[i] = remoteSeedTransport;
            DiscoveryNode remoteSeedNode = remoteSeedTransport.getLocalDiscoNode();
            knownNodes.add(remoteSeedNode);
            nodes[i] = remoteSeedNode;
            settingsBuilder.put("cluster.remote.remote" + i + ".seeds", remoteSeedNode.getAddress().toString());
            remoteIndices.put("remote" + i, new OriginalIndices(new String[] { "index" }, IndicesOptions.lenientExpandOpen()));
        }
        return mockTransportServices;
    }

    private static SearchResponse emptySearchResponse() {
        InternalSearchResponse response = new InternalSearchResponse(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
            InternalAggregations.EMPTY,
            null,
            null,
            false,
            null,
            1
        );
        return new SearchResponse(response, null, 1, 1, 0, 100, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY, null);
    }

    public void testCCSRemoteReduceMergeFails() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder);
        Settings settings = builder.build();
        boolean local = randomBoolean();
        OriginalIndices localIndices = local ? new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        Function<Boolean, AggregationReduceContext> reduceContext = finalReduce -> null;
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
                ActionListener.wrap(r -> fail("no response expected"), failure::set),
                latch
            );
            TransportSearchAction.ccsRemoteReduce(
                new TaskId("n", 1),
                searchRequest,
                localIndices,
                remoteIndicesByCluster,
                timeProvider,
                emptyReduceContextBuilder(),
                remoteClusterService,
                threadPool,
                listener,
                (r, l) -> setOnce.set(Tuple.tuple(r, l))
            );
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
            // the intention here is not to test that we throw NPE, rather to trigger a situation that makes
            // SearchResponseMerger#getMergedResponse fail unexpectedly and verify that the listener is properly notified with the NPE
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
        OriginalIndices localIndices = local ? new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
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
                    ActionListener.wrap(response::set, e -> fail("no failures expected")),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    new TaskId("n", 1),
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l))
                );
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
                    ActionListener.wrap(r -> fail("no response expected"), failure::set),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    new TaskId("n", 1),
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l))
                );
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
                    ActionListener.wrap(r -> fail("no response expected"), failure::set),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    new TaskId("n", 1),
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l))
                );
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

            // setting skip_unavailable to true for all the disconnected clusters will make the request succeed again
            for (int i : disconnectedNodesIndices) {
                RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
            }

            {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<SearchResponse> response = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(response::set, e -> fail("no failures expected")),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    new TaskId("n", 1),
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l))
                );
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

            // give transport service enough time to realize that the node is down, and to notify the connection listeners
            // so that RemoteClusterConnection is left with no connected nodes, hence it will retry connecting next
            assertTrue(disconnectedLatch.await(5, TimeUnit.SECONDS));

            service.clearAllRules();
            if (randomBoolean()) {
                for (int i : disconnectedNodesIndices) {
                    if (randomBoolean()) {
                        RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
                    }

                }
            }
            // put the following in assert busy as connections are lazily reestablished
            assertBusy(() -> {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<SearchResponse> response = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(response::set, e -> fail("no failures expected")),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    new TaskId("n", 1),
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l))
                );
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
            });
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
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    new LatchedActionListener<>(ActionListener.wrap(response::set, e -> fail("no failures expected")), latch)
                );
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
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    "index_not_found",
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    new LatchedActionListener<>(ActionListener.wrap(r -> fail("no response expected"), failure::set), latch)
                );
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
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    new LatchedActionListener<>(ActionListener.wrap(r -> fail("no response expected"), failure::set), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertEquals(0, skippedClusters.get());
                assertNotNull(failure.get());
                assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                assertThat(failure.get().getMessage(), containsString("error while communicating with remote cluster ["));
                assertThat(failure.get().getCause(), instanceOf(NodeDisconnectedException.class));
            }

            // setting skip_unavailable to true for all the disconnected clusters will make the request succeed again
            for (int i : disconnectedNodesIndices) {
                RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
            }

            {
                final CountDownLatch latch = new CountDownLatch(1);
                AtomicInteger skippedClusters = new AtomicInteger(0);
                AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    new LatchedActionListener<>(ActionListener.wrap(response::set, e -> fail("no failures expected")), latch)
                );
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

            // give transport service enough time to realize that the node is down, and to notify the connection listeners
            // so that RemoteClusterConnection is left with no connected nodes, hence it will retry connecting next
            assertTrue(disconnectedLatch.await(5, TimeUnit.SECONDS));

            service.clearAllRules();
            if (randomBoolean()) {
                for (int i : disconnectedNodesIndices) {
                    if (randomBoolean()) {
                        RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
                    }

                }
            }
            // run the following under assertBusy as connections are lazily reestablished
            assertBusy(() -> {
                final CountDownLatch latch = new CountDownLatch(1);
                AtomicInteger skippedClusters = new AtomicInteger(0);
                AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    new LatchedActionListener<>(ActionListener.wrap(response::set, e -> fail("no failures expected")), latch)
                );
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
            });
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
                source,
                timeProvider,
                emptyReduceContextBuilder()
            );
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
                source,
                timeProvider,
                emptyReduceContextBuilder()
            );
            assertEquals(0, source.from());
            assertEquals(originalFrom + originalSize, source.size());
            assertEquals(trackTotalHitsUpTo, (int) source.trackTotalHitsUpTo());
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
            searchRequest.scroll((Scroll) null);
            searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
            SearchSourceBuilder source = searchRequest.source();
            if (source != null) {
                source.pointInTimeBuilder(null);
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
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        {
            SearchRequest searchRequest = new SearchRequest();
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, 128),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(129, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp"))
            );
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(DEFAULT_PRE_FILTER_SHARD_SIZE + 1, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp")));
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(DEFAULT_PRE_FILTER_SHARD_SIZE - 1, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp")))
                .scroll("5m");
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(DEFAULT_PRE_FILTER_SHARD_SIZE + 1, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
    }

    public void testShouldPreFilterSearchShardsWithReadOnly() {
        int numIndices = randomIntBetween(2, 10);
        int numReadOnly = randomIntBetween(1, numIndices);
        String[] indices = new String[numIndices];
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder();
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
            ;
            if (--numReadOnly >= 0) {
                if (randomBoolean()) {
                    blocksBuilder.addIndexBlock(indices[i], IndexMetadata.INDEX_WRITE_BLOCK);
                } else {
                    blocksBuilder.addIndexBlock(indices[i], IndexMetadata.INDEX_READ_ONLY_BLOCK);
                }
            }
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).blocks(blocksBuilder).build();
        {
            SearchRequest searchRequest = new SearchRequest();
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(DEFAULT_PRE_FILTER_SHARD_SIZE - 1, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp"))
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(DEFAULT_PRE_FILTER_SHARD_SIZE - 1, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp"))
            );
            searchRequest.scroll("5s");
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(DEFAULT_PRE_FILTER_SHARD_SIZE - 1, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp"))
            );
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    clusterState,
                    searchRequest,
                    indices,
                    randomIntBetween(DEFAULT_PRE_FILTER_SHARD_SIZE - 1, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
    }

    public void testLocalShardIteratorFromPointInTime() {
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(0, 2);
        final String[] indices = { "test-1", "test-2" };
        final ClusterState clusterState = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(
            indices,
            numberOfShards,
            numberOfReplicas
        );
        final IndexMetadata indexMetadata = clusterState.metadata().index("test-1");
        Map<ShardId, SearchContextIdForNode> contexts = new HashMap<>();
        Set<ShardId> relocatedContexts = new HashSet<>();
        Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            final String targetNode;
            if (randomBoolean()) {
                final IndexRoutingTable routingTable = clusterState.routingTable().index(indexMetadata.getIndex());
                targetNode = randomFrom(routingTable.shard(shardId).assignedShards()).currentNodeId();
            } else {
                // relocated or no longer assigned
                relocatedContexts.add(new ShardId(indexMetadata.getIndex(), shardId));
                targetNode = randomFrom(clusterState.nodes()).getId();
            }
            contexts.put(
                new ShardId(indexMetadata.getIndex(), shardId),
                new SearchContextIdForNode(
                    null,
                    targetNode,
                    new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong(), null)
                )
            );
            aliasFilterMap.putIfAbsent(indexMetadata.getIndexUUID(), AliasFilter.EMPTY);
        }
        TimeValue keepAlive = randomBoolean() ? null : TimeValue.timeValueSeconds(between(30, 3600));

        final List<SearchShardIterator> shardIterators = TransportSearchAction.getLocalLocalShardsIteratorFromPointInTime(
            clusterState,
            OriginalIndices.NONE,
            null,
            new SearchContextId(contexts, aliasFilterMap),
            keepAlive,
            randomBoolean()
        );
        shardIterators.sort(Comparator.comparing(SearchShardIterator::shardId));
        assertThat(shardIterators, hasSize(numberOfShards));
        for (int id = 0; id < numberOfShards; id++) {
            final ShardId shardId = new ShardId(indexMetadata.getIndex(), id);
            final SearchShardIterator shardIterator = shardIterators.get(id);
            final SearchContextIdForNode context = contexts.get(shardId);
            if (context.getSearchContextId().getSearcherId() == null) {
                assertThat(shardIterator.getTargetNodeIds(), hasSize(1));
            } else {
                final List<String> targetNodes = clusterState.routingTable()
                    .index(indexMetadata.getIndex())
                    .shard(id)
                    .assignedShards()
                    .stream()
                    .map(ShardRouting::currentNodeId)
                    .toList();
                if (relocatedContexts.contains(shardId)) {
                    targetNodes.add(context.getNode());
                }
                assertThat(shardIterator.getTargetNodeIds(), containsInAnyOrder(targetNodes.toArray(new String[0])));
            }
            assertThat(shardIterator.getTargetNodeIds().get(0), equalTo(context.getNode()));
            assertThat(shardIterator.getSearchContextId(), equalTo(context.getSearchContextId()));
            assertThat(shardIterator.getSearchContextKeepAlive(), equalTo(keepAlive));
        }

        // Fails when some indices don't exist and `allowPartialSearchResults` is false.
        ShardId anotherShardId = new ShardId(new Index("another-index", IndexMetadata.INDEX_UUID_NA_VALUE), randomIntBetween(0, 10));
        contexts.put(
            anotherShardId,
            new SearchContextIdForNode(
                null,
                randomFrom(clusterState.nodes()).getId(),
                new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong(), null)
            )
        );
        IndexNotFoundException error = expectThrows(IndexNotFoundException.class, () -> {
            TransportSearchAction.getLocalLocalShardsIteratorFromPointInTime(
                clusterState,
                OriginalIndices.NONE,
                null,
                new SearchContextId(contexts, aliasFilterMap),
                keepAlive,
                false
            );
        });
        assertThat(error.getIndex().getName(), equalTo("another-index"));
        // Ok when some indices don't exist and `allowPartialSearchResults` is true.
        Optional<SearchShardIterator> anotherShardIterator = TransportSearchAction.getLocalLocalShardsIteratorFromPointInTime(
            clusterState,
            OriginalIndices.NONE,
            null,
            new SearchContextId(contexts, aliasFilterMap),
            keepAlive,
            true
        ).stream().filter(si -> si.shardId().equals(anotherShardId)).findFirst();
        assertTrue(anotherShardIterator.isPresent());
        assertThat(anotherShardIterator.get().getTargetNodeIds(), hasSize(1));
    }

    public void testCCSCompatibilityCheck() throws Exception {
        Settings settings = Settings.builder()
            .put("node.name", TransportSearchAction.class.getSimpleName())
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            TransportService transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool);

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().query(new DummyQueryBuilder() {
                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {
                    throw new IllegalArgumentException("This query isn't serializable to nodes before " + Version.CURRENT);
                }
            }));
            NodeClient client = new NodeClient(settings, threadPool);

            SearchService searchService = mock(SearchService.class);
            when(searchService.getRewriteContext(any())).thenReturn(new QueryRewriteContext(null, null, null, null));
            ClusterService clusterService = new ClusterService(
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool
            );
            TransportSearchAction action = new TransportSearchAction(
                threadPool,
                new NoneCircuitBreakerService(),
                transportService,
                searchService,
                new SearchTransportService(transportService, client, null),
                null,
                clusterService,
                actionFilters,
                null,
                null,
                null
            );

            CountDownLatch latch = new CountDownLatch(1);
            action.doExecute(null, searchRequest, new ActionListener<>() {

                @Override
                public void onResponse(SearchResponse response) {
                    latch.countDown();
                    fail("should not be called");
                }

                @Override
                public void onFailure(Exception ex) {
                    assertThat(
                        ex.getMessage(),
                        containsString("[class org.elasticsearch.action.search.SearchRequest] is not compatible with version")
                    );
                    assertThat(ex.getMessage(), containsString("and the 'search.check_ccs_compatibility' setting is enabled."));
                    assertEquals("This query isn't serializable to nodes before " + Version.CURRENT, ex.getCause().getMessage());
                    latch.countDown();
                }
            });
            latch.await();
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }
}
