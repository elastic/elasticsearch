/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.OriginalIndicesTests;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.TestProjectResolvers;
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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
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
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import static org.mockito.ArgumentMatchers.anyBoolean;
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
        List<ShardRouting> shardRoutings = SearchShardIteratorTests.randomShardRoutings(shardId);
        return new SearchShardIterator(clusterAlias, shardId, shardRoutings, originalIndices);
    }

    private static ResolvedIndices createMockResolvedIndices(
        OriginalIndices localIndices,
        Map<String, OriginalIndices> remoteIndicesByCluster
    ) {
        return new MockResolvedIndices(remoteIndicesByCluster, localIndices, Map.of());
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

        List<SearchShardIterator> groupShardsIterator = TransportSearchAction.mergeShardsIterators(
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
        Map<String, SearchShardsResponse> searchShardsResponseMap = new LinkedHashMap<>();
        // first cluster - new response
        {
            List<DiscoveryNode> nodes = List.of(DiscoveryNodeUtils.create("node1"), DiscoveryNodeUtils.create("node2"));
            Map<String, AliasFilter> aliasFilters1 = Map.of(
                "foo_id",
                AliasFilter.of(new TermsQueryBuilder("foo", "bar"), "some_alias_for_foo", "some_other_foo_alias"),
                "bar_id",
                AliasFilter.of(new MatchAllQueryBuilder(), Strings.EMPTY_ARRAY)
            );
            List<SearchShardsGroup> groups = List.of(
                new SearchShardsGroup(new ShardId("foo", "foo_id", 0), List.of("node1", "node2"), false),
                new SearchShardsGroup(new ShardId("foo", "foo_id", 1), List.of("node2", "node1"), true),
                new SearchShardsGroup(new ShardId("bar", "bar_id", 0), List.of("node2", "node1"), false)
            );
            searchShardsResponseMap.put("test_cluster_1", new SearchShardsResponse(groups, nodes, aliasFilters1));
        }
        // second cluster - legacy response
        {
            DiscoveryNode[] nodes2 = new DiscoveryNode[] { DiscoveryNodeUtils.create("node3") };
            ClusterSearchShardsGroup[] groups2 = new ClusterSearchShardsGroup[] {
                new ClusterSearchShardsGroup(
                    new ShardId("xyz", "xyz_id", 0),
                    new ShardRouting[] { TestShardRouting.newShardRouting("xyz", 0, "node3", true, ShardRoutingState.STARTED) }
                ) };
            Map<String, AliasFilter> aliasFilters2 = Map.of("xyz", AliasFilter.of(null, "some_alias_for_xyz"));
            searchShardsResponseMap.put(
                "test_cluster_2",
                SearchShardsResponse.fromLegacyResponse(new ClusterSearchShardsResponse(groups2, nodes2, aliasFilters2))
            );
        }
        Map<String, OriginalIndices> remoteIndicesByCluster = Map.of(
            "test_cluster_1",
            new OriginalIndices(new String[] { "fo*", "ba*" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            "test_cluster_2",
            new OriginalIndices(new String[] { "x*" }, SearchRequest.DEFAULT_INDICES_OPTIONS)
        );
        Map<String, AliasFilter> aliasFilters = new HashMap<>();
        searchShardsResponseMap.values().forEach(r -> aliasFilters.putAll(r.getAliasFilters()));
        List<SearchShardIterator> iteratorList = TransportSearchAction.getRemoteShardsIterator(
            searchShardsResponseMap,
            remoteIndicesByCluster,
            aliasFilters
        );
        assertThat(iteratorList, hasSize(4));
        {
            SearchShardIterator shardIt = iteratorList.get(0);
            assertTrue(shardIt.prefiltered());
            assertFalse(shardIt.skip());
            assertThat(shardIt.shardId(), equalTo(new ShardId("foo", "foo_id", 0)));
            assertArrayEquals(new String[] { "some_alias_for_foo", "some_other_foo_alias" }, shardIt.getOriginalIndices().indices());
            assertEquals("test_cluster_1", shardIt.getClusterAlias());
            assertEquals("foo", shardIt.shardId().getIndexName());
            SearchShardTarget shard = shardIt.nextOrNull();
            assertNotNull(shard);
            assertEquals(shard.getShardId().getIndexName(), "foo");
            assertThat(shard.getNodeId(), equalTo("node1"));
            shard = shardIt.nextOrNull();
            assertNotNull(shard);
            assertEquals(shard.getShardId().getIndexName(), "foo");
            assertThat(shard.getNodeId(), equalTo("node2"));
            assertNull(shardIt.nextOrNull());
        }
        {
            SearchShardIterator shardIt = iteratorList.get(1);
            assertTrue(shardIt.prefiltered());
            assertTrue(shardIt.skip());
            assertThat(shardIt.shardId(), equalTo(new ShardId("foo", "foo_id", 1)));
            assertArrayEquals(new String[] { "some_alias_for_foo", "some_other_foo_alias" }, shardIt.getOriginalIndices().indices());
            assertEquals("test_cluster_1", shardIt.getClusterAlias());
            assertEquals("foo", shardIt.shardId().getIndexName());
            SearchShardTarget shard = shardIt.nextOrNull();
            assertNotNull(shard);
            assertEquals(shard.getShardId().getIndexName(), "foo");
            assertThat(shard.getNodeId(), equalTo("node2"));
            shard = shardIt.nextOrNull();
            assertNotNull(shard);
            assertEquals(shard.getShardId().getIndexName(), "foo");
            assertThat(shard.getNodeId(), equalTo("node1"));
            assertNull(shardIt.nextOrNull());
        }
        {
            SearchShardIterator shardIt = iteratorList.get(2);
            assertTrue(shardIt.prefiltered());
            assertFalse(shardIt.skip());
            assertThat(shardIt.shardId(), equalTo(new ShardId("bar", "bar_id", 0)));
            assertArrayEquals(new String[] { "bar" }, shardIt.getOriginalIndices().indices());
            assertEquals("test_cluster_1", shardIt.getClusterAlias());
            SearchShardTarget shard = shardIt.nextOrNull();
            assertNotNull(shard);
            assertEquals(shard.getShardId().getIndexName(), "bar");
            assertThat(shard.getNodeId(), equalTo("node2"));
            shard = shardIt.nextOrNull();
            assertNotNull(shard);
            assertEquals(shard.getShardId().getIndexName(), "bar");
            assertThat(shard.getNodeId(), equalTo("node1"));
            assertNull(shardIt.nextOrNull());
        }
        {
            SearchShardIterator shardIt = iteratorList.get(3);
            assertFalse(shardIt.prefiltered());
            assertFalse(shardIt.skip());
            assertArrayEquals(new String[] { "some_alias_for_xyz" }, shardIt.getOriginalIndices().indices());
            assertThat(shardIt.shardId(), equalTo(new ShardId("xyz", "xyz_id", 0)));
            assertEquals("test_cluster_2", shardIt.getClusterAlias());
            SearchShardTarget shard = shardIt.nextOrNull();
            assertNotNull(shard);
            assertEquals(shard.getShardId().getIndexName(), "xyz");
            assertThat(shard.getNodeId(), equalTo("node3"));
            assertNull(shardIt.nextOrNull());
        }
    }

    public void testBuildConnectionLookup() {
        Function<String, DiscoveryNode> localNodes = (nodeId) -> DiscoveryNodeUtils.create(
            "local-" + nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 1024)
        );
        BiFunction<String, String, DiscoveryNode> remoteNodes = (clusterAlias, nodeId) -> DiscoveryNodeUtils.create(
            "remote-" + nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 2048)
        );
        BiFunction<String, DiscoveryNode, Transport.Connection> nodeToConnection = (clusterAlias, node) -> new Transport.Connection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public TransportVersion getTransportVersion() {
                return TransportVersion.current();
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
        Settings.Builder settingsBuilder,
        boolean skipUnavailable
    ) {
        MockTransportService[] mockTransportServices = new MockTransportService[numClusters];
        for (int i = 0; i < numClusters; i++) {
            List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
            MockTransportService remoteSeedTransport = RemoteClusterConnectionTests.startTransport(
                "node_remote" + i,
                knownNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            );
            mockTransportServices[i] = remoteSeedTransport;
            DiscoveryNode remoteSeedNode = remoteSeedTransport.getLocalNode();
            knownNodes.add(remoteSeedNode);
            nodes[i] = remoteSeedNode;
            settingsBuilder.put("cluster.remote.remote" + i + ".seeds", remoteSeedNode.getAddress().toString());
            settingsBuilder.put("cluster.remote.remote" + i + ".skip_unavailable", Boolean.toString(skipUnavailable));
            remoteIndices.put("remote" + i, new OriginalIndices(new String[] { "index" }, IndicesOptions.lenientExpandOpen()));
        }
        return mockTransportServices;
    }

    public void testCCSRemoteReduceMergeFails() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        boolean skipUnavailable = randomBoolean();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder, skipUnavailable);
        Settings settings = builder.build();
        boolean local = randomBoolean();
        OriginalIndices localIndices = local ? new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        ResolvedIndices mockResolvedIndices = createMockResolvedIndices(localIndices, remoteIndicesByCluster);

        try (
            MockTransportService service = MockTransportService.createNewService(
                settings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                null
            )
        ) {
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

            TaskId parentTaskId = new TaskId("n", 1);
            SearchTask task = new SearchTask(2, "search", "search", () -> "desc", parentTaskId, Collections.emptyMap());
            TransportSearchAction.ccsRemoteReduce(
                task,
                parentTaskId,
                searchRequest,
                mockResolvedIndices,
                new SearchResponse.Clusters(localIndices, remoteIndicesByCluster, true, alias -> randomBoolean()),
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
                resolveWithEmptySearchResponse(tuple);
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
        boolean skipUnavailable = randomBoolean();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder, skipUnavailable);
        Settings settings = builder.build();
        boolean local = randomBoolean();
        OriginalIndices localIndices = local ? new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
        int totalClusters = numClusters + (local ? 1 : 0);
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        ResolvedIndices mockResolvedIndices = createMockResolvedIndices(localIndices, remoteIndicesByCluster);

        try (
            MockTransportService service = MockTransportService.createNewService(
                settings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                null
            )
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            // using from: 0 and size: 10
            {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                final SetOnce<SearchResponse> response = new SetOnce<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionTestUtils.assertNoFailureListener(newValue -> {
                        newValue.incRef();
                        response.set(newValue);
                    }),
                    latch
                );
                TaskId parentTaskId = new TaskId("n", 1);
                SearchTask task = new SearchTask(2, "search", "search", () -> "desc", parentTaskId, Collections.emptyMap());
                TransportSearchAction.ccsRemoteReduce(
                    task,
                    parentTaskId,
                    searchRequest,
                    mockResolvedIndices,
                    new SearchResponse.Clusters(localIndices, remoteIndicesByCluster, true, alias -> randomBoolean()),
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
                    resolveWithEmptySearchResponse(tuple);
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                try {
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.RUNNING));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.FAILED));
                    assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                    assertEquals(
                        totalClusters,
                        searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL)
                    );
                    assertEquals(totalClusters == 1 ? 1 : totalClusters + 1, searchResponse.getNumReducePhases());
                } finally {
                    searchResponse.decRef();
                }
            }

            // using from: 5 and size: 6
            {
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().from(5).size(6);
                SearchRequest searchRequest = new SearchRequest(new String[] { "*", "*:*" }, sourceBuilder);
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                final SetOnce<SearchResponse> response = new SetOnce<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionTestUtils.assertNoFailureListener(newValue -> {
                        newValue.incRef();
                        response.set(newValue);
                    }),
                    latch
                );
                TaskId parentTaskId = new TaskId("n", 1);
                SearchTask task = new SearchTask(2, "search", "search", () -> "desc", parentTaskId, Collections.emptyMap());
                TransportSearchAction.ccsRemoteReduce(
                    task,
                    parentTaskId,
                    searchRequest,
                    mockResolvedIndices,
                    new SearchResponse.Clusters(localIndices, remoteIndicesByCluster, true, alias -> randomBoolean()),
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
                    resolveWithEmptySearchResponse(tuple);
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                try {
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.RUNNING));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.FAILED));
                    assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                    assertEquals(
                        totalClusters,
                        searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL)
                    );
                    assertEquals(totalClusters == 1 ? 1 : totalClusters + 1, searchResponse.getNumReducePhases());
                } finally {
                    searchResponse.decRef();
                }
            }

        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testCCSRemoteReduceWhereRemoteClustersFail() throws Exception {
        int numClusters = randomIntBetween(1, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        boolean skipUnavailable = randomBoolean();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder, skipUnavailable);
        Settings settings = builder.build();
        boolean local = randomBoolean();
        OriginalIndices localIndices = local ? new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        ResolvedIndices mockResolvedIndices = createMockResolvedIndices(localIndices, remoteIndicesByCluster);

        try (
            MockTransportService service = MockTransportService.createNewService(
                settings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                null
            )
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            {
                SearchRequest searchRequest = new SearchRequest();
                searchRequest.preference("index_not_found");
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<Exception> failure = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
                    if (skipUnavailable) {
                        assertThat(r.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(numClusters));
                    } else {
                        fail("no response expected");  // failure should be returned, not SearchResponse
                    }
                }, failure::set), latch);

                TaskId parentTaskId = new TaskId("n", 1);
                SearchTask task = new SearchTask(2, "search", "search", () -> "desc", parentTaskId, Collections.emptyMap());
                TransportSearchAction.ccsRemoteReduce(
                    task,
                    parentTaskId,
                    searchRequest,
                    mockResolvedIndices,
                    new SearchResponse.Clusters(localIndices, remoteIndicesByCluster, true, alias -> randomBoolean()),
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
                    resolveWithEmptySearchResponse(tuple);
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                if (skipUnavailable) {
                    assertNull(failure.get());
                } else {
                    assertNotNull(failure.get());
                    assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                    RemoteTransportException remoteTransportException = (RemoteTransportException) failure.get();
                    assertEquals(RestStatus.NOT_FOUND, remoteTransportException.status());
                }
            }

        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testCCSRemoteReduceWithDisconnectedRemoteClusters() throws Exception {
        int numClusters = randomIntBetween(1, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder, false);
        Settings settings = builder.build();
        boolean local = randomBoolean();
        OriginalIndices localIndices = local ? new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
        int totalClusters = numClusters + (local ? 1 : 0);
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        ResolvedIndices mockResolvedIndices = createMockResolvedIndices(localIndices, remoteIndicesByCluster);

        try (
            MockTransportService service = MockTransportService.createNewService(
                settings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                null
            )
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();

            int numDisconnectedClusters = randomIntBetween(1, numClusters);
            Set<DiscoveryNode> disconnectedNodes = Sets.newHashSetWithExpectedSize(numDisconnectedClusters);
            Set<Integer> disconnectedNodesIndices = Sets.newHashSetWithExpectedSize(numDisconnectedClusters);
            while (disconnectedNodes.size() < numDisconnectedClusters) {
                int i = randomIntBetween(0, numClusters - 1);
                if (disconnectedNodes.add(nodes[i])) {
                    assertTrue(disconnectedNodesIndices.add(i));
                }
            }

            CountDownLatch disconnectedLatch = new CountDownLatch(numDisconnectedClusters);
            RemoteClusterServiceTests.addConnectionListener(remoteClusterService, new TransportConnectionListener() {
                @Override
                public void onNodeDisconnected(DiscoveryNode node, @Nullable Exception closeException) {
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
                TaskId parentTaskId = new TaskId("n", 1);
                SearchTask task = new SearchTask(2, "search", "search", () -> "desc", parentTaskId, Collections.emptyMap());
                TransportSearchAction.ccsRemoteReduce(
                    task,
                    parentTaskId,
                    searchRequest,
                    mockResolvedIndices,
                    new SearchResponse.Clusters(localIndices, remoteIndicesByCluster, true, alias -> randomBoolean()),
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
                    resolveWithEmptySearchResponse(tuple);
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
                SetOnce<SearchResponse> response = new SetOnce<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionTestUtils.assertNoFailureListener(newValue -> {
                        newValue.mustIncRef();
                        response.set(newValue);
                    }),
                    latch
                );
                Set<String> clusterAliases = new HashSet<>(remoteClusterService.getRegisteredRemoteClusterNames());
                if (localIndices != null) {
                    clusterAliases.add("");
                }
                TaskId parentTaskId = new TaskId("n", 1);
                SearchTask task = new SearchTask(2, "search", "search", () -> "desc", parentTaskId, Collections.emptyMap());
                TransportSearchAction.ccsRemoteReduce(
                    task,
                    parentTaskId,
                    searchRequest,
                    mockResolvedIndices,
                    new SearchResponse.Clusters(localIndices, remoteIndicesByCluster, true, alias -> randomBoolean()),
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
                    resolveWithEmptySearchResponse(tuple);
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                try {
                    assertEquals(
                        disconnectedNodesIndices.size(),
                        searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED)
                    );
                    assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                    int successful = totalClusters - disconnectedNodesIndices.size();
                    assertEquals(successful, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.RUNNING));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.FAILED));
                    assertEquals(successful == 0 ? 0 : successful + 1, searchResponse.getNumReducePhases());
                } finally {
                    searchResponse.decRef();
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

            // put the following in assert busy as connections are lazily reestablished
            assertBusy(() -> {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<SearchResponse> response = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionTestUtils.assertNoFailureListener(newValue -> {
                        newValue.mustIncRef();
                        response.set(newValue);
                    }),
                    latch
                );
                Set<String> clusterAliases = new HashSet<>(remoteClusterService.getRegisteredRemoteClusterNames());
                if (localIndices != null) {
                    clusterAliases.add("");
                }
                TaskId parentTaskId = new TaskId("n", 1);
                SearchTask task = new SearchTask(2, "search", "search", () -> "desc", parentTaskId, Collections.emptyMap());
                TransportSearchAction.ccsRemoteReduce(
                    task,
                    parentTaskId,
                    searchRequest,
                    mockResolvedIndices,
                    new SearchResponse.Clusters(localIndices, remoteIndicesByCluster, true, alias -> randomBoolean()),
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
                    resolveWithEmptySearchResponse(tuple);
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                try {
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED));
                    assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                    assertEquals(
                        totalClusters,
                        searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL)
                    );
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.RUNNING));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL));
                    assertEquals(0, searchResponse.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.FAILED));
                    assertEquals(totalClusters == 1 ? 1 : totalClusters + 1, searchResponse.getNumReducePhases());
                } finally {
                    searchResponse.decRef();
                }
            });
            assertEquals(0, service.getConnectionManager().size());
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    private static void resolveWithEmptySearchResponse(Tuple<SearchRequest, ActionListener<SearchResponse>> tuple) {
        ActionListener.respondAndRelease(
            tuple.v2(),
            new SearchResponse(
                SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
                InternalAggregations.EMPTY,
                null,
                false,
                null,
                null,
                1,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY,
                null
            )
        );
    }

    public void testCollectSearchShards() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder, false);
        Settings settings = builder.build();
        try (
            MockTransportService service = MockTransportService.createNewService(
                settings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                null
            )
        ) {
            service.start();
            service.acceptIncomingRequests();

            TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            {
                final CountDownLatch latch = new CountDownLatch(1);
                AtomicReference<Map<String, SearchShardsResponse>> response = new AtomicReference<>();
                var clusters = new SearchResponse.Clusters(null, remoteIndicesByCluster, false, clusterAlias -> true);
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    new MatchAllQueryBuilder(),
                    randomBoolean(),
                    null,
                    remoteIndicesByCluster,
                    clusters,
                    timeProvider,
                    service,
                    new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(response::set), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertNotNull(response.get());
                Map<String, SearchShardsResponse> map = response.get();
                assertEquals(numClusters, map.size());
                for (int i = 0; i < numClusters; i++) {
                    String clusterAlias = "remote" + i;
                    assertTrue(map.containsKey(clusterAlias));
                    SearchShardsResponse shardsResponse = map.get(clusterAlias);
                    assertThat(shardsResponse.getNodes(), hasSize(1));
                }
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            }
            {
                final CountDownLatch latch = new CountDownLatch(1);
                AtomicReference<Exception> failure = new AtomicReference<>();
                var clusters = new SearchResponse.Clusters(null, remoteIndicesByCluster, false, clusterAlias -> true);
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    "index_not_found",
                    null,
                    new MatchAllQueryBuilder(),
                    randomBoolean(),
                    null,
                    remoteIndicesByCluster,
                    clusters,
                    timeProvider,
                    service,
                    new LatchedActionListener<>(ActionListener.wrap(r -> fail("no response expected"), failure::set), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertEquals(numClusters, clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED));
                assertNotNull(failure.get());
                assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                RemoteTransportException remoteTransportException = (RemoteTransportException) failure.get();
                assertEquals(RestStatus.NOT_FOUND, remoteTransportException.status());
            }

            int numDisconnectedClusters = randomIntBetween(1, numClusters);
            Set<DiscoveryNode> disconnectedNodes = Sets.newHashSetWithExpectedSize(numDisconnectedClusters);
            Set<Integer> disconnectedNodesIndices = Sets.newHashSetWithExpectedSize(numDisconnectedClusters);
            while (disconnectedNodes.size() < numDisconnectedClusters) {
                int i = randomIntBetween(0, numClusters - 1);
                if (disconnectedNodes.add(nodes[i])) {
                    assertTrue(disconnectedNodesIndices.add(i));
                }
            }

            CountDownLatch disconnectedLatch = new CountDownLatch(numDisconnectedClusters);
            RemoteClusterServiceTests.addConnectionListener(remoteClusterService, new TransportConnectionListener() {
                @Override
                public void onNodeDisconnected(DiscoveryNode node, @Nullable Exception closeException) {
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
                AtomicReference<Exception> failure = new AtomicReference<>();
                var clusters = new SearchResponse.Clusters(null, remoteIndicesByCluster, false, clusterAlias -> false);
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    new MatchAllQueryBuilder(),
                    randomBoolean(),
                    null,
                    remoteIndicesByCluster,
                    clusters,
                    timeProvider,
                    service,
                    new LatchedActionListener<>(ActionListener.wrap(r -> fail("no response expected"), failure::set), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertEquals(numDisconnectedClusters, clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED));
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
                AtomicReference<Map<String, SearchShardsResponse>> response = new AtomicReference<>();
                var clusters = new SearchResponse.Clusters(null, remoteIndicesByCluster, false, clusterAlias -> true);
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    new MatchAllQueryBuilder(),
                    randomBoolean(),
                    null,
                    remoteIndicesByCluster,
                    clusters,
                    timeProvider,
                    service,
                    new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(response::set), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertNotNull(response.get());
                Map<String, SearchShardsResponse> map = response.get();
                assertEquals(numClusters - disconnectedNodesIndices.size(), map.size());
                assertEquals(disconnectedNodesIndices.size(), clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED));
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
                AtomicReference<Map<String, SearchShardsResponse>> response = new AtomicReference<>();
                var clusters = new SearchResponse.Clusters(null, remoteIndicesByCluster, false, clusterAlias -> true);
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    new MatchAllQueryBuilder(),
                    randomBoolean(),
                    null,
                    remoteIndicesByCluster,
                    clusters,
                    timeProvider,
                    service,
                    new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(response::set), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertEquals(0, clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED));
                assertNotNull(response.get());
                Map<String, SearchShardsResponse> map = response.get();
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
            try (
                SearchResponseMerger merger = TransportSearchAction.createSearchResponseMerger(
                    source,
                    timeProvider,
                    emptyReduceContextBuilder()
                )
            ) {
                assertEquals(0, merger.from);
                assertEquals(10, merger.size);
                assertEquals(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, merger.trackTotalHitsUpTo);
                assertEquals(0, source.from());
                assertEquals(10, source.size());
                assertNull(source.trackTotalHitsUpTo());
            }
        }
        {
            try (
                SearchResponseMerger merger = TransportSearchAction.createSearchResponseMerger(
                    null,
                    timeProvider,
                    emptyReduceContextBuilder()
                )
            ) {
                assertEquals(0, merger.from);
                assertEquals(10, merger.size);
                assertEquals(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, merger.trackTotalHitsUpTo);
            }
        }
        {
            SearchSourceBuilder source = new SearchSourceBuilder();
            int originalFrom = randomIntBetween(0, 1000);
            source.from(originalFrom);
            int originalSize = randomIntBetween(0, 1000);
            source.size(originalSize);
            int trackTotalHitsUpTo = randomIntBetween(0, Integer.MAX_VALUE);
            source.trackTotalHitsUpTo(trackTotalHitsUpTo);
            try (
                SearchResponseMerger merger = TransportSearchAction.createSearchResponseMerger(
                    source,
                    timeProvider,
                    emptyReduceContextBuilder()
                )
            ) {
                assertEquals(0, source.from());
                assertEquals(originalFrom + originalSize, source.size());
                assertEquals(trackTotalHitsUpTo, (int) source.trackTotalHitsUpTo());
                assertEquals(originalFrom, merger.from);
                assertEquals(originalSize, merger.size);
                assertEquals(trackTotalHitsUpTo, merger.trackTotalHitsUpTo);
            }
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
            searchRequest.scroll(TimeValue.timeValueSeconds(5));
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
            searchRequest.scroll(null);
            searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
            SearchSourceBuilder source = searchRequest.source();
            if (source != null) {
                source.pointInTimeBuilder(null);
                source.knnSearch(List.of());
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
        {
            SearchRequest searchRequest = new SearchRequest();
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.knnSearch(List.of(new KnnSearchBuilder("field", new float[] { 1, 2, 3 }, 10, 50, null, null)));
            searchRequest.source(source);

            searchRequest.setCcsMinimizeRoundtrips(true);
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
            searchRequest.setCcsMinimizeRoundtrips(false);
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
    }

    public void testAdjustSearchType() {
        {
            // If the search includes kNN, we should always use DFS_QUERY_THEN_FETCH
            SearchRequest searchRequest = new SearchRequest();
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.knnSearch(List.of(new KnnSearchBuilder("field", new float[] { 1, 2, 3 }, 10, 50, null, null)));
            searchRequest.source(source);

            TransportSearchAction.adjustSearchType(searchRequest, randomBoolean());
            assertEquals(SearchType.DFS_QUERY_THEN_FETCH, searchRequest.searchType());
        }
        {
            // Suggest-only searches should always use QUERY_THEN_FETCH
            SearchRequest searchRequest = new SearchRequest().searchType(RandomPicks.randomFrom(random(), SearchType.values()));
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.suggest(new SuggestBuilder().addSuggestion("field", new TermSuggestionBuilder("value")));
            searchRequest.source(source);

            TransportSearchAction.adjustSearchType(searchRequest, randomBoolean());
            assertFalse(searchRequest.requestCache());
            assertEquals(SearchType.QUERY_THEN_FETCH, searchRequest.searchType());
        }
        {
            // Single-shard searches should always use QUERY_THEN_FETCH in absence of kNN search
            SearchRequest searchRequest = new SearchRequest().searchType(RandomPicks.randomFrom(random(), SearchType.values()));

            TransportSearchAction.adjustSearchType(searchRequest, true);
            assertEquals(SearchType.QUERY_THEN_FETCH, searchRequest.searchType());
        }
    }

    public void testShouldPreFilterSearchShards() {
        int numIndices = randomIntBetween(2, 10);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        final ProjectId projectId = randomProjectIdOrDefault();
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        final ProjectState projectState = clusterState.projectState(projectId);
        {
            SearchRequest searchRequest = new SearchRequest();
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, 128),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
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
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
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
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(DEFAULT_PRE_FILTER_SHARD_SIZE - 1, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp")))
                .scroll(TimeValue.timeValueMinutes(5));
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(DEFAULT_PRE_FILTER_SHARD_SIZE + 1, 10000),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
        }
    }

    public void testShouldPreFilterSearchShardsWithReadOnly() {
        final ProjectId projectId = randomProjectIdOrDefault();
        int numIndices = randomIntBetween(2, 10);
        int numReadOnly = randomIntBetween(1, numIndices);
        String[] indices = new String[numIndices];
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder();
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
            if (--numReadOnly >= 0) {
                if (randomBoolean()) {
                    blocksBuilder.addIndexBlock(projectId, indices[i], IndexMetadata.INDEX_WRITE_BLOCK);
                } else {
                    blocksBuilder.addIndexBlock(projectId, indices[i], IndexMetadata.INDEX_READ_ONLY_BLOCK);
                }
            }
        }
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .blocks(blocksBuilder)
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        final ProjectState projectState = clusterState.projectState(projectId);
        {
            SearchRequest searchRequest = new SearchRequest();
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
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
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
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
            searchRequest.scroll(TimeValue.timeValueSeconds(5));
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
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
                    projectState,
                    searchRequest,
                    indices,
                    randomIntBetween(2, DEFAULT_PRE_FILTER_SHARD_SIZE - 1),
                    DEFAULT_PRE_FILTER_SHARD_SIZE
                )
            );
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(
                    projectState,
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
        final ProjectId project = randomProjectIdOrDefault();
        final ClusterState clusterState = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(
            project,
            indices,
            numberOfShards,
            numberOfReplicas
        );
        final IndexMetadata indexMetadata = clusterState.metadata().getProject(project).index("test-1");
        Map<ShardId, SearchContextIdForNode> contexts = new HashMap<>();
        Set<ShardId> relocatedContexts = new HashSet<>();
        Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            final String targetNode;
            if (randomBoolean()) {
                final IndexRoutingTable routingTable = clusterState.routingTable(project).index(indexMetadata.getIndex());
                targetNode = randomFrom(routingTable.shard(shardId).assignedShards()).currentNodeId();
            } else {
                // relocated or no longer assigned
                relocatedContexts.add(new ShardId(indexMetadata.getIndex(), shardId));
                targetNode = randomFrom(clusterState.nodes().getAllNodes()).getId();
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

        final List<SearchShardIterator> shardIterators = TransportSearchAction.getLocalShardsIteratorFromPointInTime(
            clusterState.projectState(project),
            null,
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
                final List<String> targetNodes = clusterState.routingTable(project)
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
                randomFrom(clusterState.nodes().getAllNodes()).getId(),
                new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong(), null)
            )
        );
        IndexNotFoundException error = expectThrows(IndexNotFoundException.class, () -> {
            TransportSearchAction.getLocalShardsIteratorFromPointInTime(
                clusterState.projectState(project),
                null,
                null,
                new SearchContextId(contexts, aliasFilterMap),
                keepAlive,
                false
            );
        });
        assertThat(error.getIndex().getName(), equalTo("another-index"));
        // Ok when some indices don't exist and `allowPartialSearchResults` is true.
        Optional<SearchShardIterator> anotherShardIterator = TransportSearchAction.getLocalShardsIteratorFromPointInTime(
            clusterState.projectState(project),
            null,
            null,
            new SearchContextId(contexts, aliasFilterMap),
            keepAlive,
            true
        ).stream().filter(si -> si.shardId().equals(anotherShardId)).findFirst();
        assertTrue(anotherShardIterator.isPresent());
        assertThat(anotherShardIterator.get().getTargetNodeIds(), hasSize(0));
    }

    public void testCCSCompatibilityCheck() throws Exception {
        Settings settings = Settings.builder()
            .put("node.name", TransportSearchAction.class.getSimpleName())
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        TransportVersion transportVersion = TransportVersionUtils.getNextVersion(TransportVersions.MINIMUM_CCS_VERSION, true);
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            TransportService transportService = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                transportVersion,
                threadPool
            );

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().query(new DummyQueryBuilder() {
                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {
                    throw new IllegalArgumentException("Not serializable to " + transportVersion);
                }
            }));
            NodeClient client = new NodeClient(settings, threadPool, TestProjectResolvers.alwaysThrow());

            SearchService searchService = mock(SearchService.class);
            when(searchService.getRewriteContext(any(), any(), any(), anyBoolean())).thenReturn(
                new QueryRewriteContext(null, null, null, null, null, null)
            );
            ClusterService clusterService = new ClusterService(
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            clusterService.getClusterApplierService().setInitialState(ClusterState.EMPTY_STATE);

            TransportSearchAction action = new TransportSearchAction(
                threadPool,
                new NoneCircuitBreakerService(),
                transportService,
                searchService,
                null,
                new SearchTransportService(transportService, client, null),
                null,
                clusterService,
                actionFilters,
                TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext()),
                TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                null,
                null,
                new SearchResponseMetrics(TelemetryProvider.NOOP.getMeterRegistry()),
                client,
                new UsageService()
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
                    assertEquals("Not serializable to " + transportVersion, ex.getCause().getMessage());
                    latch.countDown();
                }
            });
            latch.await();
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }
}
