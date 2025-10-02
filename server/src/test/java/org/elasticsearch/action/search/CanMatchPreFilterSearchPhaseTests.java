/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.CanMatchNodeResponse.ResponseOrFailure;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.indices.DateFieldRangeInfo;
import org.elasticsearch.search.CanMatchShardResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static org.elasticsearch.action.search.SearchAsyncActionTests.getShardsIter;
import static org.elasticsearch.core.Types.forciblyCast;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;

public class CanMatchPreFilterSearchPhaseTests extends ESTestCase {

    private final CoordinatorRewriteContextProvider EMPTY_CONTEXT_PROVIDER = new StaticCoordinatorRewriteContextProviderBuilder().build();

    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testFilterShards() throws InterruptedException {

        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        DiscoveryNode replicaNode = randomBoolean() ? null : DiscoveryNodeUtils.create("node_2");
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        final boolean shard2 = randomBoolean();

        final AtomicInteger numRequests = new AtomicInteger();
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(
                Transport.Connection connection,
                CanMatchNodeRequest request,
                SearchTask task,
                ActionListener<CanMatchNodeResponse> listener
            ) {
                numRequests.incrementAndGet();
                final List<ResponseOrFailure> responses = new ArrayList<>();
                for (CanMatchNodeRequest.Shard shard : request.getShardLevelRequests()) {
                    responses.add(new ResponseOrFailure(new CanMatchShardResponse(shard.shardId().id() == 0 ? shard1 : shard2, null)));
                }

                new Thread(() -> listener.onResponse(new CanMatchNodeResponse(responses))).start();
            }
        };

        AtomicReference<List<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<SearchShardIterator> shardsIter = getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            2,
            randomBoolean(),
            primaryNode,
            replicaNode
        );
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);

        CanMatchPreFilterSearchPhase.execute(
            logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", AliasFilter.EMPTY),
            Collections.emptyMap(),
            threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
            searchRequest,
            shardsIter,
            timeProvider,
            null,
            true,
            EMPTY_CONTEXT_PROVIDER
        ).addListener(ActionTestUtils.assertNoFailureListener(iter -> {
            result.set(iter);
            latch.countDown();
        }));
        latch.await();

        assertThat(numRequests.get(), replicaNode == null ? equalTo(1) : lessThanOrEqualTo(2));

        if (shard1 && shard2) {
            for (SearchShardIterator i : result.get()) {
                assertFalse(i.skip());
            }
        } else if (shard1 == false && shard2 == false) {
            assertFalse(result.get().get(0).skip());
            assertTrue(result.get().get(1).skip());
        } else {
            assertEquals(0, result.get().get(0).shardId().id());
            assertEquals(1, result.get().get(1).shardId().id());
            assertEquals(shard1, result.get().get(0).skip() == false);
            assertEquals(shard2, result.get().get(1).skip() == false);
        }
    }

    public void testFilterWithFailure() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node_2");
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        final boolean useReplicas = randomBoolean();
        final boolean fullFailure = randomBoolean();
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(
                Transport.Connection connection,
                CanMatchNodeRequest request,
                SearchTask task,
                ActionListener<CanMatchNodeResponse> listener
            ) {
                if (fullFailure && randomBoolean()) {
                    throw new IllegalArgumentException("boom");
                }
                final List<ResponseOrFailure> responses = new ArrayList<>();
                for (CanMatchNodeRequest.Shard shard : request.getShardLevelRequests()) {
                    boolean throwException = shard.shardId().id() != 0;
                    if (throwException) {
                        responses.add(new ResponseOrFailure(new NullPointerException()));
                    } else {
                        responses.add(new ResponseOrFailure(new CanMatchShardResponse(shard1, null)));
                    }
                }

                new Thread(() -> {
                    if (fullFailure) {
                        listener.onFailure(new NullPointerException());
                    } else {
                        listener.onResponse(new CanMatchNodeResponse(responses));
                    }
                }).start();
            }
        };

        AtomicReference<List<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<SearchShardIterator> shardsIter = getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            2,
            useReplicas,
            primaryNode,
            replicaNode
        );

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);

        CanMatchPreFilterSearchPhase.execute(
            logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", AliasFilter.EMPTY),
            Collections.emptyMap(),
            threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
            searchRequest,
            shardsIter,
            timeProvider,
            null,
            true,
            EMPTY_CONTEXT_PROVIDER
        ).addListener(ActionTestUtils.assertNoFailureListener(iter -> {
            result.set(iter);
            latch.countDown();
        }));

        latch.await();

        assertEquals(0, result.get().get(0).shardId().id());
        assertEquals(1, result.get().get(1).shardId().id());
        if (fullFailure) {
            assertFalse(result.get().get(0).skip()); // never skip the failure
        } else {
            assertEquals(shard1, result.get().get(0).skip() == false);
        }
        assertFalse(result.get().get(1).skip()); // never skip the failure
    }

    public void testSortShards() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node_2");
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        for (SortOrder order : SortOrder.values()) {
            List<ShardId> shardIds = new ArrayList<>();
            List<MinAndMax<?>> minAndMaxes = new ArrayList<>();
            Set<ShardId> shardToSkip = new HashSet<>();

            SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
                @Override
                public void sendCanMatch(
                    Transport.Connection connection,
                    CanMatchNodeRequest request,
                    SearchTask task,
                    ActionListener<CanMatchNodeResponse> listener
                ) {
                    final List<ResponseOrFailure> responses = new ArrayList<>();
                    for (CanMatchNodeRequest.Shard shard : request.getShardLevelRequests()) {
                        Long min = rarely() ? null : randomLong();
                        Long max = min == null ? null : randomLongBetween(min, Long.MAX_VALUE);
                        MinAndMax<?> minMax = min == null ? null : new MinAndMax<>(min, max);
                        boolean canMatch = frequently();
                        synchronized (shardIds) {
                            shardIds.add(shard.shardId());
                            minAndMaxes.add(minMax);
                            if (canMatch == false) {
                                shardToSkip.add(shard.shardId());
                            }
                        }

                        responses.add(new ResponseOrFailure(new CanMatchShardResponse(canMatch, minMax)));
                    }

                    new Thread(() -> listener.onResponse(new CanMatchNodeResponse(responses))).start();
                }
            };

            AtomicReference<List<SearchShardIterator>> result = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            List<SearchShardIterator> shardsIter = getShardsIter(
                "logs",
                new OriginalIndices(new String[] { "logs" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
                randomIntBetween(2, 20),
                randomBoolean(),
                primaryNode,
                replicaNode
            );
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp").order(order)));
            searchRequest.allowPartialSearchResults(true);

            CanMatchPreFilterSearchPhase.execute(
                logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", AliasFilter.EMPTY),
                Collections.emptyMap(),
                threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                searchRequest,
                shardsIter,
                timeProvider,
                null,
                true,
                EMPTY_CONTEXT_PROVIDER
            ).addListener(ActionTestUtils.assertNoFailureListener(iter -> {
                result.set(iter);
                latch.countDown();
            }));
            latch.await();
            ShardId[] expected = IntStream.range(0, shardIds.size())
                .boxed()
                .sorted(Comparator.comparing(minAndMaxes::get, forciblyCast(MinAndMax.getComparator(order))).thenComparing(shardIds::get))
                .map(shardIds::get)
                .toArray(ShardId[]::new);
            if (shardToSkip.size() == expected.length) {
                // we need at least one shard to produce the empty result for aggs
                shardToSkip.remove(new ShardId("logs", "_na_", 0));
            }
            int pos = 0;
            for (SearchShardIterator i : result.get()) {
                assertEquals(shardToSkip.contains(i.shardId()), i.skip());
                assertEquals(expected[pos++], i.shardId());
            }
        }
    }

    public void testInvalidSortShards() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node_2");
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        for (SortOrder order : SortOrder.values()) {
            int numShards = randomIntBetween(2, 20);
            List<ShardId> shardIds = new ArrayList<>();
            Set<ShardId> shardToSkip = new HashSet<>();

            SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
                @Override
                public void sendCanMatch(
                    Transport.Connection connection,
                    CanMatchNodeRequest request,
                    SearchTask task,
                    ActionListener<CanMatchNodeResponse> listener
                ) {
                    final List<ResponseOrFailure> responses = new ArrayList<>();
                    for (CanMatchNodeRequest.Shard shard : request.getShardLevelRequests()) {
                        final MinAndMax<?> minMax;
                        if (shard.shardId().id() == numShards - 1) {
                            minMax = new MinAndMax<>(new BytesRef("bar"), new BytesRef("baz"));
                        } else {
                            Long min = randomLong();
                            Long max = randomLongBetween(min, Long.MAX_VALUE);
                            minMax = new MinAndMax<>(min, max);
                        }
                        boolean canMatch = frequently();
                        synchronized (shardIds) {
                            shardIds.add(shard.shardId());
                            if (canMatch == false) {
                                shardToSkip.add(shard.shardId());
                            }
                        }
                        responses.add(new ResponseOrFailure(new CanMatchShardResponse(canMatch, minMax)));
                    }

                    new Thread(() -> listener.onResponse(new CanMatchNodeResponse(responses))).start();
                }
            };

            AtomicReference<List<SearchShardIterator>> result = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            List<SearchShardIterator> shardsIter = getShardsIter(
                "logs",
                new OriginalIndices(new String[] { "logs" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
                numShards,
                randomBoolean(),
                primaryNode,
                replicaNode
            );
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp").order(order)));
            searchRequest.allowPartialSearchResults(true);

            CanMatchPreFilterSearchPhase.execute(
                logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", AliasFilter.EMPTY),
                Collections.emptyMap(),
                threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                searchRequest,
                shardsIter,
                timeProvider,
                null,
                shardsIter.size() > shardToSkip.size(),
                EMPTY_CONTEXT_PROVIDER
            ).addListener(ActionTestUtils.assertNoFailureListener(iter -> {
                result.set(iter);
                latch.countDown();
            }));

            latch.await();
            int shardId = 0;
            for (SearchShardIterator i : result.get()) {
                assertThat(i.shardId().id(), equalTo(shardId++));
                assertEquals(shardToSkip.contains(i.shardId()), i.skip());
            }
            assertThat(result.get().size(), equalTo(numShards));
        }
    }

    // test using @timestamp
    public void testCanMatchFilteringOnCoordinatorThatCanBeSkippedUsingTimestamp() throws Exception {
        doCanMatchFilteringOnCoordinatorThatCanBeSkipped(DataStream.TIMESTAMP_FIELD_NAME);
    }

    // test using event.ingested
    public void testCanMatchFilteringOnCoordinatorThatCanBeSkippedUsingEventIngested() throws Exception {
        doCanMatchFilteringOnCoordinatorThatCanBeSkipped(IndexMetadata.EVENT_INGESTED_FIELD_NAME);
    }

    public void testCanMatchFilteringOnCoordinatorSkipsBasedOnTier() throws Exception {
        // we'll test that we're executing _tier coordinator rewrite for indices (data stream backing or regular) without any @timestamp
        // or event.ingested fields
        // for both data stream backing and regular indices we'll have one index in hot and one in warm. the warm indices will be skipped as
        // our queries will filter based on _tier: hot

        Map<Index, Settings.Builder> indexNameToSettings = new HashMap<>();
        ClusterState state = ClusterState.EMPTY_STATE;

        String dataStreamName = randomAlphaOfLengthBetween(10, 20);
        Index warmDataStreamIndex = new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1), UUIDs.base64UUID());
        indexNameToSettings.put(
            warmDataStreamIndex,
            settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, warmDataStreamIndex.getUUID())
                .put(DataTier.TIER_PREFERENCE, "data_warm,data_hot")
        );
        Index hotDataStreamIndex = new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 2), UUIDs.base64UUID());
        indexNameToSettings.put(
            hotDataStreamIndex,
            settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, hotDataStreamIndex.getUUID())
                .put(DataTier.TIER_PREFERENCE, "data_hot")
        );
        DataStream dataStream = DataStreamTestHelper.newInstance(dataStreamName, List.of(warmDataStreamIndex, hotDataStreamIndex));

        Index warmRegularIndex = new Index("warm-index", UUIDs.base64UUID());
        indexNameToSettings.put(
            warmRegularIndex,
            settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, warmRegularIndex.getUUID())
                .put(DataTier.TIER_PREFERENCE, "data_warm,data_hot")
        );
        Index hotRegularIndex = new Index("hot-index", UUIDs.base64UUID());
        indexNameToSettings.put(
            hotRegularIndex,
            settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, hotRegularIndex.getUUID())
                .put(DataTier.TIER_PREFERENCE, "data_hot")
        );

        List<Index> allIndices = new ArrayList<>(4);
        allIndices.addAll(dataStream.getIndices());
        allIndices.add(warmRegularIndex);
        allIndices.add(hotRegularIndex);

        List<Index> hotIndices = List.of(hotRegularIndex, hotDataStreamIndex);
        List<Index> warmIndices = List.of(warmRegularIndex, warmDataStreamIndex);

        for (Index index : allIndices) {
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(indexNameToSettings.get(index))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).put(indexMetadataBuilder);
            state = ClusterState.builder(state).metadata(metadataBuilder).build();
        }

        ClusterState finalState = state;
        CoordinatorRewriteContextProvider coordinatorRewriteContextProvider = new CoordinatorRewriteContextProvider(
            parserConfig(),
            mock(Client.class),
            System::currentTimeMillis,
            () -> finalState.projectState(),
            (index) -> null
        );

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(CoordinatorRewriteContext.TIER_FIELD_NAME, "data_hot"));

        assignShardsAndExecuteCanMatchPhase(
            List.of(dataStream),
            List.of(hotRegularIndex, warmRegularIndex),
            coordinatorRewriteContextProvider,
            boolQueryBuilder,
            List.of(),
            null,
            (updatedSearchShardIterators, requests) -> {
                var skippedShards = updatedSearchShardIterators.stream().filter(SearchShardIterator::skip).toList();
                var nonSkippedShards = updatedSearchShardIterators.stream()
                    .filter(searchShardIterator -> searchShardIterator.skip() == false)
                    .toList();

                boolean allSkippedShardAreFromWarmIndices = skippedShards.stream()
                    .allMatch(shardIterator -> warmIndices.contains(shardIterator.shardId().getIndex()));
                assertThat(allSkippedShardAreFromWarmIndices, equalTo(true));
                boolean allNonSkippedShardAreHotIndices = nonSkippedShards.stream()
                    .allMatch(shardIterator -> hotIndices.contains(shardIterator.shardId().getIndex()));
                assertThat(allNonSkippedShardAreHotIndices, equalTo(true));
                boolean allRequestMadeToHotIndices = requests.stream()
                    .allMatch(request -> hotIndices.contains(request.shardId().getIndex()));
                assertThat(allRequestMadeToHotIndices, equalTo(true));
            }
        );
    }

    public void doCanMatchFilteringOnCoordinatorThatCanBeSkipped(String timestampField) throws Exception {
        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream = DataStreamTestHelper.newInstance("mydata", List.of(dataStreamIndex1, dataStreamIndex2));

        List<Index> regularIndices = randomList(0, 2, () -> new Index(randomAlphaOfLength(10), UUIDs.base64UUID()));

        long indexMinTimestamp = randomLongBetween(0, 5000);
        long indexMaxTimestamp = randomLongBetween(indexMinTimestamp, 5000 * 2);
        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProviderBuilder.addIndexMinMaxTimestamps(dataStreamIndex, timestampField, indexMinTimestamp, indexMaxTimestamp);
        }

        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timestampField);
        // We query a range outside of the timestamp range covered by both datastream indices
        rangeQueryBuilder.from(indexMaxTimestamp + 1).to(indexMaxTimestamp + 2);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(rangeQueryBuilder);

        if (randomBoolean()) {
            // Add an additional filter that cannot be evaluated in the coordinator but shouldn't
            // affect the end result as we're filtering
            queryBuilder.filter(new TermQueryBuilder("fake", "value"));
        }

        assignShardsAndExecuteCanMatchPhase(
            List.of(dataStream),
            regularIndices,
            contextProviderBuilder.build(),
            queryBuilder,
            List.of(),
            null,
            (updatedSearchShardIterators, requests) -> {
                List<SearchShardIterator> skippedShards = updatedSearchShardIterators.stream().filter(SearchShardIterator::skip).toList();

                List<SearchShardIterator> nonSkippedShards = updatedSearchShardIterators.stream()
                    .filter(searchShardIterator -> searchShardIterator.skip() == false)
                    .toList();

                int regularIndexShardCount = (int) updatedSearchShardIterators.stream()
                    .filter(s -> regularIndices.contains(s.shardId().getIndex()))
                    .count();

                // When all the shards can be skipped we should query at least 1
                // in order to get a valid search response.
                if (regularIndexShardCount == 0) {
                    assertThat(nonSkippedShards.size(), equalTo(1)); // FIXME - fails here with expected 1 but was 11 OR
                } else {
                    boolean allNonSkippedShardsAreFromRegularIndices = nonSkippedShards.stream()
                        .allMatch(shardIterator -> regularIndices.contains(shardIterator.shardId().getIndex()));

                    assertThat(allNonSkippedShardsAreFromRegularIndices, equalTo(true)); // FIXME - OR fails here with "false"
                }

                boolean allSkippedShardAreFromDataStream = skippedShards.stream()
                    .allMatch(shardIterator -> dataStream.getIndices().contains(shardIterator.shardId().getIndex()));
                assertThat(allSkippedShardAreFromDataStream, equalTo(true));

                boolean allRequestsWereTriggeredAgainstRegularIndices = requests.stream()
                    .allMatch(request -> regularIndices.contains(request.shardId().getIndex()));
                assertThat(allRequestsWereTriggeredAgainstRegularIndices, equalTo(true));
            }
        );
    }

    public void testCoordinatorCanMatchFilteringThatCanBeSkippedUsingBothTimestamps() throws Exception {
        Index dataStreamIndex1 = new Index(".ds-twoTimestamps0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-twoTimestamps0002", UUIDs.base64UUID());
        DataStream dataStream = DataStreamTestHelper.newInstance("mydata", List.of(dataStreamIndex1, dataStreamIndex2));

        List<Index> regularIndices = randomList(1, 2, () -> new Index(randomAlphaOfLength(10), UUIDs.base64UUID()));

        long indexMinTimestamp = randomLongBetween(0, 5000);
        long indexMaxTimestamp = randomLongBetween(indexMinTimestamp, 5000 * 2);
        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            // use same range for both @timestamp and event.ingested
            contextProviderBuilder.addIndexMinMaxForTimestampAndEventIngested(
                dataStreamIndex,
                indexMinTimestamp,
                indexMaxTimestamp,
                indexMinTimestamp,
                indexMaxTimestamp
            );
        }

        /**
         * Expected behavior: if either @timestamp or 'event.ingested' filters in the query are "out of range" (do not
         * overlap the range in cluster state), then all shards in the datastream should be skipped.
         * Only if both @timestamp or 'event.ingested' filters are "in range" should the data stream shards be searched
         */
        boolean timestampQueryOutOfRange = randomBoolean();
        boolean eventIngestedQueryOutOfRange = randomBoolean();
        int timestampOffset = timestampQueryOutOfRange ? 1 : -500;
        int eventIngestedOffset = eventIngestedQueryOutOfRange ? 1 : -500;

        RangeQueryBuilder tsRangeQueryBuilder = new RangeQueryBuilder(DataStream.TIMESTAMP_FIELD_NAME);
        tsRangeQueryBuilder.from(indexMaxTimestamp + timestampOffset).to(indexMaxTimestamp + 2);

        RangeQueryBuilder eventIngestedRangeQueryBuilder = new RangeQueryBuilder(IndexMetadata.EVENT_INGESTED_FIELD_NAME);
        eventIngestedRangeQueryBuilder.from(indexMaxTimestamp + eventIngestedOffset).to(indexMaxTimestamp + 2);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(tsRangeQueryBuilder).filter(eventIngestedRangeQueryBuilder);

        if (randomBoolean()) {
            // Add an additional filter that cannot be evaluated in the coordinator but shouldn't
            // affect the end result as we're filtering
            queryBuilder.filter(new TermQueryBuilder("fake", "value"));
        }

        assignShardsAndExecuteCanMatchPhase(
            List.of(dataStream),
            regularIndices,
            contextProviderBuilder.build(),
            queryBuilder,
            List.of(),
            null,
            (updatedSearchShardIterators, requests) -> {
                List<SearchShardIterator> skippedShards = updatedSearchShardIterators.stream().filter(SearchShardIterator::skip).toList();
                List<SearchShardIterator> nonSkippedShards = updatedSearchShardIterators.stream()
                    .filter(searchShardIterator -> searchShardIterator.skip() == false)
                    .toList();

                if (timestampQueryOutOfRange || eventIngestedQueryOutOfRange) {
                    // data stream shards should have been skipped
                    assertThat(skippedShards.size(), greaterThan(0));
                    boolean allSkippedShardAreFromDataStream = skippedShards.stream()
                        .allMatch(shardIterator -> dataStream.getIndices().contains(shardIterator.shardId().getIndex()));
                    assertThat(allSkippedShardAreFromDataStream, equalTo(true));

                    boolean allNonSkippedShardsAreFromRegularIndices = nonSkippedShards.stream()
                        .allMatch(shardIterator -> regularIndices.contains(shardIterator.shardId().getIndex()));
                    assertThat(allNonSkippedShardsAreFromRegularIndices, equalTo(true));

                    boolean allRequestsWereTriggeredAgainstRegularIndices = requests.stream()
                        .allMatch(request -> regularIndices.contains(request.shardId().getIndex()));
                    assertThat(allRequestsWereTriggeredAgainstRegularIndices, equalTo(true));

                } else {
                    assertThat(skippedShards.size(), equalTo(0));
                    long countSkippedShardsFromDatastream = nonSkippedShards.stream()
                        .filter(iter -> dataStream.getIndices().contains(iter.shardId().getIndex()))
                        .count();
                    assertThat(countSkippedShardsFromDatastream, greaterThan(0L));
                }
            }
        );
    }

    public void testCanMatchFilteringOnCoordinatorParsingFails() throws Exception {
        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream = DataStreamTestHelper.newInstance("mydata", List.of(dataStreamIndex1, dataStreamIndex2));

        List<Index> regularIndices = randomList(0, 2, () -> new Index(randomAlphaOfLength(10), UUIDs.base64UUID()));

        String timeField = randomFrom(DataStream.TIMESTAMP_FIELD_NAME, IndexMetadata.EVENT_INGESTED_FIELD_NAME);

        long indexMinTimestamp = randomLongBetween(0, 5000);
        long indexMaxTimestamp = randomLongBetween(indexMinTimestamp, 5000 * 2);
        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProviderBuilder.addIndexMinMaxTimestamps(dataStreamIndex, timeField, indexMinTimestamp, indexMaxTimestamp);
        }

        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timeField);
        // Query with a non default date format
        rangeQueryBuilder.from("2020-1-01").to("2021-1-01");

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(rangeQueryBuilder);

        if (randomBoolean()) {
            // Add an additional filter that cannot be evaluated in the coordinator but shouldn't
            // affect the end result as we're filtering
            queryBuilder.filter(new TermQueryBuilder("fake", "value"));
        }

        assignShardsAndExecuteCanMatchPhase(
            List.of(dataStream),
            regularIndices,
            contextProviderBuilder.build(),
            queryBuilder,
            List.of(),
            null,
            this::assertAllShardsAreQueried
        );
    }

    public void testCanMatchFilteringOnCoordinatorThatCanNotBeSkipped() throws Exception {
        // Generate indices
        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream = DataStreamTestHelper.newInstance("mydata", List.of(dataStreamIndex1, dataStreamIndex2));

        List<Index> regularIndices = randomList(0, 2, () -> new Index(randomAlphaOfLength(10), UUIDs.base64UUID()));

        String timeField = randomFrom(DataStream.TIMESTAMP_FIELD_NAME, IndexMetadata.EVENT_INGESTED_FIELD_NAME);

        long indexMinTimestamp = 10;
        long indexMaxTimestamp = 20;
        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProviderBuilder.addIndexMinMaxTimestamps(dataStreamIndex, timeField, indexMinTimestamp, indexMaxTimestamp);
        }

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        // Query inside of the data stream index range
        if (randomBoolean()) {
            // Query generation
            RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timeField);
            // We query a range within the timestamp range covered by both datastream indices
            rangeQueryBuilder.from(indexMinTimestamp).to(indexMaxTimestamp);

            queryBuilder.filter(rangeQueryBuilder);

            if (randomBoolean()) {
                // Add an additional filter that cannot be evaluated in the coordinator but shouldn't
                // affect the end result as we're filtering
                queryBuilder.filter(new TermQueryBuilder("fake", "value"));
            }
        } else {
            // We query a range outside of the timestamp range covered by both datastream indices
            RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timeField).from(indexMaxTimestamp + 1).to(indexMaxTimestamp + 2);

            TermQueryBuilder termQueryBuilder = new TermQueryBuilder("fake", "value");

            // This is always evaluated as true in the coordinator as we cannot determine there if
            // the term query clause is false.
            queryBuilder.should(rangeQueryBuilder).should(termQueryBuilder);
        }

        assignShardsAndExecuteCanMatchPhase(
            List.of(dataStream),
            regularIndices,
            contextProviderBuilder.build(),
            queryBuilder,
            List.of(),
            null,
            this::assertAllShardsAreQueried
        );
    }

    public void testCanMatchFilteringOnCoordinatorWithTimestampAndEventIngestedThatCanNotBeSkipped() throws Exception {
        // Generate indices
        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream = DataStreamTestHelper.newInstance("mydata", List.of(dataStreamIndex1, dataStreamIndex2));

        List<Index> regularIndices = randomList(0, 2, () -> new Index(randomAlphaOfLength(10), UUIDs.base64UUID()));

        long indexMinTimestampForTs = 10;
        long indexMaxTimestampForTs = 20;
        long indexMinTimestampForEventIngested = 10;
        long indexMaxTimestampForEventIngested = 20;
        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProviderBuilder.addIndexMinMaxForTimestampAndEventIngested(
                dataStreamIndex,
                indexMinTimestampForTs,
                indexMaxTimestampForTs,
                indexMinTimestampForEventIngested,
                indexMaxTimestampForEventIngested
            );
        }

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        // Query inside of the data stream index range
        if (randomBoolean()) {
            // Query generation
            // We query a range within both timestamp ranges covered by both datastream indices
            RangeQueryBuilder tsRangeQueryBuilder = new RangeQueryBuilder(DataStream.TIMESTAMP_FIELD_NAME);
            tsRangeQueryBuilder.from(indexMinTimestampForTs).to(indexMaxTimestampForTs);

            RangeQueryBuilder eventIngestedRangeQueryBuilder = new RangeQueryBuilder(IndexMetadata.EVENT_INGESTED_FIELD_NAME);
            eventIngestedRangeQueryBuilder.from(indexMinTimestampForEventIngested).to(indexMaxTimestampForEventIngested);

            queryBuilder.filter(tsRangeQueryBuilder).filter(eventIngestedRangeQueryBuilder);

            if (randomBoolean()) {
                // Add an additional filter that cannot be evaluated in the coordinator but shouldn't
                // affect the end result as we're filtering
                queryBuilder.filter(new TermQueryBuilder("fake", "value"));
            }
        } else {
            // We query a range outside of the both ranges covered by both datastream indices
            RangeQueryBuilder tsRangeQueryBuilder = new RangeQueryBuilder(DataStream.TIMESTAMP_FIELD_NAME).from(indexMaxTimestampForTs + 1)
                .to(indexMaxTimestampForTs + 2);
            RangeQueryBuilder eventIngestedRangeQueryBuilder = new RangeQueryBuilder(IndexMetadata.EVENT_INGESTED_FIELD_NAME).from(
                indexMaxTimestampForEventIngested + 1
            ).to(indexMaxTimestampForEventIngested + 2);

            TermQueryBuilder termQueryBuilder = new TermQueryBuilder("fake", "value");

            // This is always evaluated as true in the coordinator as we cannot determine there if
            // the term query clause is false.
            queryBuilder.should(tsRangeQueryBuilder).should(eventIngestedRangeQueryBuilder).should(termQueryBuilder);
        }

        assignShardsAndExecuteCanMatchPhase(
            List.of(dataStream),
            regularIndices,
            contextProviderBuilder.build(),
            queryBuilder,
            List.of(),
            null,
            this::assertAllShardsAreQueried
        );
    }

    public void testCanMatchFilteringOnCoordinator_withSignificantTermsAggregation_withDefaultBackgroundFilter() throws Exception {
        Index index1 = new Index("index1", UUIDs.base64UUID());
        Index index2 = new Index("index2", UUIDs.base64UUID());
        Index index3 = new Index("index3", UUIDs.base64UUID());

        String timeField = randomFrom(DataStream.TIMESTAMP_FIELD_NAME, IndexMetadata.EVENT_INGESTED_FIELD_NAME);

        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        contextProviderBuilder.addIndexMinMaxTimestamps(index1, timeField, 0, 999);
        contextProviderBuilder.addIndexMinMaxTimestamps(index2, timeField, 1000, 1999);
        contextProviderBuilder.addIndexMinMaxTimestamps(index3, timeField, 2000, 2999);

        QueryBuilder query = new BoolQueryBuilder().filter(new RangeQueryBuilder(timeField).from(2100).to(2200));
        AggregationBuilder aggregation = new SignificantTermsAggregationBuilder("significant_terms");

        assignShardsAndExecuteCanMatchPhase(
            List.of(),
            List.of(index1, index2, index3),
            contextProviderBuilder.build(),
            query,
            List.of(aggregation),
            null,
            // The default background filter matches the whole index, so all shards must be queried.
            this::assertAllShardsAreQueried
        );
    }

    public void testCanMatchFilteringOnCoordinator_withSignificantTermsAggregation_withBackgroundFilter() throws Exception {
        String timestampField = randomFrom(IndexMetadata.EVENT_INGESTED_FIELD_NAME, DataStream.TIMESTAMP_FIELD_NAME);

        Index index1 = new Index("index1", UUIDs.base64UUID());
        Index index2 = new Index("index2", UUIDs.base64UUID());
        Index index3 = new Index("index3", UUIDs.base64UUID());
        Index index4 = new Index("index4", UUIDs.base64UUID());

        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        contextProviderBuilder.addIndexMinMaxTimestamps(index1, timestampField, 0, 999);
        contextProviderBuilder.addIndexMinMaxTimestamps(index2, timestampField, 1000, 1999);
        contextProviderBuilder.addIndexMinMaxTimestamps(index3, timestampField, 2000, 2999);
        contextProviderBuilder.addIndexMinMaxTimestamps(index4, timestampField, 3000, 3999);

        QueryBuilder query = new BoolQueryBuilder().filter(new RangeQueryBuilder(timestampField).from(3100).to(3200));
        AggregationBuilder aggregation = new SignificantTermsAggregationBuilder("significant_terms").backgroundFilter(
            new RangeQueryBuilder(timestampField).from(0).to(1999)
        );

        assignShardsAndExecuteCanMatchPhase(
            List.of(),
            List.of(index1, index2, index3),
            contextProviderBuilder.build(),
            query,
            List.of(aggregation),
            null,
            (updatedSearchShardIterators, requests) -> {
                // The search query matches index4, the background query matches index1 and index2,
                // so index3 is the only one that must be skipped.
                for (SearchShardIterator shard : updatedSearchShardIterators) {
                    if (shard.shardId().getIndex().getName().equals("index3")) {
                        assertTrue(shard.skip());
                    } else {
                        assertFalse(shard.skip());
                    }
                }
            }
        );
    }

    public void testCanMatchFilteringOnCoordinator_withSignificantTermsAggregation_withSuggest() throws Exception {
        Index index1 = new Index("index1", UUIDs.base64UUID());
        Index index2 = new Index("index2", UUIDs.base64UUID());
        Index index3 = new Index("index3", UUIDs.base64UUID());

        String timestampField = randomFrom(IndexMetadata.EVENT_INGESTED_FIELD_NAME, DataStream.TIMESTAMP_FIELD_NAME);

        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        contextProviderBuilder.addIndexMinMaxTimestamps(index1, timestampField, 0, 999);
        contextProviderBuilder.addIndexMinMaxTimestamps(index2, timestampField, 1000, 1999);
        contextProviderBuilder.addIndexMinMaxTimestamps(index3, timestampField, 2000, 2999);

        QueryBuilder query = new BoolQueryBuilder().filter(new RangeQueryBuilder(timestampField).from(2100).to(2200));
        AggregationBuilder aggregation = new SignificantTermsAggregationBuilder("significant_terms").backgroundFilter(
            new RangeQueryBuilder(timestampField).from(2000).to(2300)
        );
        SuggestBuilder suggest = new SuggestBuilder().setGlobalText("test");

        assignShardsAndExecuteCanMatchPhase(
            List.of(),
            List.of(index1, index2, index3),
            contextProviderBuilder.build(),
            query,
            List.of(aggregation),
            suggest,
            // The query and aggregation and match only index3, but suggest should match everything.
            this::assertAllShardsAreQueried
        );
    }

    public void testCanMatchFilteringOnCoordinator_withSignificantTermsAggregation_withSuggest_withTwoTimestamps() throws Exception {
        Index index1 = new Index("index1", UUIDs.base64UUID());
        Index index2 = new Index("index2", UUIDs.base64UUID());
        Index index3 = new Index("index3", UUIDs.base64UUID());

        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        contextProviderBuilder.addIndexMinMaxForTimestampAndEventIngested(index1, 0, 999, 0, 999);
        contextProviderBuilder.addIndexMinMaxForTimestampAndEventIngested(index2, 1000, 1999, 1000, 1999);
        contextProviderBuilder.addIndexMinMaxForTimestampAndEventIngested(index3, 2000, 2999, 2000, 2999);

        String fieldInRange = IndexMetadata.EVENT_INGESTED_FIELD_NAME;
        String fieldOutOfRange = DataStream.TIMESTAMP_FIELD_NAME;

        if (randomBoolean()) {
            fieldInRange = DataStream.TIMESTAMP_FIELD_NAME;
            fieldOutOfRange = IndexMetadata.EVENT_INGESTED_FIELD_NAME;
        }

        QueryBuilder query = new BoolQueryBuilder().filter(new RangeQueryBuilder(fieldInRange).from(2100).to(2200))
            .filter(new RangeQueryBuilder(fieldOutOfRange).from(8888).to(9999));
        AggregationBuilder aggregation = new SignificantTermsAggregationBuilder("significant_terms").backgroundFilter(
            new RangeQueryBuilder(fieldInRange).from(2000).to(2300)
        );
        SuggestBuilder suggest = new SuggestBuilder().setGlobalText("test");

        assignShardsAndExecuteCanMatchPhase(
            List.of(),
            List.of(index1, index2, index3),
            contextProviderBuilder.build(),
            query,
            List.of(aggregation),
            suggest,
            // The query and aggregation and match only index3, but suggest should match everything.
            this::assertAllShardsAreQueried
        );
    }

    public void testCanMatchFilteringOnCoordinatorThatCanBeSkippedTsdb() throws Exception {
        DataStream dataStream1;
        {
            Index index1 = new Index(".ds-ds10001", UUIDs.base64UUID());
            Index index2 = new Index(".ds-ds10002", UUIDs.base64UUID());
            dataStream1 = DataStreamTestHelper.newInstance("ds1", List.of(index1, index2));
        }
        DataStream dataStream2;
        {
            Index index1 = new Index(".ds-ds20001", UUIDs.base64UUID());
            Index index2 = new Index(".ds-ds20002", UUIDs.base64UUID());
            dataStream2 = DataStreamTestHelper.newInstance("ds2", List.of(index1, index2));
        }

        long indexMinTimestamp = randomLongBetween(0, 5000);
        long indexMaxTimestamp = randomLongBetween(indexMinTimestamp, 5000 * 2);
        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        for (Index index : dataStream1.getIndices()) {
            contextProviderBuilder.addIndexMinMaxTimestamps(index, DataStream.TIMESTAMP_FIELD_NAME, indexMinTimestamp, indexMaxTimestamp);
        }
        for (Index index : dataStream2.getIndices()) {
            contextProviderBuilder.addIndex(index);
        }

        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(DataStream.TIMESTAMP_FIELD_NAME);
        // We query a range outside of the timestamp range covered by both datastream indices
        rangeQueryBuilder.from(indexMaxTimestamp + 1).to(indexMaxTimestamp + 2);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(rangeQueryBuilder);

        if (randomBoolean()) {
            // Add an additional filter that cannot be evaluated in the coordinator but shouldn't
            // affect the end result as we're filtering
            queryBuilder.filter(new TermQueryBuilder("fake", "value"));
        }

        assignShardsAndExecuteCanMatchPhase(
            List.of(dataStream1, dataStream2),
            List.of(),
            contextProviderBuilder.build(),
            queryBuilder,
            List.of(),
            null,
            (updatedSearchShardIterators, requests) -> {
                var skippedShards = updatedSearchShardIterators.stream().filter(SearchShardIterator::skip).toList();
                var nonSkippedShards = updatedSearchShardIterators.stream()
                    .filter(searchShardIterator -> searchShardIterator.skip() == false)
                    .toList();

                boolean allSkippedShardAreFromDataStream1 = skippedShards.stream()
                    .allMatch(shardIterator -> dataStream1.getIndices().contains(shardIterator.shardId().getIndex()));
                assertThat(allSkippedShardAreFromDataStream1, equalTo(true));
                boolean allNonSkippedShardAreFromDataStream1 = nonSkippedShards.stream()
                    .noneMatch(shardIterator -> dataStream1.getIndices().contains(shardIterator.shardId().getIndex()));
                assertThat(allNonSkippedShardAreFromDataStream1, equalTo(true));
                boolean allRequestMadeToDataStream1 = requests.stream()
                    .allMatch(request -> dataStream1.getIndices().contains(request.shardId().getIndex()));
                assertThat(allRequestMadeToDataStream1, equalTo(false));

                boolean allSkippedShardAreFromDataStream2 = skippedShards.stream()
                    .allMatch(shardIterator -> dataStream2.getIndices().contains(shardIterator.shardId().getIndex()));
                assertThat(allSkippedShardAreFromDataStream2, equalTo(false));
                boolean allNonSkippedShardAreFromDataStream2 = nonSkippedShards.stream()
                    .noneMatch(shardIterator -> dataStream2.getIndices().contains(shardIterator.shardId().getIndex()));
                assertThat(allNonSkippedShardAreFromDataStream2, equalTo(false));
                boolean allRequestMadeToDataStream2 = requests.stream()
                    .allMatch(request -> dataStream2.getIndices().contains(request.shardId().getIndex()));
                assertThat(allRequestMadeToDataStream2, equalTo(true));
            }
        );
    }

    public void testCanMatchFilteringOnCoordinatorWithMissingShards() throws Exception {
        // we'll test that we're executing _tier coordinator rewrite for indices (data stream backing or regular) without any @timestamp
        // or event.ingested fields
        // for both data stream backing and regular indices we'll have one index in hot and one UNASSIGNED (targeting warm though).
        // the warm indices will be skipped as our queries will filter based on _tier: hot and the can match phase will not report error the
        // missing index even if allow_partial_search_results is false (because the warm index would've not been part of the search anyway)

        Map<Index, Settings.Builder> indexNameToSettings = new HashMap<>();
        ClusterState state = ClusterState.EMPTY_STATE;

        String dataStreamName = randomAlphaOfLengthBetween(10, 20);
        Index warmDataStreamIndex = new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1), UUIDs.base64UUID());
        indexNameToSettings.put(
            warmDataStreamIndex,
            settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, warmDataStreamIndex.getUUID())
                .put(DataTier.TIER_PREFERENCE, "data_warm,data_hot")
        );
        Index hotDataStreamIndex = new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 2), UUIDs.base64UUID());
        indexNameToSettings.put(
            hotDataStreamIndex,
            settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, hotDataStreamIndex.getUUID())
                .put(DataTier.TIER_PREFERENCE, "data_hot")
        );
        DataStream dataStream = DataStreamTestHelper.newInstance(dataStreamName, List.of(warmDataStreamIndex, hotDataStreamIndex));

        Index warmRegularIndex = new Index("warm-index", UUIDs.base64UUID());
        indexNameToSettings.put(
            warmRegularIndex,
            settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, warmRegularIndex.getUUID())
                .put(DataTier.TIER_PREFERENCE, "data_warm,data_hot")
        );
        Index hotRegularIndex = new Index("hot-index", UUIDs.base64UUID());
        indexNameToSettings.put(
            hotRegularIndex,
            settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, hotRegularIndex.getUUID())
                .put(DataTier.TIER_PREFERENCE, "data_hot")
        );

        List<Index> allIndices = new ArrayList<>(4);
        allIndices.addAll(dataStream.getIndices());
        allIndices.add(warmRegularIndex);
        allIndices.add(hotRegularIndex);

        List<Index> hotIndices = List.of(hotRegularIndex, hotDataStreamIndex);
        List<Index> warmIndices = List.of(warmRegularIndex, warmDataStreamIndex);

        for (Index index : allIndices) {
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(indexNameToSettings.get(index))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).put(indexMetadataBuilder);
            state = ClusterState.builder(state).metadata(metadataBuilder).build();
        }

        ClusterState finalState = state;
        CoordinatorRewriteContextProvider coordinatorRewriteContextProvider = new CoordinatorRewriteContextProvider(
            parserConfig(),
            mock(Client.class),
            System::currentTimeMillis,
            () -> finalState.projectState(),
            (index) -> null
        );

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(CoordinatorRewriteContext.TIER_FIELD_NAME, "data_hot"));

        {
            // test that a search doesn't fail if the query filters out the unassigned shards
            // via _tier (coordinator rewrite will eliminate the shards that don't match)
            assignShardsAndExecuteCanMatchPhase(
                List.of(dataStream),
                List.of(hotRegularIndex, warmRegularIndex),
                coordinatorRewriteContextProvider,
                boolQueryBuilder,
                List.of(),
                null,
                warmIndices,
                false,
                (updatedSearchShardIterators, requests) -> {
                    var skippedShards = updatedSearchShardIterators.stream().filter(SearchShardIterator::skip).toList();
                    var nonSkippedShards = updatedSearchShardIterators.stream()
                        .filter(searchShardIterator -> searchShardIterator.skip() == false)
                        .toList();

                    boolean allSkippedShardAreFromWarmIndices = skippedShards.stream()
                        .allMatch(shardIterator -> warmIndices.contains(shardIterator.shardId().getIndex()));
                    assertThat(allSkippedShardAreFromWarmIndices, equalTo(true));
                    boolean allNonSkippedShardAreHotIndices = nonSkippedShards.stream()
                        .allMatch(shardIterator -> hotIndices.contains(shardIterator.shardId().getIndex()));
                    assertThat(allNonSkippedShardAreHotIndices, equalTo(true));
                    boolean allRequestMadeToHotIndices = requests.stream()
                        .allMatch(request -> hotIndices.contains(request.shardId().getIndex()));
                    assertThat(allRequestMadeToHotIndices, equalTo(true));
                }
            );
        }

        {
            // test that a search does fail if the query does NOT filter ALL the
            // unassigned shards
            CountDownLatch latch = new CountDownLatch(1);
            Tuple<SubscribableListener<List<SearchShardIterator>>, List<ShardSearchRequest>> canMatchPhaseAndRequests =
                getCanMatchPhaseAndRequests(
                    List.of(dataStream),
                    List.of(hotRegularIndex, warmRegularIndex),
                    coordinatorRewriteContextProvider,
                    boolQueryBuilder,
                    List.of(),
                    null,
                    List.of(hotRegularIndex, warmRegularIndex, warmDataStreamIndex),
                    false
                );

            canMatchPhaseAndRequests.v1().addListener(new ActionListener<>() {
                @Override
                public void onResponse(List<SearchShardIterator> searchShardIterators) {
                    fail(null, "unexpected success with result [%s] while expecting to handle failure with [%s]", searchShardIterators);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(SearchPhaseExecutionException.class));
                    latch.countDown();
                }
            });
            latch.await(10, TimeUnit.SECONDS);
        }
    }

    private void assertAllShardsAreQueried(List<SearchShardIterator> updatedSearchShardIterators, List<ShardSearchRequest> requests) {
        int skippedShards = (int) updatedSearchShardIterators.stream().filter(SearchShardIterator::skip).count();

        assertThat(skippedShards, equalTo(0));

        int nonSkippedShards = (int) updatedSearchShardIterators.stream()
            .filter(searchShardIterator -> searchShardIterator.skip() == false)
            .count();

        assertThat(nonSkippedShards, equalTo(updatedSearchShardIterators.size()));

        int shardsWithPrimariesAssigned = (int) updatedSearchShardIterators.stream().filter(s -> s.size() > 0).count();
        assertThat(requests.size(), equalTo(shardsWithPrimariesAssigned));
    }

    private void assignShardsAndExecuteCanMatchPhase(
        List<DataStream> dataStreams,
        List<Index> regularIndices,
        CoordinatorRewriteContextProvider contextProvider,
        QueryBuilder query,
        List<AggregationBuilder> aggregations,
        SuggestBuilder suggest,
        BiConsumer<List<SearchShardIterator>, List<ShardSearchRequest>> canMatchResultsConsumer
    ) throws Exception {
        assignShardsAndExecuteCanMatchPhase(
            dataStreams,
            regularIndices,
            contextProvider,
            query,
            aggregations,
            suggest,
            List.of(),
            true,
            canMatchResultsConsumer
        );
    }

    private void assignShardsAndExecuteCanMatchPhase(
        List<DataStream> dataStreams,
        List<Index> regularIndices,
        CoordinatorRewriteContextProvider contextProvider,
        QueryBuilder query,
        List<AggregationBuilder> aggregations,
        SuggestBuilder suggest,
        List<Index> unassignedIndices,
        boolean allowPartialResults,
        BiConsumer<List<SearchShardIterator>, List<ShardSearchRequest>> canMatchResultsConsumer
    ) throws Exception {
        AtomicReference<List<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Tuple<SubscribableListener<List<SearchShardIterator>>, List<ShardSearchRequest>> canMatchAndShardRequests =
            getCanMatchPhaseAndRequests(
                dataStreams,
                regularIndices,
                contextProvider,
                query,
                aggregations,
                suggest,
                unassignedIndices,
                allowPartialResults
            );

        canMatchAndShardRequests.v1().addListener(ActionTestUtils.assertNoFailureListener(iter -> {
            result.set(iter);
            latch.countDown();
        }));
        latch.await();

        List<SearchShardIterator> updatedSearchShardIterators = new ArrayList<>();
        for (SearchShardIterator updatedSearchShardIterator : result.get()) {
            updatedSearchShardIterators.add(updatedSearchShardIterator);
        }

        canMatchResultsConsumer.accept(updatedSearchShardIterators, canMatchAndShardRequests.v2());
    }

    private Tuple<SubscribableListener<List<SearchShardIterator>>, List<ShardSearchRequest>> getCanMatchPhaseAndRequests(
        List<DataStream> dataStreams,
        List<Index> regularIndices,
        CoordinatorRewriteContextProvider contextProvider,
        QueryBuilder query,
        List<AggregationBuilder> aggregations,
        SuggestBuilder suggest,
        List<Index> unassignedIndices,
        boolean allowPartialResults
    ) {
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node_2");
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        List<String> indicesToSearch = new ArrayList<>();
        for (DataStream dataStream : dataStreams) {
            indicesToSearch.add(dataStream.getName());
        }
        for (Index regularIndex : regularIndices) {
            indicesToSearch.add(regularIndex.getName());
        }

        String[] indices = indicesToSearch.toArray(new String[0]);
        OriginalIndices originalIndices = new OriginalIndices(indices, SearchRequest.DEFAULT_INDICES_OPTIONS);

        final List<SearchShardIterator> shardIters = new ArrayList<>();
        for (var dataStream : dataStreams) {
            boolean atLeastOnePrimaryAssigned = false;
            for (var dataStreamIndex : dataStream.getIndices()) {
                // If we have to execute the can match request against all the shards
                // and none is assigned, the phase is considered as failed meaning that the next phase won't be executed
                boolean withAssignedPrimaries = randomBoolean() || atLeastOnePrimaryAssigned == false;
                int numShards = randomIntBetween(1, 6);
                if (unassignedIndices.contains(dataStreamIndex)) {
                    shardIters.addAll(getShardsIter(dataStreamIndex, originalIndices, numShards, false, null, null));
                } else {
                    shardIters.addAll(
                        getShardsIter(dataStreamIndex, originalIndices, numShards, false, withAssignedPrimaries ? primaryNode : null, null)
                    );
                    atLeastOnePrimaryAssigned |= withAssignedPrimaries;
                }
            }
        }

        for (Index regularIndex : regularIndices) {
            if (unassignedIndices.contains(regularIndex)) {
                shardIters.addAll(getShardsIter(regularIndex, originalIndices, randomIntBetween(1, 6), false, null, null));
            } else {
                shardIters.addAll(
                    getShardsIter(regularIndex, originalIndices, randomIntBetween(1, 6), randomBoolean(), primaryNode, replicaNode)
                );
            }
        }
        CollectionUtil.timSort(shardIters);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indices);
        searchRequest.allowPartialSearchResults(allowPartialResults);

        final AliasFilter aliasFilter;
        if (aggregations.isEmpty() == false || randomBoolean()) {
            // Apply the query on the request body
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
            searchSourceBuilder.query(query);
            for (AggregationBuilder aggregation : aggregations) {
                searchSourceBuilder.aggregation(aggregation);
            }
            if (suggest != null) {
                searchSourceBuilder.suggest(suggest);
            }
            searchRequest.source(searchSourceBuilder);

            // Sometimes apply the same query in the alias filter too
            aliasFilter = AliasFilter.of(aggregations.isEmpty() && randomBoolean() ? query : null, Strings.EMPTY_ARRAY);
        } else {
            // Apply the query as an alias filter
            aliasFilter = AliasFilter.of(query, Strings.EMPTY_ARRAY);
        }

        Map<String, AliasFilter> aliasFilters = new HashMap<>();
        for (var dataStream : dataStreams) {
            for (var dataStreamIndex : dataStream.getIndices()) {
                aliasFilters.put(dataStreamIndex.getUUID(), aliasFilter);
            }
        }

        for (Index regularIndex : regularIndices) {
            aliasFilters.put(regularIndex.getUUID(), aliasFilter);
        }

        // We respond by default that the query can match
        final List<ShardSearchRequest> requests = Collections.synchronizedList(new ArrayList<>());
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(
                Transport.Connection connection,
                CanMatchNodeRequest request,
                SearchTask task,
                ActionListener<CanMatchNodeResponse> listener
            ) {
                final List<ResponseOrFailure> responses = new ArrayList<>();
                for (CanMatchNodeRequest.Shard shard : request.getShardLevelRequests()) {
                    requests.add(request.createShardSearchRequest(shard));
                    responses.add(new ResponseOrFailure(new CanMatchShardResponse(true, null)));
                }

                new Thread(() -> listener.onResponse(new CanMatchNodeResponse(responses))).start();
            }
        };

        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );

        return new Tuple<>(
            CanMatchPreFilterSearchPhase.execute(
                logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                aliasFilters,
                Collections.emptyMap(),
                threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                searchRequest,
                shardIters,
                timeProvider,
                null,
                true,
                contextProvider
            ),
            requests
        );
    }

    static class StaticCoordinatorRewriteContextProviderBuilder {
        private ClusterState clusterState = ClusterState.EMPTY_STATE;
        private final Map<Index, DateFieldRangeInfo> fields = new HashMap<>();

        private void addIndexMinMaxTimestamps(Index index, String fieldName, long minTimeStamp, long maxTimestamp) {
            if (clusterState.metadata().getProject().index(index) != null) {
                throw new IllegalArgumentException("Min/Max timestamps for " + index + " were already defined");
            }

            IndexLongFieldRange timestampRange = IndexLongFieldRange.NO_SHARDS.extendWithShardRange(
                0,
                1,
                ShardLongFieldRange.of(minTimeStamp, maxTimestamp)
            );

            Settings.Builder indexSettings = settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());

            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0);
            if (fieldName.equals(DataStream.TIMESTAMP_FIELD_NAME)) {
                indexMetadataBuilder.timestampRange(timestampRange);
                fields.put(index, new DateFieldRangeInfo(new DateFieldMapper.DateFieldType(fieldName), null, null, null));
            } else if (fieldName.equals(IndexMetadata.EVENT_INGESTED_FIELD_NAME)) {
                indexMetadataBuilder.eventIngestedRange(timestampRange);
                fields.put(index, new DateFieldRangeInfo(null, null, new DateFieldMapper.DateFieldType(fieldName), null));
            }

            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata()).put(indexMetadataBuilder);
            clusterState = ClusterState.builder(clusterState).metadata(metadataBuilder).build();
        }

        /**
         * Add min/max timestamps to IndexMetadata for the specified index for both @timestamp and 'event.ingested'
         */
        private void addIndexMinMaxForTimestampAndEventIngested(
            Index index,
            long minTimestampForTs,
            long maxTimestampForTs,
            long minTimestampForEventIngested,
            long maxTimestampForEventIngested
        ) {
            if (clusterState.metadata().getProject().index(index) != null) {
                throw new IllegalArgumentException("Min/Max timestamps for " + index + " were already defined");
            }

            IndexLongFieldRange tsTimestampRange = IndexLongFieldRange.NO_SHARDS.extendWithShardRange(
                0,
                1,
                ShardLongFieldRange.of(minTimestampForTs, maxTimestampForTs)
            );
            IndexLongFieldRange eventIngestedTimestampRange = IndexLongFieldRange.NO_SHARDS.extendWithShardRange(
                0,
                1,
                ShardLongFieldRange.of(minTimestampForEventIngested, maxTimestampForEventIngested)
            );

            Settings.Builder indexSettings = settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());

            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .timestampRange(tsTimestampRange)
                .eventIngestedRange(eventIngestedTimestampRange);

            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata()).put(indexMetadataBuilder);
            clusterState = ClusterState.builder(clusterState).metadata(metadataBuilder).build();
            fields.put(
                index,
                new DateFieldRangeInfo(
                    new DateFieldMapper.DateFieldType(DataStream.TIMESTAMP_FIELD_NAME),
                    null,
                    new DateFieldMapper.DateFieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME),
                    null
                )
            );
        }

        private void addIndex(Index index) {
            if (clusterState.metadata().getProject().index(index) != null) {
                throw new IllegalArgumentException("Min/Max timestamps for " + index + " were already defined");
            }

            Settings.Builder indexSettings = settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0);

            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata()).put(indexMetadataBuilder);
            clusterState = ClusterState.builder(clusterState).metadata(metadataBuilder).build();
            fields.put(index, new DateFieldRangeInfo(new DateFieldMapper.DateFieldType(DataStream.TIMESTAMP_FIELD_NAME), null, null, null));
        }

        public CoordinatorRewriteContextProvider build() {
            return new CoordinatorRewriteContextProvider(
                XContentParserConfiguration.EMPTY,
                mock(Client.class),
                System::currentTimeMillis,
                () -> clusterState.projectState(),
                fields::get
            );
        }
    }
}
