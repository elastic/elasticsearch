/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.action.search.SearchAsyncActionTests.getShardsIter;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class CanMatchPreFilterSearchPhaseTests extends ESTestCase {

    private final CoordinatorRewriteContextProvider EMPTY_CONTEXT_PROVIDER = new StaticCoordinatorRewriteContextProviderBuilder().build();

    public void testFilterShards() throws InterruptedException {

        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        final boolean shard2 = randomBoolean();

        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                new Thread(() -> listener.onResponse(new SearchService.CanMatchResponse(request.shardId().id() == 0 ? shard1 :
                    shard2, null))).start();
            }
        };

        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            2, randomBoolean(), primaryNode, replicaNode);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);

        CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(), EsExecutors.DIRECT_EXECUTOR_SERVICE,
            searchRequest, null, shardsIter, timeProvider, ClusterState.EMPTY_STATE, null,
            (iter) -> new SearchPhase("test") {
                    @Override
                    public void run() throws IOException {
                        result.set(iter);
                        latch.countDown();
                    }}, SearchResponse.Clusters.EMPTY, EMPTY_CONTEXT_PROVIDER);

        canMatchPhase.start();
        latch.await();

        if (shard1 && shard2) {
            for (SearchShardIterator i : result.get()) {
                assertFalse(i.skip());
            }
        } else if (shard1 == false &&  shard2 == false) {
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
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                boolean throwException = request.shardId().id() != 0;
                if (throwException && randomBoolean()) {
                    throw new IllegalArgumentException("boom");
                } else {
                    new Thread(() -> {
                        if (throwException == false) {
                            listener.onResponse(new SearchService.CanMatchResponse(shard1, null));
                        } else {
                            listener.onFailure(new NullPointerException());
                        }
                    }).start();
                }
            }
        };

        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            2, randomBoolean(), primaryNode, replicaNode);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);

        CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(), EsExecutors.DIRECT_EXECUTOR_SERVICE,
            searchRequest, null, shardsIter, timeProvider, ClusterState.EMPTY_STATE, null,
            (iter) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    result.set(iter);
                    latch.countDown();
                }}, SearchResponse.Clusters.EMPTY, EMPTY_CONTEXT_PROVIDER);

        canMatchPhase.start();
        latch.await();

        assertEquals(0, result.get().get(0).shardId().id());
        assertEquals(1, result.get().get(1).shardId().id());
        assertEquals(shard1, result.get().get(0).skip() == false);
        assertFalse(result.get().get(1).skip()); // never skip the failure
    }

    /*
     * In cases that a query coordinating node held all the shards for a query, the can match phase would recurse and end in stack overflow
     * when subjected to max concurrent search requests. This test is a test for that situation.
     */
    public void testLotsOfShards() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);

        final Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        final DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));


        final SearchTransportService searchTransportService =
            new SearchTransportService(null, null, null) {
                @Override
                public void sendCanMatch(
                    Transport.Connection connection,
                    ShardSearchRequest request,
                    SearchTask task,
                    ActionListener<SearchService.CanMatchResponse> listener) {
                    listener.onResponse(new SearchService.CanMatchResponse(randomBoolean(), null));
                }
            };

        final CountDownLatch latch = new CountDownLatch(1);
        final OriginalIndices originalIndices = new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS);
        final GroupShardsIterator<SearchShardIterator> shardsIter =
            getShardsIter("idx", originalIndices, 4096, randomBoolean(), primaryNode, replicaNode);
        final ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(1, Runtime.getRuntime().availableProcessors()));
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);
        SearchTransportService transportService = new SearchTransportService(null, null, null);
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(response -> {},
            (e) -> { throw new AssertionError("unexpected", e);});
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        final CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(
            logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            searchRequest,
            null,
            shardsIter,
            timeProvider,
            ClusterState.EMPTY_STATE,
            null,
            (iter) -> new AbstractSearchAsyncAction<>(
                "test",
                logger,
                transportService,
                (cluster, node) -> {
                        assert cluster == null : "cluster was not null: " + cluster;
                        return lookup.get(node);
                    },
                aliasFilters,
                Collections.emptyMap(),
                executor,
                searchRequest,
                responseListener,
                iter,
                new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
                ClusterState.EMPTY_STATE,
                null,
                new ArraySearchPhaseResults<>(iter.size()),
                randomIntBetween(1, 32),
                SearchResponse.Clusters.EMPTY) {

                @Override
                protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                    return new SearchPhase("test") {
                        @Override
                        public void run() {
                            latch.countDown();
                        }
                    };
                }

                @Override
                protected void executePhaseOnShard(
                    final SearchShardIterator shardIt,
                    final SearchShardTarget shard,
                    final SearchActionListener<SearchPhaseResult> listener) {
                    if (randomBoolean()) {
                        listener.onResponse(new SearchPhaseResult() {});
                    } else {
                        listener.onFailure(new Exception("failure"));
                    }
                }
            }, SearchResponse.Clusters.EMPTY, EMPTY_CONTEXT_PROVIDER);

        canMatchPhase.start();
        latch.await();
        executor.shutdown();
    }

    public void testSortShards() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        for (SortOrder order : SortOrder.values()) {
            List<ShardId> shardIds = new ArrayList<>();
            List<MinAndMax<?>> minAndMaxes = new ArrayList<>();
            Set<ShardId> shardToSkip = new HashSet<>();

            SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
                @Override
                public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                         ActionListener<SearchService.CanMatchResponse> listener) {
                    Long min = rarely() ? null : randomLong();
                    Long max = min == null ? null  : randomLongBetween(min, Long.MAX_VALUE);
                    MinAndMax<?> minMax = min == null ? null : new MinAndMax<>(min, max);
                    boolean canMatch = frequently();
                    synchronized (shardIds) {
                        shardIds.add(request.shardId());
                        minAndMaxes.add(minMax);
                        if (canMatch == false) {
                            shardToSkip.add(request.shardId());
                        }
                    }
                    new Thread(() -> listener.onResponse(new SearchService.CanMatchResponse(canMatch, minMax))).start();
                }
            };

            AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("logs",
                new OriginalIndices(new String[]{"logs"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
                randomIntBetween(2, 20), randomBoolean(), primaryNode, replicaNode);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp").order(order)));
            searchRequest.allowPartialSearchResults(true);

            CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
                Collections.emptyMap(), EsExecutors.DIRECT_EXECUTOR_SERVICE,
                searchRequest, null, shardsIter, timeProvider, ClusterState.EMPTY_STATE, null,
                (iter) -> new SearchPhase("test") {
                    @Override
                    public void run() {
                        result.set(iter);
                        latch.countDown();
                    }
                }, SearchResponse.Clusters.EMPTY, EMPTY_CONTEXT_PROVIDER);

            canMatchPhase.start();
            latch.await();
            ShardId[] expected = IntStream.range(0, shardIds.size())
                .boxed()
                .sorted(Comparator.comparing(minAndMaxes::get, MinAndMax.getComparator(order)).thenComparing(shardIds::get))
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
        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        for (SortOrder order : SortOrder.values()) {
            int numShards = randomIntBetween(2, 20);
            List<ShardId> shardIds = new ArrayList<>();
            Set<ShardId> shardToSkip = new HashSet<>();

            SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
                @Override
                public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                         ActionListener<SearchService.CanMatchResponse> listener) {
                    final MinAndMax<?> minMax;
                    if (request.shardId().id() == numShards-1) {
                        minMax = new MinAndMax<>(new BytesRef("bar"), new BytesRef("baz"));
                    } else {
                        Long min = randomLong();
                        Long max = randomLongBetween(min, Long.MAX_VALUE);
                        minMax = new MinAndMax<>(min, max);
                    }
                    boolean canMatch = frequently();
                    synchronized (shardIds) {
                        shardIds.add(request.shardId());
                        if (canMatch == false) {
                            shardToSkip.add(request.shardId());
                        }
                    }
                    new Thread(() -> listener.onResponse(new SearchService.CanMatchResponse(canMatch, minMax))).start();
                }
            };

            AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("logs",
                new OriginalIndices(new String[]{"logs"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
                numShards, randomBoolean(), primaryNode, replicaNode);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp").order(order)));
            searchRequest.allowPartialSearchResults(true);

            CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
                Collections.emptyMap(), EsExecutors.DIRECT_EXECUTOR_SERVICE,
                searchRequest, null, shardsIter, timeProvider, ClusterState.EMPTY_STATE, null,
                (iter) -> new SearchPhase("test") {
                    @Override
                    public void run() {
                        result.set(iter);
                        latch.countDown();
                    }
                }, SearchResponse.Clusters.EMPTY, EMPTY_CONTEXT_PROVIDER);

            canMatchPhase.start();
            latch.await();
            int shardId = 0;
            for (SearchShardIterator i : result.get()) {
                assertThat(i.shardId().id(), equalTo(shardId++));
                assertEquals(shardToSkip.contains(i.shardId()), i.skip());
            }
            assertThat(result.get().size(), equalTo(numShards));
        }
    }

    public void testCanMatchFilteringOnCoordinatorThatCanBeSkipped() throws Exception {
        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream =
            new DataStream("mydata", new DataStream.TimestampField("@timestamp"), List.of(dataStreamIndex1, dataStreamIndex2));

        List<Index> regularIndices =
            randomList(0, 2, () -> new Index(randomAlphaOfLength(10), UUIDs.base64UUID()));

        long indexMinTimestamp = randomLongBetween(0, 5000);
        long indexMaxTimestamp = randomLongBetween(indexMinTimestamp, 5000 * 2);
        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        String timestampFieldName = dataStream.getTimeStampField().getName();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProviderBuilder.addIndexMinMaxTimestamps(dataStreamIndex, timestampFieldName, indexMinTimestamp, indexMaxTimestamp);
        }

        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timestampFieldName);
        // We query a range outside of the timestamp range covered by both datastream indices
        rangeQueryBuilder
            .from(indexMaxTimestamp + 1)
            .to(indexMaxTimestamp + 2);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
            .filter(rangeQueryBuilder);

        if (randomBoolean()) {
            // Add an additional filter that cannot be evaluated in the coordinator but shouldn't
            // affect the end result as we're filtering
            queryBuilder.filter(new TermQueryBuilder("fake", "value"));
        }

        assignShardsAndExecuteCanMatchPhase(dataStream, regularIndices, contextProviderBuilder.build(), queryBuilder,
            (updatedSearchShardIterators, requests) -> {
                List<SearchShardIterator> skippedShards = updatedSearchShardIterators.stream()
                    .filter(SearchShardIterator::skip)
                    .collect(Collectors.toList());;

                List<SearchShardIterator> nonSkippedShards = updatedSearchShardIterators.stream()
                    .filter(searchShardIterator -> searchShardIterator.skip() == false)
                    .collect(Collectors.toList());;

                int regularIndexShardCount = (int) updatedSearchShardIterators.stream()
                    .filter(s -> regularIndices.contains(s.shardId().getIndex()))
                    .count();

                // When all the shards can be skipped we should query at least 1
                // in order to get a valid search response.
                if (regularIndexShardCount == 0) {
                    assertThat(nonSkippedShards.size(), equalTo(1));
                } else {
                    boolean allNonSkippedShardsAreFromRegularIndices = nonSkippedShards.stream()
                        .allMatch(shardIterator -> regularIndices.contains(shardIterator.shardId().getIndex()));

                    assertThat(allNonSkippedShardsAreFromRegularIndices, equalTo(true));
                }

                boolean allSkippedShardAreFromDataStream = skippedShards.stream()
                    .allMatch(shardIterator -> dataStream.getIndices().contains(shardIterator.shardId().getIndex()));
                assertThat(allSkippedShardAreFromDataStream, equalTo(true));

                boolean allRequestsWereTriggeredAgainstRegularIndices = requests.stream()
                    .allMatch(request -> regularIndices.contains(request.shardId().getIndex()));
                assertThat(allRequestsWereTriggeredAgainstRegularIndices, equalTo(true));
            });
    }

    public void testCanMatchFilteringOnCoordinatorParsingFails() throws Exception {
        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream =
            new DataStream("mydata", new DataStream.TimestampField("@timestamp"), List.of(dataStreamIndex1, dataStreamIndex2));

        List<Index> regularIndices =
            randomList(0, 2, () -> new Index(randomAlphaOfLength(10), UUIDs.base64UUID()));

        long indexMinTimestamp = randomLongBetween(0, 5000);
        long indexMaxTimestamp = randomLongBetween(indexMinTimestamp, 5000 * 2);
        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        String timestampFieldName = dataStream.getTimeStampField().getName();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProviderBuilder.addIndexMinMaxTimestamps(dataStreamIndex, timestampFieldName, indexMinTimestamp, indexMaxTimestamp);
        }

        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timestampFieldName);
        // Query with a non default date format
        rangeQueryBuilder
            .from("2020-1-01")
            .to("2021-1-01");

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
            .filter(rangeQueryBuilder);

        if (randomBoolean()) {
            // Add an additional filter that cannot be evaluated in the coordinator but shouldn't
            // affect the end result as we're filtering
            queryBuilder.filter(new TermQueryBuilder("fake", "value"));
        }

        assignShardsAndExecuteCanMatchPhase(dataStream,
            regularIndices,
            contextProviderBuilder.build(),
            queryBuilder,
            this::assertAllShardsAreQueried
        );
    }

    public void testCanMatchFilteringOnCoordinatorThatCanNotBeSkipped() throws Exception {
        // Generate indices
        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream =
            new DataStream("mydata", new DataStream.TimestampField("@timestamp"), List.of(dataStreamIndex1, dataStreamIndex2));

        List<Index> regularIndices =
            randomList(0, 2, () -> new Index(randomAlphaOfLength(10), UUIDs.base64UUID()));

        long indexMinTimestamp = 10;
        long indexMaxTimestamp = 20;
        StaticCoordinatorRewriteContextProviderBuilder contextProviderBuilder = new StaticCoordinatorRewriteContextProviderBuilder();
        String timestampFieldName = dataStream.getTimeStampField().getName();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProviderBuilder.addIndexMinMaxTimestamps(dataStreamIndex, timestampFieldName, indexMinTimestamp, indexMaxTimestamp);
        }

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        // Query inside of the data stream index range
        if (randomBoolean()) {
            // Query generation
            RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timestampFieldName);
            // We query a range within the timestamp range covered by both datastream indices
            rangeQueryBuilder
                .from(indexMinTimestamp)
                .to(indexMaxTimestamp);

            queryBuilder.filter(rangeQueryBuilder);

            if (randomBoolean()) {
                // Add an additional filter that cannot be evaluated in the coordinator but shouldn't
                // affect the end result as we're filtering
                queryBuilder.filter(new TermQueryBuilder("fake", "value"));
            }
        } else {
            // We query a range outside of the timestamp range covered by both datastream indices
            RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timestampFieldName)
                .from(indexMaxTimestamp + 1)
                .to(indexMaxTimestamp + 2);

            TermQueryBuilder termQueryBuilder = new TermQueryBuilder("fake", "value");

            // This is always evaluated as true in the coordinator as we cannot determine there if
            // the term query clause is false.
            queryBuilder.should(rangeQueryBuilder)
                .should(termQueryBuilder);
        }

        assignShardsAndExecuteCanMatchPhase(dataStream,
            regularIndices,
            contextProviderBuilder.build(),
            queryBuilder,
            this::assertAllShardsAreQueried
        );
    }

    private void assertAllShardsAreQueried(List<SearchShardIterator> updatedSearchShardIterators, List<ShardSearchRequest> requests) {
        int skippedShards = (int) updatedSearchShardIterators.stream()
            .filter(SearchShardIterator::skip)
            .count();

        assertThat(skippedShards, equalTo(0));

        int nonSkippedShards = (int) updatedSearchShardIterators.stream()
            .filter(searchShardIterator -> searchShardIterator.skip() == false)
            .count();

        assertThat(nonSkippedShards, equalTo(updatedSearchShardIterators.size()));

        int shardsWithPrimariesAssigned = (int) updatedSearchShardIterators.stream()
            .filter(s -> s.size() > 0)
            .count();
        assertThat(requests.size(), equalTo(shardsWithPrimariesAssigned));
    }

    private <QB extends AbstractQueryBuilder<QB>>
    void assignShardsAndExecuteCanMatchPhase(DataStream dataStream,
                                             List<Index> regularIndices,
                                             CoordinatorRewriteContextProvider contextProvider,
                                             AbstractQueryBuilder<QB> query,
                                             BiConsumer<List<SearchShardIterator>,
                                                 List<ShardSearchRequest>> canMatchResultsConsumer) throws Exception {
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        List<String> indicesToSearch = new ArrayList<>();
        indicesToSearch.add(dataStream.getName());
        for (Index regularIndex : regularIndices) {
            indicesToSearch.add(regularIndex.getName());
        }

        String[] indices = indicesToSearch.toArray(new String[0]);
        OriginalIndices originalIndices = new OriginalIndices(indices, SearchRequest.DEFAULT_INDICES_OPTIONS);

        boolean atLeastOnePrimaryAssigned = false;
        final List<SearchShardIterator> originalShardIters = new ArrayList<>();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            // If we have to execute the can match request against all the shards
            // and none is assigned, the phase is considered as failed meaning that the next phase won't be executed
            boolean withAssignedPrimaries = randomBoolean() || atLeastOnePrimaryAssigned == false;
            int numShards = randomIntBetween(1, 6);
            originalShardIters.addAll(
                getShardsIter(dataStreamIndex,
                    originalIndices,
                    numShards,
                    false,
                    withAssignedPrimaries ? primaryNode : null,
                    null)
            );
            atLeastOnePrimaryAssigned |= withAssignedPrimaries;
        }

        for (Index regularIndex : regularIndices) {
            originalShardIters.addAll(
                getShardsIter(regularIndex,
                    originalIndices,
                    randomIntBetween(1, 6),
                    randomBoolean(),
                    primaryNode,
                    replicaNode)
            );
        }
        GroupShardsIterator<SearchShardIterator> shardsIter = GroupShardsIterator.sortAndCreate(originalShardIters);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indices);
        searchRequest.allowPartialSearchResults(true);

        final AliasFilter aliasFilter;
        if (randomBoolean()) {
            // Apply the query on the request body
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
            searchSourceBuilder.query(query);
            searchRequest.source(searchSourceBuilder);

            // Sometimes apply the same query in the alias filter too
            aliasFilter = new AliasFilter(randomBoolean() ? query : null, Strings.EMPTY_ARRAY);
        } else {
            // Apply the query as an alias filter
            aliasFilter = new AliasFilter(query, Strings.EMPTY_ARRAY);
        }

        Map<String, AliasFilter> aliasFilters = new HashMap<>();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            aliasFilters.put(dataStreamIndex.getUUID(), aliasFilter);
        }

        for (Index regularIndex : regularIndices) {
            aliasFilters.put(regularIndex.getUUID(), aliasFilter);
        }

        // We respond by default that the query can match
        final List<ShardSearchRequest> requests = Collections.synchronizedList(new ArrayList<>());
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                requests.add(request);
                listener.onResponse(new SearchService.CanMatchResponse(true, null));
            }
        };

        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);

        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            aliasFilters,
            Collections.emptyMap(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            searchRequest,
            null,
            shardsIter,
            timeProvider,
            ClusterState.EMPTY_STATE,
            null,
            (iter) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    result.set(iter);
                    latch.countDown();
                }
            },
            SearchResponse.Clusters.EMPTY,
            contextProvider);

        canMatchPhase.start();
        latch.await();

        List<SearchShardIterator> updatedSearchShardIterators = new ArrayList<>();
        for (SearchShardIterator updatedSearchShardIterator : result.get()) {
            updatedSearchShardIterators.add(updatedSearchShardIterator);
        }

        canMatchResultsConsumer.accept(updatedSearchShardIterators, requests);
    }

    private static class StaticCoordinatorRewriteContextProviderBuilder {
        private ClusterState clusterState = ClusterState.EMPTY_STATE;
        private final Map<Index, DateFieldMapper.DateFieldType> fields = new HashMap<>();

        private void addIndexMinMaxTimestamps(Index index, String fieldName, long minTimeStamp, long maxTimestamp) {
            if (clusterState.metadata().index(index) != null) {
                throw new IllegalArgumentException("Min/Max timestamps for " + index + " were already defined");
            }

            IndexLongFieldRange timestampRange = IndexLongFieldRange.NO_SHARDS
                .extendWithShardRange(0, 1, ShardLongFieldRange.of(minTimeStamp, maxTimestamp));

            Settings.Builder indexSettings = settings(Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());

            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .timestampRange(timestampRange);

            Metadata.Builder metadataBuilder =
                Metadata.builder(clusterState.metadata())
                    .put(indexMetadataBuilder);

            clusterState = ClusterState.builder(clusterState)
                .metadata(metadataBuilder)
                .build();

            fields.put(index, new DateFieldMapper.DateFieldType(fieldName));
        }

        public CoordinatorRewriteContextProvider build() {
            return new CoordinatorRewriteContextProvider(NamedXContentRegistry.EMPTY,
                mock(NamedWriteableRegistry.class),
                mock(Client.class),
                System::currentTimeMillis,
                () -> clusterState,
                fields::get);
        }
    }
}
