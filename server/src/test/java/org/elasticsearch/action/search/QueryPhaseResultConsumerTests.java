/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestEsExecutors;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;

public class QueryPhaseResultConsumerTests extends ESTestCase {

    private SearchPhaseController searchPhaseController;
    private ThreadPool threadPool;
    private EsThreadPoolExecutor executor;

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(new SearchModule(Settings.EMPTY, emptyList()).getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Before
    public void setup() {
        searchPhaseController = new SearchPhaseController((t, s) -> new AggregationReduceContext.Builder() {
            @Override
            public AggregationReduceContext forPartialReduction(
                @Nullable Collection<org.elasticsearch.search.SearchHits> topHitsToRelease
            ) {
                return new AggregationReduceContext.ForPartial(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    null,
                    t,
                    mock(AggregationBuilder.class),
                    b -> {},
                    topHitsToRelease
                );
            }

            @Override
            public AggregationReduceContext forFinalReduction(@Nullable Collection<org.elasticsearch.search.SearchHits> topHitsToRelease) {
                return new AggregationReduceContext.ForFinal(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    null,
                    t,
                    mock(AggregationBuilder.class),
                    b -> {},
                    PipelineAggregator.PipelineTree.EMPTY,
                    topHitsToRelease
                );
            }
        });
        threadPool = new TestThreadPool(SearchPhaseControllerTests.class.getName());
        executor = EsExecutors.newFixed(
            "test",
            1,
            10,
            TestEsExecutors.testOnlyDaemonThreadFactory("test"),
            threadPool.getThreadContext(),
            randomFrom(TaskTrackingConfig.DEFAULT, TaskTrackingConfig.DO_NOT_TRACK)
        );
    }

    @After
    public void cleanup() {
        executor.shutdownNow();
        terminate(threadPool);
    }

    public void testProgressListenerExceptionsAreCaught() throws Exception {

        ThrowingSearchProgressListener searchProgressListener = new ThrowingSearchProgressListener();

        List<SearchShard> searchShards = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            searchShards.add(new SearchShard(null, new ShardId("index", "uuid", i)));
        }
        long timestamp = randomLongBetween(1000, Long.MAX_VALUE - 1000);
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            timestamp,
            timestamp,
            () -> timestamp + 1000
        );
        searchProgressListener.notifyListShards(searchShards, Collections.emptyMap(), SearchResponse.Clusters.EMPTY, false, timeProvider);

        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.setBatchedReduceSize(2);
        AtomicReference<Exception> onPartialMergeFailure = new AtomicReference<>();
        try (
            QueryPhaseResultConsumer queryPhaseResultConsumer = new QueryPhaseResultConsumer(
                searchRequest,
                executor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                searchPhaseController,
                () -> false,
                searchProgressListener,
                10,
                e -> onPartialMergeFailure.accumulateAndGet(e, (prev, curr) -> {
                    curr.addSuppressed(prev);
                    return curr;
                })
            )
        ) {

            CountDownLatch partialReduceLatch = new CountDownLatch(10);

            for (int i = 0; i < 10; i++) {
                SearchShardTarget searchShardTarget = new SearchShardTarget("node", new ShardId("index", "uuid", i), null);
                QuerySearchResult querySearchResult = new QuerySearchResult();
                TopDocs topDocs = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
                querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), new DocValueFormat[0]);
                querySearchResult.setSearchShardTarget(searchShardTarget);
                querySearchResult.setShardIndex(i);
                queryPhaseResultConsumer.consumeResult(querySearchResult, partialReduceLatch::countDown);
            }

            assertEquals(10, searchProgressListener.onQueryResult.get());
            assertTrue(partialReduceLatch.await(10, TimeUnit.SECONDS));
            assertNull(onPartialMergeFailure.get());
            assertEquals(8, searchProgressListener.onPartialReduce.get());

            queryPhaseResultConsumer.reduce();
            assertEquals(1, searchProgressListener.onFinalReduce.get());
        }
    }

    /**
     * Adds batches with a high simulated size, expecting the CB to trip before deserialization.
     */
    public void testBatchedEstimateSizeTooBig() throws Exception {
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder().aggregation(new SumAggregationBuilder("sum")));

        var aggCount = randomIntBetween(1, 10);
        var circuitBreakerLimit = ByteSizeValue.ofMb(256);
        var circuitBreaker = newLimitedBreaker(circuitBreakerLimit);
        // More than what the CircuitBreaker should allow
        long aggregationEstimatedSize = (long) (circuitBreakerLimit.getBytes() * 1.05 / aggCount);

        try (
            QueryPhaseResultConsumer queryPhaseResultConsumer = new QueryPhaseResultConsumer(
                searchRequest,
                executor,
                circuitBreaker,
                searchPhaseController,
                () -> false,
                new SearchProgressListener() {},
                10,
                e -> {}
            )
        ) {
            for (int i = 0; i < aggCount; i++) {
                // Add a dummy merge result with a high estimated size
                var mergeResult = new QueryPhaseResultConsumer.MergeResult(List.of(), null, new DelegatingDelayableWriteable<>(() -> {
                    fail("This shouldn't be called");
                    return null;
                }), aggregationEstimatedSize);
                queryPhaseResultConsumer.addBatchedPartialResult(new SearchPhaseController.TopDocsStats(0), mergeResult);
            }

            try {
                queryPhaseResultConsumer.reduce();
                fail("Expecting a circuit breaking exception to be thrown");
            } catch (CircuitBreakingException e) {
                // The last merge result estimate should break
                assertThat(e.getBytesWanted(), equalTo(aggregationEstimatedSize));
            }
        }
    }

    /**
     * Adds batches with a high simulated size, expecting the CB to trip before deserialization.
     * <p>
     *     Similar to {@link #testBatchedEstimateSizeTooBig()}, but this tests the extra size
     * </p>
     */
    public void testBatchedEstimateSizeTooBigAfterDeserialization() throws Exception {
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder().aggregation(new SumAggregationBuilder("sum")));

        var aggCount = randomIntBetween(1, 10);
        var circuitBreakerLimit = ByteSizeValue.ofMb(256);
        var circuitBreaker = newLimitedBreaker(circuitBreakerLimit);
        // Less than the CB, but more after the 1.5x
        long aggregationEstimatedSize = (long) (circuitBreakerLimit.getBytes() * 0.75 / aggCount);
        long totalAggregationsEstimatedSize = aggregationEstimatedSize * aggCount;

        try (
            QueryPhaseResultConsumer queryPhaseResultConsumer = new QueryPhaseResultConsumer(
                searchRequest,
                executor,
                circuitBreaker,
                searchPhaseController,
                () -> false,
                new SearchProgressListener() {},
                10,
                e -> {}
            )
        ) {
            for (int i = 0; i < aggCount; i++) {
                // Add a dummy merge result with a high estimated size
                var mergeResult = new QueryPhaseResultConsumer.MergeResult(List.of(), null, new DelegatingDelayableWriteable<>(() -> {
                    fail("This shouldn't be called");
                    return null;
                }), aggregationEstimatedSize);
                queryPhaseResultConsumer.addBatchedPartialResult(new SearchPhaseController.TopDocsStats(0), mergeResult);
            }

            try {
                queryPhaseResultConsumer.reduce();
                fail("Expecting a circuit breaking exception to be thrown");
            } catch (CircuitBreakingException e) {
                assertThat(circuitBreaker.getUsed(), greaterThanOrEqualTo(aggregationEstimatedSize));
                // A final +0.5x is added to account for the serialized->deserialized extra size
                assertThat(e.getBytesWanted(), equalTo(Math.round(totalAggregationsEstimatedSize * 0.5)));
            }
        }
    }

    /**
     * Exercises {@link QueryPhaseResultConsumer} with real {@link InternalTopHits} reductions (production-shaped
     * {@link AggregationReduceContext}) and verifies pooled per-shard {@link SearchHits} are released after the
     * coordinator builds a {@link SearchResponse} and drops its reference.
     */
    public void testTopHitsShardSearchHitsReleasedAfterSearchResponseDecRef() throws Exception {
        final String aggName = "th";
        SearchPhaseController productionLikeController = new SearchPhaseController((t, agg) -> new AggregationReduceContext.Builder() {
            @Override
            public AggregationReduceContext forPartialReduction(
                @Nullable Collection<org.elasticsearch.search.SearchHits> topHitsToRelease
            ) {
                return new AggregationReduceContext.ForPartial(BigArrays.NON_RECYCLING_INSTANCE, null, t, agg, b -> {}, topHitsToRelease);
            }

            @Override
            public AggregationReduceContext forFinalReduction(@Nullable Collection<org.elasticsearch.search.SearchHits> topHitsToRelease) {
                return new AggregationReduceContext.ForFinal(BigArrays.NON_RECYCLING_INSTANCE, null, t, agg, b -> {}, topHitsToRelease);
            }
        });

        int numShards = 3;
        SearchRequest request = new SearchRequest("index");
        request.source(new SearchSourceBuilder().aggregation(new TopHitsAggregationBuilder(aggName).size(1)).size(0));
        request.setBatchedReduceSize(2);

        List<SearchHits> shardHitsToTrack = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numShards);
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = productionLikeController.newSearchPhaseResults(
                executor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                numShards,
                e -> {
                    throw new AssertionError("unexpected partial merge failure", e);
                }
            )
        ) {
            for (int i = 0; i < numShards; i++) {
                SearchShardTarget searchShardTarget = new SearchShardTarget("node", new ShardId("index", "uuid", i), null);
                QuerySearchResult querySearchResult = new QuerySearchResult(new ShardSearchContextId("", i), searchShardTarget, null);
                try {
                    SearchHit hit = new SearchHit(0, "id-" + i);
                    hit.sourceRef(Source.fromMap(Map.of("f", "v"), XContentType.JSON).internalSourceRef());
                    hit.score(1.0f);
                    SearchHits searchHits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
                    assertTrue(searchHits.isPooled());
                    shardHitsToTrack.add(searchHits);

                    TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(
                        new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(0, 1.0f) }),
                        1.0f
                    );
                    InternalTopHits internalTopHits = new InternalTopHits(aggName, 0, 1, topDocsAndMaxScore, searchHits, null);
                    querySearchResult.topDocs(
                        new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
                        new DocValueFormat[0]
                    );
                    querySearchResult.aggregations(InternalAggregations.from(Collections.singletonList(internalTopHits)));
                    querySearchResult.setShardIndex(i);
                    querySearchResult.size(0);
                    consumer.consumeResult(querySearchResult, latch::countDown);
                } finally {
                    querySearchResult.decRef();
                }
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));

            SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
            assertNotNull(reduce.aggregations());
            InternalTopHits reducedTopHits = reduce.aggregations().get(aggName);
            assertNotNull(reducedTopHits);
            assertThat(reducedTopHits.getHits().getHits().length, equalTo(1));

            SearchResponseSections sections = reduce.buildResponse(SearchHits.EMPTY_WITH_TOTAL_HITS, Collections.emptyList());
            SearchResponse response = new SearchResponse(
                sections,
                null,
                numShards,
                numShards,
                0,
                0,
                null,
                SearchResponse.Clusters.EMPTY,
                null
            );
            try {
                assertNotNull(response.getAggregations());
            } finally {
                response.decRef();
            }
        }

        for (SearchHits shardHits : shardHitsToTrack) {
            assertFalse(shardHits.hasReferences());
        }
    }

    /**
     * DelayableWriteable that delegates expansion to a supplier.
     */
    private static class DelegatingDelayableWriteable<T extends Writeable> extends DelayableWriteable<T> {
        private final Supplier<T> supplier;

        private DelegatingDelayableWriteable(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("Not to be called");
        }

        @Override
        public T expand() {
            return supplier.get();
        }

        @Override
        public Serialized<T> asSerialized(Reader<T> reader, NamedWriteableRegistry registry) {
            throw new UnsupportedOperationException("Not to be called");
        }

        @Override
        public boolean isSerialized() {
            return true;
        }

        @Override
        public long getSerializedSize() {
            return 0;
        }

        @Override
        public void close() {
            // noop
        }
    }

    private static class ThrowingSearchProgressListener extends SearchProgressListener {
        private final AtomicInteger onQueryResult = new AtomicInteger(0);
        private final AtomicInteger onPartialReduce = new AtomicInteger(0);
        private final AtomicInteger onFinalReduce = new AtomicInteger(0);

        @Override
        protected void onListShards(
            List<SearchShard> shards,
            Map<String, Integer> skippedByClusterAlias,
            SearchResponse.Clusters clusters,
            boolean fetchPhase,
            TransportSearchAction.SearchTimeProvider timeProvider
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void onQueryResult(int shardIndex, QuerySearchResult queryResult) {
            onQueryResult.incrementAndGet();
            throw new UnsupportedOperationException();
        }

        @Override
        protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
            onPartialReduce.incrementAndGet();
            throw new UnsupportedOperationException();
        }

        @Override
        protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
            onFinalReduce.incrementAndGet();
            throw new UnsupportedOperationException();
        }
    }
}
