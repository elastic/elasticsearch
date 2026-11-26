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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

public class QueryPhaseResultConsumerTests extends ESTestCase {

    private SearchPhaseController searchPhaseController;
    private ThreadPool threadPool;
    private EsThreadPoolExecutor executor;

    @Before
    public void setup() {
        searchPhaseController = new SearchPhaseController((t, s) -> new AggregationReduceContext.Builder() {
            @Override
            public AggregationReduceContext forPartialReduction() {
                return new AggregationReduceContext.ForPartial(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    null,
                    t,
                    mock(AggregationBuilder.class),
                    b -> {}
                );
            }

            public AggregationReduceContext forFinalReduction() {
                return new AggregationReduceContext.ForFinal(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    null,
                    t,
                    mock(AggregationBuilder.class),
                    b -> {},
                    PipelineAggregator.PipelineTree.EMPTY
                );
            };
        });
        threadPool = new TestThreadPool(SearchPhaseControllerTests.class.getName());
        executor = EsExecutors.newFixed(
            "test",
            1,
            10,
            EsExecutors.daemonThreadFactory("test"),
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
        searchProgressListener.notifyListShards(searchShards, Collections.emptyList(), SearchResponse.Clusters.EMPTY, false, timeProvider);

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

    public void testResultArrivingAfterCloseIsDiscarded() {
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder().aggregation(new SumAggregationBuilder("test")));
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofMb(64));

        try (
            QueryPhaseResultConsumer consumer = new QueryPhaseResultConsumer(
                searchRequest,
                executor,
                circuitBreaker,
                searchPhaseController,
                () -> false,
                SearchProgressListener.NOOP,
                2,
                e -> {
                    throw new AssertionError("unexpected partial merge failure", e);
                }
            )
        ) {
            QuerySearchResult early = queryResultWithAggs(0);
            consumer.consumeResult(early, () -> {});
            early.decRef();
            assertThat(circuitBreaker.getUsed(), greaterThan(0L));

            // a failed phase closes the consumer while its shard requests are still in flight
            consumer.close();
            assertEquals(0L, circuitBreaker.getUsed());
            assertFalse(early.hasReferences());

            // this result arrives too late to be buffered into state doClose released, or charged to a breaker that
            // nothing will credit back
            QuerySearchResult late = queryResultWithAggs(1);
            AtomicBoolean nextRan = new AtomicBoolean();
            consumer.consumeResult(late, () -> nextRan.set(true));

            assertTrue("the shard still has to be counted down", nextRan.get());
            assertEquals("a discarded result must not charge the breaker", 0L, circuitBreaker.getUsed());
            assertNull("a discarded result must release its aggregations", late.aggregations());
            late.decRef();
            assertFalse("the caller's reference must be the last one", late.hasReferences());
        }
    }

    public void testConcurrentConsumeAndCloseDiscardsLateResults() {
        // repeated because a single round usually misses the window below
        for (int round = 0; round < 50; round++) {
            int numShards = randomIntBetween(2, 8);
            SearchRequest searchRequest = new SearchRequest("index");
            searchRequest.source(new SearchSourceBuilder().aggregation(new SumAggregationBuilder("test")));
            CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofMb(64));

            List<QuerySearchResult> shardResults = new ArrayList<>(numShards);
            for (int i = 0; i < numShards; i++) {
                shardResults.add(queryResultWithAggs(i));
            }

            // One expected result per shard, so batchReduceSize is numShards and no partial merge is ever queued.
            // A merge still running at close is a separate, pre-existing problem.
            QueryPhaseResultConsumer consumer = new QueryPhaseResultConsumer(
                searchRequest,
                executor,
                circuitBreaker,
                searchPhaseController,
                () -> false,
                SearchProgressListener.NOOP,
                numShards,
                e -> {
                    throw new AssertionError("unexpected partial merge failure", e);
                }
            );

            // the check inside consume's lock only matters when a close lands between the unlocked check and the
            // lock, where the consume charges the breaker and then hits the buffer doClose already released
            startInParallel(numShards + 1, i -> {
                if (i == numShards) {
                    consumer.close();
                } else {
                    consumer.consumeResult(shardResults.get(i), () -> {});
                }
            });
            consumer.close();

            assertEquals("nothing may stay charged to the breaker", 0L, circuitBreaker.getUsed());
            for (QuerySearchResult result : shardResults) {
                assertNull("every result must have released its aggregations", result.aggregations());
                result.decRef();
                assertFalse(result.hasReferences());
            }
        }
    }

    public void testFinalReduceWithOnlyBatchedResultsWithoutAggregations() throws Exception {
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder().aggregation(new SumAggregationBuilder("test")));
        int nodeResponses = randomIntBetween(1, 10);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofMb(64));

        try (
            QueryPhaseResultConsumer consumer = new QueryPhaseResultConsumer(
                searchRequest,
                executor,
                circuitBreaker,
                searchPhaseController,
                () -> false,
                SearchProgressListener.NOOP,
                nodeResponses,
                e -> {
                    throw new AssertionError("unexpected partial merge failure", e);
                }
            )
        ) {
            for (int i = 0; i < nodeResponses; i++) {
                consumer.addBatchedPartialResult(new SearchPhaseController.TopDocsStats(0), emptyMergeResult());
            }

            assertNull(consumer.reduce().aggregations());
        }
        assertEquals("empty node responses must not leave a charge", 0L, circuitBreaker.getUsed());
    }

    /**
     * Node responses without aggregations, from nodes whose shards all failed or were skipped, must not change the reduced aggregations.
     */
    public void testFinalReduceIgnoresBatchedResultsWithoutAggregations() throws Exception {
        SearchRequest originalRequest = new SearchRequest("index");
        originalRequest.source(new SearchSourceBuilder().aggregation(new SumAggregationBuilder("test")));
        // from three shard results on, the first ones are partially reduced into a local merge result
        originalRequest.setBatchedReduceSize(2);
        SearchRequest searchRequest = randomBoolean() ? originalRequest : withoutFinalReduce(originalRequest);
        int shards = randomIntBetween(0, 5);
        // without buffered shard results, a node response has to carry the aggregations
        boolean batchedWithAggs = shards == 0 || randomBoolean();
        int emptyResponses = randomIntBetween(1, 10);
        double batchedSum = randomDoubleBetween(1, 100, true);
        CircuitBreaker circuitBreaker = newLimitedBreaker(ByteSizeValue.ofMb(64));

        // the buffered shards plus one shard per node response
        try (QueryPhaseResultConsumer consumer = newReducingConsumer(searchRequest, circuitBreaker, shards + 1 + emptyResponses)) {
            CountDownLatch consumed = new CountDownLatch(shards);
            for (int i = 0; i < shards; i++) {
                QuerySearchResult result = queryResultWithAggs(i);
                consumer.consumeResult(result, consumed::countDown);
                result.decRef();
            }
            assertTrue(consumed.await(10, TimeUnit.SECONDS));

            List<QueryPhaseResultConsumer.MergeResult> nodeResults = new ArrayList<>();
            for (int i = 0; i < emptyResponses; i++) {
                nodeResults.add(emptyMergeResult());
            }
            if (batchedWithAggs) {
                consumePartiallyReducedResult(consumer, shards);
                nodeResults.add(mergeResultOf(new Sum("test", batchedSum, DocValueFormat.RAW, Map.of())));
            }
            Collections.shuffle(nodeResults, random());
            for (QueryPhaseResultConsumer.MergeResult nodeResult : nodeResults) {
                consumer.addBatchedPartialResult(new SearchPhaseController.TopDocsStats(0), nodeResult);
            }

            InternalAggregations reduced = consumer.reduce().aggregations();
            Sum sum = reduced.get("test");
            assertEquals(shards + (batchedWithAggs ? batchedSum : 0), sum.value(), 1e-9);
        }
        assertEquals(0L, circuitBreaker.getUsed());
    }

    private QueryPhaseResultConsumer newReducingConsumer(SearchRequest searchRequest, CircuitBreaker circuitBreaker, int numShards) {
        return new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            circuitBreaker,
            reducingController(),
            () -> false,
            SearchProgressListener.NOOP,
            numShards,
            e -> {
                throw new AssertionError("unexpected partial merge failure", e);
            }
        );
    }

    private static SearchPhaseController reducingController() {
        return new SearchPhaseController((t, agg) -> new AggregationReduceContext.Builder() {
            @Override
            public AggregationReduceContext forPartialReduction() {
                return new AggregationReduceContext.ForPartial(BigArrays.NON_RECYCLING_INSTANCE, null, t, agg, b -> {});
            }

            @Override
            public AggregationReduceContext forFinalReduction() {
                return new AggregationReduceContext.ForFinal(BigArrays.NON_RECYCLING_INSTANCE, null, t, agg, b -> {});
            }
        });
    }

    // with several clusters and ccs_minimize_roundtrips, each cluster skips the final reduce and the merging node does it
    private static SearchRequest withoutFinalReduce(SearchRequest request) {
        return SearchRequest.subSearchRequest(new TaskId("n", 1), request, new String[] { "index" }, "remote", 0, false);
    }

    // a data node returns its shards as partially reduced results, with the top docs and aggregations in its merge result
    private static void consumePartiallyReducedResult(QueryPhaseResultConsumer consumer, int shardIndex) {
        QuerySearchResult result = new QuerySearchResult(
            new ShardSearchContextId("", shardIndex),
            new SearchShardTarget("node", new ShardId("index", "uuid", shardIndex), null),
            null
        );
        result.setShardIndex(shardIndex);
        result.markAsPartiallyReduced();
        consumer.consumeResult(result, () -> {});
        result.decRef();
    }

    private static QueryPhaseResultConsumer.MergeResult mergeResultOf(InternalAggregation aggregation) {
        InternalAggregations aggs = InternalAggregations.from(List.of(aggregation));
        return new QueryPhaseResultConsumer.MergeResult(List.of(), null, aggs, DelayableWriteable.getSerializedSize(aggs));
    }

    private static QueryPhaseResultConsumer.MergeResult emptyMergeResult() {
        return new QueryPhaseResultConsumer.MergeResult(List.of(), null, null, 0L);
    }

    private static QuerySearchResult queryResultWithAggs(int shardIndex) {
        QuerySearchResult result = new QuerySearchResult(
            new ShardSearchContextId("", shardIndex),
            new SearchShardTarget("node", new ShardId("index", "uuid", shardIndex), null),
            null
        );
        result.topDocs(
            new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
            new DocValueFormat[0]
        );
        result.aggregations(InternalAggregations.from(List.of(new Sum("test", 1.0D, DocValueFormat.RAW, Map.of()))));
        result.setShardIndex(shardIndex);
        return result;
    }

    private static class ThrowingSearchProgressListener extends SearchProgressListener {
        private final AtomicInteger onQueryResult = new AtomicInteger(0);
        private final AtomicInteger onPartialReduce = new AtomicInteger(0);
        private final AtomicInteger onFinalReduce = new AtomicInteger(0);

        @Override
        protected void onListShards(
            List<SearchShard> shards,
            List<SearchShard> skippedShards,
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
