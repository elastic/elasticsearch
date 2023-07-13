/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
                return new AggregationReduceContext.ForPartial(BigArrays.NON_RECYCLING_INSTANCE, null, t, mock(AggregationBuilder.class));
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
        searchProgressListener.notifyListShards(searchShards, Collections.emptyList(), SearchResponse.Clusters.EMPTY, false);

        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.setBatchedReduceSize(2);
        AtomicReference<Exception> onPartialMergeFailure = new AtomicReference<>();
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
        );

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

    private static class ThrowingSearchProgressListener extends SearchProgressListener {
        private final AtomicInteger onQueryResult = new AtomicInteger(0);
        private final AtomicInteger onPartialReduce = new AtomicInteger(0);
        private final AtomicInteger onFinalReduce = new AtomicInteger(0);

        @Override
        protected void onListShards(
            List<SearchShard> shards,
            List<SearchShard> skippedShards,
            SearchResponse.Clusters clusters,
            boolean fetchPhase
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void onQueryResult(int shardIndex) {
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
