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
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CountOnlyQueryPhaseResultConsumerTests extends ESTestCase {

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

        try (
            CountOnlyQueryPhaseResultConsumer queryPhaseResultConsumer = new CountOnlyQueryPhaseResultConsumer(searchProgressListener, 10)
        ) {
            AtomicInteger nextCounter = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                SearchShardTarget searchShardTarget = new SearchShardTarget("node", new ShardId("index", "uuid", i), null);
                QuerySearchResult querySearchResult = new QuerySearchResult();
                TopDocs topDocs = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
                querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), new DocValueFormat[0]);
                querySearchResult.setSearchShardTarget(searchShardTarget);
                querySearchResult.setShardIndex(i);
                queryPhaseResultConsumer.consumeResult(querySearchResult, nextCounter::incrementAndGet);
            }

            assertEquals(10, searchProgressListener.onQueryResult.get());
            queryPhaseResultConsumer.reduce();
            assertEquals(1, searchProgressListener.onFinalReduce.get());
            assertEquals(10, nextCounter.get());
        }
    }

    public void testNullShardResultHandling() throws Exception {
        try (
            CountOnlyQueryPhaseResultConsumer queryPhaseResultConsumer = new CountOnlyQueryPhaseResultConsumer(
                SearchProgressListener.NOOP,
                10
            )
        ) {
            AtomicInteger nextCounter = new AtomicInteger(0);
            for (int i = 0; i < 10; i++) {
                SearchShardTarget searchShardTarget = new SearchShardTarget("node", new ShardId("index", "uuid", i), null);
                QuerySearchResult querySearchResult = QuerySearchResult.nullInstance();
                querySearchResult.setSearchShardTarget(searchShardTarget);
                querySearchResult.setShardIndex(i);
                queryPhaseResultConsumer.consumeResult(querySearchResult, nextCounter::incrementAndGet);
            }
            var reducePhase = queryPhaseResultConsumer.reduce();
            assertEquals(0, reducePhase.totalHits().value());
            assertEquals(TotalHits.Relation.EQUAL_TO, reducePhase.totalHits().relation());
            assertFalse(reducePhase.isEmptyResult());
            assertEquals(10, nextCounter.get());
        }
    }

    public void testEmptyResults() throws Exception {
        try (
            CountOnlyQueryPhaseResultConsumer queryPhaseResultConsumer = new CountOnlyQueryPhaseResultConsumer(
                SearchProgressListener.NOOP,
                10
            )
        ) {
            var reducePhase = queryPhaseResultConsumer.reduce();
            assertEquals(0, reducePhase.totalHits().value());
            assertEquals(TotalHits.Relation.EQUAL_TO, reducePhase.totalHits().relation());
            assertTrue(reducePhase.isEmptyResult());
        }
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
