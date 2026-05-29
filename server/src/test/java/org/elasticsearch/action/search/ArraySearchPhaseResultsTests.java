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
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ArraySearchPhaseResultsTests extends ESTestCase {

    public void testConsumeResultAccumulatesDirectoryMetrics() {
        int numShards = 5;
        long expectedBytesRead = 0;
        try (ArraySearchPhaseResults<QuerySearchResult> results = new ArraySearchPhaseResults<>(numShards)) {
            for (int i = 0; i < numShards; i++) {
                long shardBytes = (i + 1) * 7L;
                expectedBytesRead += shardBytes;
                results.consumeResult(querySearchResultWithMetrics(i, storeMetrics(shardBytes)), () -> {});
            }
            DirectoryMetrics observed = results.getDirectoryMetrics();
            assertFalse(observed.isEmpty());
            assertEquals(expectedBytesRead, observed.metrics(StoreMetrics.NAME).cast(StoreMetrics.class).getBytesRead());
        }
    }

    public void testEmptyMetricsAreSkipped() {
        try (ArraySearchPhaseResults<QuerySearchResult> results = new ArraySearchPhaseResults<>(3)) {
            for (int i = 0; i < 3; i++) {
                results.consumeResult(querySearchResultWithMetrics(i, null), () -> {});
            }
            assertTrue(results.getDirectoryMetrics().isEmpty());
        }
    }

    public void testConcurrentDirectoryMetricsAccumulation() throws Exception {
        int numShards = 64;
        long perShardBytes = 13L;
        try (ArraySearchPhaseResults<QuerySearchResult> results = new ArraySearchPhaseResults<>(numShards)) {
            int feeders = 4;
            CountDownLatch start = new CountDownLatch(1);
            Thread[] threads = new Thread[feeders];
            AtomicInteger nextShard = new AtomicInteger();
            for (int t = 0; t < feeders; t++) {
                threads[t] = new Thread(() -> {
                    safeAwait(start);
                    int idx;
                    while ((idx = nextShard.getAndIncrement()) < numShards) {
                        results.consumeResult(querySearchResultWithMetrics(idx, storeMetrics(perShardBytes)), () -> {});
                    }
                });
                threads[t].start();
            }
            start.countDown();
            for (Thread thread : threads) {
                thread.join(TimeUnit.SECONDS.toMillis(10));
            }

            assertEquals(
                numShards * perShardBytes,
                results.getDirectoryMetrics().metrics(StoreMetrics.NAME).cast(StoreMetrics.class).getBytesRead()
            );
        }
    }

    private static QuerySearchResult querySearchResultWithMetrics(int shardIndex, DirectoryMetrics metrics) {
        SearchShardTarget target = new SearchShardTarget("node", new ShardId("index", "uuid", shardIndex), null);
        QuerySearchResult result = new QuerySearchResult();
        TopDocs topDocs = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
        result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), new DocValueFormat[0]);
        result.setSearchShardTarget(target);
        result.setShardIndex(shardIndex);
        if (metrics != null) {
            result.setDirectoryMetrics(metrics);
        }
        return result;
    }

    private static DirectoryMetrics storeMetrics(long bytesRead) {
        DirectoryMetrics.Builder b = new DirectoryMetrics.Builder();
        b.add(StoreMetrics.NAME, new StoreMetrics(bytesRead));
        return b.build();
    }
}
