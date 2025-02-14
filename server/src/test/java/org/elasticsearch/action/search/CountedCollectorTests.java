/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

public class CountedCollectorTests extends ESTestCase {
    public void testCollect() throws InterruptedException {
        try (ArraySearchPhaseResults<SearchPhaseResult> consumer = new ArraySearchPhaseResults<>(randomIntBetween(1, 100))) {
            List<Integer> state = new ArrayList<>();
            int numResultsExpected = randomIntBetween(1, consumer.getAtomicArray().length());
            MockSearchPhaseContext context = new MockSearchPhaseContext(consumer.getAtomicArray().length());
            CountDownLatch latch = new CountDownLatch(1);
            boolean maybeFork = randomBoolean();
            Executor executor = (runnable) -> {
                if (randomBoolean() && maybeFork) {
                    new Thread(runnable).start();

                } else {
                    runnable.run();
                }
            };
            CountedCollector<SearchPhaseResult> collector = new CountedCollector<>(consumer, numResultsExpected, latch::countDown, context);
            for (int i = 0; i < numResultsExpected; i++) {
                int shardID = i;
                switch (randomIntBetween(0, 2)) {
                    case 0 -> {
                        state.add(0);
                        executor.execute(() -> collector.countDown());
                    }
                    case 1 -> {
                        state.add(1);
                        executor.execute(() -> {
                            DfsSearchResult dfsSearchResult = new DfsSearchResult(
                                new ShardSearchContextId(UUIDs.randomBase64UUID(), shardID),
                                null,
                                null
                            );
                            dfsSearchResult.setShardIndex(shardID);
                            dfsSearchResult.setSearchShardTarget(new SearchShardTarget("foo", new ShardId("bar", "baz", shardID), null));
                            collector.onResult(dfsSearchResult);
                        });
                    }
                    case 2 -> {
                        state.add(2);
                        executor.execute(
                            () -> collector.onFailure(
                                shardID,
                                new SearchShardTarget("foo", new ShardId("bar", "baz", shardID), null),
                                new RuntimeException("boom")
                            )
                        );
                    }
                    default -> fail("unknown state");
                }
            }
            latch.await();
            assertEquals(numResultsExpected, state.size());
            AtomicArray<SearchPhaseResult> results = consumer.getAtomicArray();
            for (int i = 0; i < numResultsExpected; i++) {
                switch (state.get(i)) {
                    case 0 -> assertNull(results.get(i));
                    case 1 -> {
                        assertNotNull(results.get(i));
                        assertEquals(i, results.get(i).getContextId().getId());
                    }
                    case 2 -> {
                        final int shardId = i;
                        assertEquals(1, context.failures.stream().filter(f -> f.shardId() == shardId).count());
                    }
                    default -> fail("unknown state");
                }
            }

            for (int i = numResultsExpected; i < results.length(); i++) {
                assertNull("index: " + i, results.get(i));
            }
            context.results.close();
        }
    }
}
