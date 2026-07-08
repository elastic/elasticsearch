/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;

/**
 * Regression test for the data race behind https://github.com/elastic/elasticsearch/issues/152904,
 * in isolation from {@link org.elasticsearch.compute.operator.topn.ParallelTopNOperator}.
 * <p>
 * Upstream operators routinely alias a shared ref-counted block between sibling pages -- e.g. a
 * degenerate, full-range {@link OrdinalBytesRefBlock#slice}/{@link OrdinalBytesRefBlock#keepMask}
 * just {@code incRef}s the dictionary/block and returns it. Once sibling pages carrying the same
 * block are released concurrently -- as happens once
 * {@link org.elasticsearch.compute.operator.topn.ParallelTopNOperator} fans pages out to
 * background worker threads -- two threads can race {@code decRef} on the same shared block.
 * Before {@link org.elasticsearch.core.AbstractRefCounted} became thread safe (its reference
 * count was a plain, non-atomic field), that race could lose a decrement -- leaking the block --
 * or, more rarely, observe "invalid decRef call: already closed".
 * <p>
 * This was not reliably reproducible through the full {@code CsvIT} test, which needs a real
 * cluster and a specific query shape to hit the race window. This isolates the minimal shape of
 * the bug directly -- one block shared by many threads, all releasing at (as close to) the same
 * instant as a {@link CyclicBarrier} can arrange -- and repeats it many times, since each
 * iteration here is orders of magnitude cheaper than a cluster-backed IT run.
 */
public class OrdinalBytesRefBlockConcurrencyTests extends ESTestCase {

    private CircuitBreaker breaker;
    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
        breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        blockFactory = BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService)).build();
    }

    @After
    public void checkBreakerReleased() {
        assertEquals(0L, breaker.getUsed());
    }

    public void testConcurrentDecRefOnSharedBlockRaces() throws Exception {
        int threadCount = Math.max(4, Runtime.getRuntime().availableProcessors());
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        List<Throwable> escaped = new CopyOnWriteArrayList<>();
        try {
            for (int iter = 0; iter < 20_000 && escaped.isEmpty(); iter++) {
                IntBlock ordinals = blockFactory.newConstantIntBlockWith(0, 1);
                BytesRefVector dict = blockFactory.newConstantBytesRefVector(new BytesRef("shared"), 1);
                OrdinalBytesRefBlock shared = new OrdinalBytesRefBlock(ordinals, dict);
                // `threadCount` sibling owners, matching the incRef a degenerate Block#slice or
                // Block#keepMask performs in production when aliasing this block into another page.
                for (int j = 1; j < threadCount; j++) {
                    shared.incRef();
                }

                CyclicBarrier barrier = new CyclicBarrier(threadCount);
                List<Future<?>> futures = new ArrayList<>(threadCount);
                for (int j = 0; j < threadCount; j++) {
                    futures.add(pool.submit(() -> {
                        barrier.await();
                        shared.close();
                        return null;
                    }));
                }
                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        escaped.add(e.getCause());
                    }
                }
            }
        } finally {
            pool.shutdown();
            pool.awaitTermination(30, TimeUnit.SECONDS);
        }
        assertThat("expected no races decRef'ing the shared block", escaped, empty());
    }
}
