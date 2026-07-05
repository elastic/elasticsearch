/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MapMatcher;
import org.junit.After;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

// Creates a bunch of readers and writers to verify the thread-safety of ComputeSearchContextByShardId.
public class ComputeSearchContextByShardIdTests extends ESTestCase {
    private final int nThreads = Runtime.getRuntime().availableProcessors() * 2;
    private final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

    private static final int MAX_CONTEXTS = 1000;
    private final AcquiredSearchContexts contexts = new AcquiredSearchContexts(MAX_CONTEXTS);
    private final IndexedByShardId<ComputeSearchContext> globalView = contexts.globalView();

    private static final int CHUNK_SIZE = 10;

    @After
    public void tearDown() throws Exception {
        executorService.shutdown();
        super.tearDown();
    }

    public void testMultithreadedSafety() throws Exception {
        var expected = new ConcurrentHashMap<Integer, ComputeSearchContext>();
        Collection<Future<?>> readers = new ArrayList<>();
        var shouldContinue = new AtomicBoolean(true);
        for (int i = 0; i < nThreads / 2; i++) {
            readers.add(executorService.submit(newReader(shouldContinue, globalView)));
        }
        int numWriters = MAX_CONTEXTS / CHUNK_SIZE;
        var cdl = new CountDownLatch(numWriters);
        for (int i = 0; i < numWriters; i++) {
            executorService.submit(newWriter(expected, cdl));
        }
        cdl.await();
        var actual = new HashMap<Integer, ComputeSearchContext>();
        contexts.globalView().iterable().forEach(e -> actual.put(e.index(), e));
        assertEquals(expected, actual);
        MapMatcher.assertMap(actual, MapMatcher.matchesMap(expected));
        shouldContinue.set(false);
        // Verify no exceptions from the reading tasks
        for (Future<?> future : readers) {
            future.get();
        }
    }

    private Runnable newWriter(ConcurrentHashMap<Integer, ComputeSearchContext> expected, CountDownLatch cdl) {
        return () -> {
            List<SearchContext> newContexts = IntStream.range(0, CHUNK_SIZE)
                .mapToObj(i -> Mockito.mock(SearchContext.class, Mockito.withSettings().stubOnly()))
                .toList();
            contexts.newSubRangeView(newContexts).iterable().forEach(e -> expected.put(e.index(), e));
            try {
                Thread.sleep(randomInt(10));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            cdl.countDown();
        };
    }

    private static Runnable newReader(AtomicBoolean shouldContinue, IndexedByShardId<ComputeSearchContext> globalContexts) {
        return () -> {
            while (shouldContinue.get()) {
                int shardId = READER_RANDOM.nextInt(MAX_CONTEXTS);
                try {
                    ComputeSearchContext actual = globalContexts.get(shardId);
                    assertThat(actual.index(), equalTo(shardId));
                    assertThat(actual.searchContext(), notNullValue());
                    try {
                        Thread.sleep(READER_RANDOM.nextInt(5));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } catch (IndexOutOfBoundsException e) {/* ignore */}
            }
        };
    }

    // We explicitly use the non-shared Random to avoid synchronization.
    private static final Random READER_RANDOM = new Random(0);

}
