/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// Creates a bunch of readers and writers to verify the thread-safety of ComputeSearchContextByShardId.
public class ComputeSearchContextByShardIdTests extends ESTestCase {
    private final int nThreads = Runtime.getRuntime().availableProcessors() * 2;
    private final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

    private static final int MAX_CONTEXTS = 1000;
    private final ComputeSearchContextByShardId contexts = new ComputeSearchContextByShardId(MAX_CONTEXTS);

    private static final int CHUNK_SIZE = 10;

    @After
    public void tearDown() throws Exception {
        executorService.shutdown();
        super.tearDown();
    }

    public void testFoo() throws Exception {
        List<ComputeSearchContext> expected = java.util.Collections.synchronizedList(new ArrayList<>());
        Collection<Future<?>> readers = new ArrayList<>();
        var shouldContinue = new AtomicBoolean(true);
        for (int i = 0; i < nThreads / 2; i++) {
            readers.add(executorService.submit(newReader(shouldContinue, contexts)));
        }
        int numWriters = MAX_CONTEXTS / CHUNK_SIZE;
        var cdl = new CountDownLatch(numWriters);
        for (int i = 0; i < numWriters; i++) {
            executorService.submit(newWriter(expected, cdl));
        }
        cdl.await();
        var actual = new ArrayList<ComputeSearchContext>();
        contexts.iterable().forEach(actual::add);
        assertEquals(expected, actual);
        shouldContinue.set(false);
        // Verify no exceptions from the reading tasks
        for (Future<?> future : readers) {
            future.get();
        }
    }

    private Runnable newWriter(List<ComputeSearchContext> expected, CountDownLatch cdl) {
        return () -> {
            synchronized (contexts) { // This is the way the class is used in production (lock on the whole instance when adding multiple)
                for (int chunkIndex = 0; chunkIndex < CHUNK_SIZE; chunkIndex++) {
                    try {
                        var context = createComputeSearchContext();
                        expected.add(context);
                        contexts.add(context);
                        Thread.sleep(randomInt(10));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            cdl.countDown();
        };
    }

    private static Runnable newReader(AtomicBoolean shouldContinue, ComputeSearchContextByShardId contexts) {
        return () -> {
            while (shouldContinue.get()) {
                int shardId = READER_RANDOM.nextInt(MAX_CONTEXTS);
                try {
                    ComputeSearchContext actual = contexts.get(shardId);
                    assertThat(actual.index(), greaterThanOrEqualTo(0));
                    assertThat(actual.searchContext(), notNullValue());
                    assertThat(((TestComputeSearchContext) actual).field, notNullValue());
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

    // Extra hack to ensure some fields are non-final, which makes it more likely to catch unsafe publishing issues.
    private static class TestComputeSearchContext extends ComputeSearchContext {
        Object field = null;

        TestComputeSearchContext(int index, SearchContext searchContext) {
            super(index, searchContext);
        }
    }

    private static ComputeSearchContext createComputeSearchContext() {
        var result = new TestComputeSearchContext(
            Math.abs(randomInt()),
            Mockito.mock(SearchContext.class, Mockito.withSettings().stubOnly())
        );
        result.field = new Object();
        return result;
    }
}
