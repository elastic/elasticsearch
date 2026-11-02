/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.hasSize;

/**
 * Tests the generic parsed-footer cache without depending on any specific format. The cache treats
 * values as opaque, so a {@link String} stand-in for the format-specific metadata type
 * ({@code ParquetMetadata}, {@code OrcTail}, ...) is sufficient to exercise every invariant.
 */
public class ParsedFooterCacheTests extends ESTestCase {

    private ParsedFooterCache<String> cache;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        cache = new ParsedFooterCache<>(8);
    }

    public void testGetReturnsNullOnMiss() {
        assertNull(cache.get(key("file.parquet", 1000)));
    }

    public void testGetOrLoadPopulatesCache() throws ExecutionException {
        FooterByteCache.Key k = key("file.parquet", 1000);
        String expected = "footer-1";
        String result = cache.getOrLoad(k, ignore -> expected);
        assertSame(expected, result);
        assertSame(expected, cache.get(k));
    }

    public void testGetOrLoadInvokesLoaderOnce() throws ExecutionException {
        FooterByteCache.Key k = key("file.parquet", 1000);
        String first = "first";
        AtomicInteger loadCount = new AtomicInteger();
        cache.getOrLoad(k, ignore -> {
            loadCount.incrementAndGet();
            return first;
        });
        String second = cache.getOrLoad(k, ignore -> {
            loadCount.incrementAndGet();
            return "second";
        });
        assertEquals("loader invoked only on cache miss", 1, loadCount.get());
        assertSame(first, second);
    }

    public void testSamePathDifferentLengthAreDifferentKeys() throws ExecutionException {
        FooterByteCache.Key k1 = key("file.parquet", 1000);
        FooterByteCache.Key k2 = key("file.parquet", 2000);
        String v1 = "v1";
        String v2 = "v2";
        cache.getOrLoad(k1, ignore -> v1);
        cache.getOrLoad(k2, ignore -> v2);
        assertSame(v1, cache.get(k1));
        assertSame(v2, cache.get(k2));
    }

    public void testCapacityEvictionPreservesNewestEntries() throws ExecutionException {
        // Verifies the cap-by-entry-count invariant: once the capacity is exceeded, the most
        // recent loads remain available. The exact eviction order is delegated to ES Cache and
        // is intentionally not asserted here beyond "the newest survives".
        ParsedFooterCache<String> tiny = new ParsedFooterCache<>(2);
        FooterByteCache.Key k1 = key("a.parquet", 1);
        FooterByteCache.Key k2 = key("b.parquet", 2);
        FooterByteCache.Key k3 = key("c.parquet", 3);
        tiny.getOrLoad(k1, ignore -> "1");
        tiny.getOrLoad(k2, ignore -> "2");
        tiny.getOrLoad(k3, ignore -> "3");
        assertNull("oldest entry evicted once budget is exceeded", tiny.get(k1));
        assertNotNull(tiny.get(k3));
    }

    public void testInvalidateAll() throws ExecutionException {
        FooterByteCache.Key k = key("file.parquet", 1000);
        cache.getOrLoad(k, ignore -> "v");
        assertNotNull(cache.get(k));
        cache.invalidateAll();
        assertNull(cache.get(k));
    }

    /**
     * Verifies thundering-herd protection: concurrent {@code getOrLoad} calls for the same key
     * invoke the loader exactly once. This is the core invariant — without it the parse would
     * still run N times under the producer fan-out pattern that motivated this cache.
     */
    public void testThunderingHerdCoalescesConcurrentLoads() throws Exception {
        FooterByteCache.Key k = key("shared.parquet", 5000);
        String expected = "winner";
        AtomicInteger loadCount = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        int threadCount = randomIntBetween(4, 16);
        AtomicReference<AssertionError> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await(10, TimeUnit.SECONDS);
                    String result = cache.getOrLoad(k, ignore -> {
                        loadCount.incrementAndGet();
                        return expected;
                    });
                    assertSame(expected, result);
                } catch (AssertionError e) {
                    failure.compareAndSet(null, e);
                } catch (Exception e) {
                    failure.compareAndSet(null, new AssertionError("Unexpected exception", e));
                }
            }, "herd-" + i);
            t.start();
            threads.add(t);
        }
        start.countDown();
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse("Thread " + t.getName() + " did not finish in time", t.isAlive());
        }
        AssertionError err = failure.get();
        if (err != null) {
            throw err;
        }
        assertEquals("loader invoked exactly once across all concurrent callers", 1, loadCount.get());
    }

    /**
     * Every caller of a load that trips a breaker receives the loader's own exception from {@link ParsedFooterCache#getOrLoad};
     * {@link ParsedFooterCache#rethrowStructural} is where the sharing is broken, so each caller is routed through it and must
     * get its own instance.
     */
    public void testCopiedBreakerFailureKeepsItsCause() {
        IllegalStateException cause = new IllegalStateException("cause");
        CircuitBreakingException shared = new CircuitBreakingException("[parent] Data too large", CircuitBreaker.Durability.TRANSIENT);
        shared.initCause(cause);

        CircuitBreakingException copy = expectThrows(
            CircuitBreakingException.class,
            () -> ParsedFooterCache.rethrowStructural(new ExecutionException(shared))
        );

        assertNotSame(shared, copy);
        assertSame(cause, copy.getCause());
    }

    public void testConcurrentCallersOnAFailedLoadReceiveTheirOwnException() throws Exception {
        FooterByteCache.Key k = key("tripped.parquet", 1000);
        CircuitBreakingException boom = new CircuitBreakingException(
            "[parent] Data too large, data for [parquet reader]",
            1024,
            512,
            CircuitBreaker.Durability.TRANSIENT
        );
        int threadCount = randomIntBetween(4, 16);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<AssertionError> failure = new AtomicReference<>();
        Queue<CircuitBreakingException> thrown = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await(10, TimeUnit.SECONDS);
                    ExecutionException ex = expectThrows(ExecutionException.class, () -> cache.getOrLoad(k, ignore -> { throw boom; }));
                    thrown.add(expectThrows(CircuitBreakingException.class, () -> ParsedFooterCache.rethrowStructural(ex)));
                } catch (AssertionError e) {
                    failure.compareAndSet(null, e);
                } catch (Exception e) {
                    failure.compareAndSet(null, new AssertionError("Unexpected exception", e));
                }
            }, "herd-cbe-" + i);
            t.start();
            threads.add(t);
        }
        start.countDown();
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse("Thread " + t.getName() + " did not finish in time", t.isAlive());
        }
        AssertionError err = failure.get();
        if (err != null) {
            throw err;
        }

        assertThat(thrown, hasSize(threadCount));
        Set<CircuitBreakingException> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        for (CircuitBreakingException e : thrown) {
            assertNotSame(boom, e);
            assertTrue("each caller must get its own instance", distinct.add(e));
            assertEquals(boom.getMessage(), e.getMessage());
            assertEquals(boom.getBytesWanted(), e.getBytesWanted());
            assertEquals(boom.getByteLimit(), e.getByteLimit());
            assertEquals(boom.getDurability(), e.getDurability());
            assertArrayEquals(boom.getStackTrace(), e.getStackTrace());
        }
    }

    public void testGetOrLoadPropagatesLoaderException() {
        FooterByteCache.Key k = key("bad.parquet", 1000);
        ExecutionException ex = expectThrows(ExecutionException.class, () -> cache.getOrLoad(k, ignore -> {
            throw new RuntimeException("simulated parse failure");
        }));
        assertNotNull(ex.getCause());
        assertEquals("simulated parse failure", ex.getCause().getMessage());
    }

    public void testGetOrLoadFailsWhenLoaderReturnsNull() {
        // The cache documents that {@code getOrLoad} surfaces an ExecutionException if the loader
        // returns null; verify that the underlying ES Cache contract still holds for callers.
        FooterByteCache.Key k = key("null.parquet", 1000);
        expectThrows(ExecutionException.class, () -> cache.getOrLoad(k, ignore -> null));
        assertNull("a failed load must not leave a phantom entry behind", cache.get(k));
    }

    public void testConstructorRejectsNonPositiveMaxEntries() {
        expectThrows(IllegalArgumentException.class, () -> new ParsedFooterCache<String>(0));
        expectThrows(IllegalArgumentException.class, () -> new ParsedFooterCache<String>(-1));
    }

    private static FooterByteCache.Key key(String path, long length) {
        return new FooterByteCache.Key(AbstractTestStorageObject.NOOP, path, length);
    }
}
