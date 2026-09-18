/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests the generic parsed-footer cache without depending on any specific format. The cache treats
 * values as opaque, so a {@link String} stand-in for the format-specific metadata type
 * ({@code ParquetMetadata}, {@code OrcTail}, ...) is sufficient to exercise every invariant.
 */
public class ParsedFooterCacheTests extends ESTestCase {

    private static final TimeValue TTL = TimeValue.timeValueMinutes(5);

    /** Weighs each String at 1 KiB so byte budgets translate to predictable entry counts. */
    private static final long ENTRY_WEIGHT = 1024;

    /**
     * Longer than {@code assertBusy}'s 10s default. The first load is held open until waiters park;
     * if this latch timed out first the future would complete and waiters would look TERMINATED.
     */
    private static final TimeValue LOADER_HOLD_TIMEOUT = TimeValue.timeValueSeconds(30);

    private ParsedFooterCache<String> cache;

    @Before
    public void initCache() {
        cache = new ParsedFooterCache<>(8 * ENTRY_WEIGHT, TTL, ignored -> ENTRY_WEIGHT);
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

    public void testPutThenGetReturnsSameInstance() {
        FooterByteCache.Key k = key("file.parquet", 1000);
        String footer = "seeded";
        cache.put(k, footer);
        assertSame(footer, cache.get(k));
    }

    public void testGetOrLoadAfterPutDoesNotInvokeLoader() throws ExecutionException {
        FooterByteCache.Key k = key("file.parquet", 1000);
        String seeded = "seeded";
        cache.put(k, seeded);
        AtomicInteger loadCount = new AtomicInteger();
        String result = cache.getOrLoad(k, ignore -> {
            loadCount.incrementAndGet();
            return "loaded";
        });
        assertEquals("loader must not run after an explicit seed", 0, loadCount.get());
        assertSame(seeded, result);
    }

    public void testPutReplacesPreviousValue() {
        FooterByteCache.Key k = key("file.parquet", 1000);
        String first = "first";
        String second = "second";
        cache.put(k, first);
        cache.put(k, second);
        assertSame(second, cache.get(k));
    }

    public void testPutRejectsNullValue() {
        FooterByteCache.Key k = key("file.parquet", 1000);
        expectThrows(IllegalArgumentException.class, () -> cache.put(k, null));
        assertNull("a rejected put must not leave a phantom entry", cache.get(k));
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
        // Verifies the cap-by-weight invariant: once the byte budget is exceeded, the most
        // recent loads remain available. The exact eviction order is delegated to ES Cache and
        // is intentionally not asserted here beyond "the newest survives".
        ParsedFooterCache<String> tiny = new ParsedFooterCache<>(2 * ENTRY_WEIGHT, TTL, ignored -> ENTRY_WEIGHT);
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
     * The first load is held open until the waiters are observed blocked, so a later cache hit
     * cannot masquerade as coalescing.
     */
    public void testThunderingHerdCoalescesConcurrentLoads() throws Exception {
        FooterByteCache.Key k = key("shared.parquet", 5000);
        String expected = "winner";
        AtomicInteger loadCount = new AtomicInteger();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicReference<AssertionError> failure = new AtomicReference<>();

        Thread loaderThread = startHerdThread("herd-loader", failure, () -> {
            String result = cache.getOrLoad(k, ignore -> {
                loadCount.incrementAndGet();
                loaderStarted.countDown();
                safeAwait(releaseLoader, LOADER_HOLD_TIMEOUT);
                return expected;
            });
            assertSame(expected, result);
        });
        safeAwait(loaderStarted);

        int waiterCount = randomIntBetween(3, 15);
        List<Thread> waiters = new ArrayList<>(waiterCount);
        for (int i = 0; i < waiterCount; i++) {
            waiters.add(startHerdThread("herd-waiter-" + i, failure, () -> {
                String result = cache.getOrLoad(k, ignore -> {
                    loadCount.incrementAndGet();
                    return "should-not-run";
                });
                assertSame(expected, result);
            }));
        }
        try {
            awaitBlockedOnInFlight(waiters, failure);
        } finally {
            releaseAndJoinHerd(releaseLoader, loaderThread, waiters, failure);
        }
        assertEquals("loader invoked exactly once across all concurrent callers", 1, loadCount.get());
        assertSame(expected, cache.get(k));
    }

    public void testThunderingHerdPropagatesLoaderFailureAndClearsInFlight() throws Exception {
        FooterByteCache.Key k = key("bad.parquet", 1000);
        RuntimeException boom = new RuntimeException("simulated parse failure");
        AtomicInteger loadCount = new AtomicInteger();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicReference<AssertionError> failure = new AtomicReference<>();

        Thread loaderThread = startHerdThread("herd-fail-loader", failure, () -> {
            ExecutionException ex = expectThrows(ExecutionException.class, () -> cache.getOrLoad(k, ignore -> {
                loadCount.incrementAndGet();
                loaderStarted.countDown();
                safeAwait(releaseLoader, LOADER_HOLD_TIMEOUT);
                throw boom;
            }));
            assertSame(boom, ex.getCause());
        });
        safeAwait(loaderStarted);

        int waiterCount = randomIntBetween(3, 15);
        List<Thread> waiters = new ArrayList<>(waiterCount);
        for (int i = 0; i < waiterCount; i++) {
            waiters.add(startHerdThread("herd-fail-waiter-" + i, failure, () -> {
                ExecutionException ex = expectThrows(ExecutionException.class, () -> cache.getOrLoad(k, ignore -> {
                    loadCount.incrementAndGet();
                    return "should-not-run";
                }));
                assertSame(boom, ex.getCause());
            }));
        }
        try {
            awaitBlockedOnInFlight(waiters, failure);
        } finally {
            releaseAndJoinHerd(releaseLoader, loaderThread, waiters, failure);
        }
        assertEquals("failed load still coalesced to a single loader invocation", 1, loadCount.get());
        assertNull("a failed load must not leave a phantom entry behind", cache.get(k));

        AtomicInteger retryCount = new AtomicInteger();
        String recoveredValue = "recovered";
        String recovered = cache.getOrLoad(k, ignore -> {
            retryCount.incrementAndGet();
            return recoveredValue;
        });
        assertEquals("in-flight entry must be cleared so a later call loads again", 1, retryCount.get());
        assertSame(recoveredValue, recovered);
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

    public void testConstructorRejectsNonPositiveMaxWeight() {
        expectThrows(IllegalArgumentException.class, () -> new ParsedFooterCache<String>(0, TTL, ignored -> ENTRY_WEIGHT));
        expectThrows(IllegalArgumentException.class, () -> new ParsedFooterCache<String>(-1, TTL, ignored -> ENTRY_WEIGHT));
    }

    public void testWeigherDrivesEviction() throws ExecutionException {
        // A single entry weighing more than half the budget forces the next insert to evict it:
        // eviction tracks bytes reported by the weigher, not entry counts.
        ParsedFooterCache<String> weighted = new ParsedFooterCache<>(1000, TTL, v -> v.length() * 100L);
        FooterByteCache.Key big = key("big.parquet", 1);
        FooterByteCache.Key alsoBig = key("also-big.parquet", 2);
        weighted.getOrLoad(big, ignore -> "sevenchr"); // 8 chars -> 800 bytes
        weighted.getOrLoad(alsoBig, ignore -> "sixchar!"); // 8 chars -> 800 bytes, exceeds 1000 budget
        assertNull("heavier-than-half entry evicted by the next big insert", weighted.get(big));
        assertNotNull(weighted.get(alsoBig));
    }

    /**
     * A value weighing more than the entire budget must never be inserted: the backing Cache would
     * link it at the LRU head and then prune from the tail until the weight fits, discarding the
     * whole working set and finally the new entry itself, leaving an empty cache.
     */
    public void testPutSkipsEntryHeavierThanBudget() {
        ParsedFooterCache<String> weighted = new ParsedFooterCache<>(1000, TTL, v -> v.length() * 100L);
        FooterByteCache.Key small = key("small.parquet", 1);
        FooterByteCache.Key oversized = key("wide.parquet", 2);

        weighted.put(small, "abc"); // 300 bytes
        weighted.put(oversized, "eleven chrs"); // 11 chars -> 1100 bytes, over the 1000 budget

        assertNull("an entry heavier than the budget must not be cached", weighted.get(oversized));
        assertEquals("the existing working set must survive the refused insert", "abc", weighted.get(small));
    }

    public void testGetOrLoadSkipsEntryHeavierThanBudget() throws ExecutionException {
        ParsedFooterCache<String> weighted = new ParsedFooterCache<>(1000, TTL, v -> v.length() * 100L);
        FooterByteCache.Key small = key("small.parquet", 1);
        FooterByteCache.Key oversized = key("wide.parquet", 2);

        weighted.put(small, "abc");
        assertEquals("eleven chrs", weighted.getOrLoad(oversized, ignored -> "eleven chrs"));

        assertNull("an entry heavier than the budget must not be cached", weighted.get(oversized));
        assertEquals("the existing working set must survive the refused load", "abc", weighted.get(small));
    }

    /** An entry weighing exactly the budget still fits: the Cache prunes only while weight > budget. */
    public void testPutAdmitsEntryWeighingExactlyTheBudget() {
        ParsedFooterCache<String> weighted = new ParsedFooterCache<>(1000, TTL, v -> v.length() * 100L);
        FooterByteCache.Key exact = key("exact.parquet", 1);
        weighted.put(exact, "ten chars!"); // 10 chars -> 1000 bytes
        assertEquals("ten chars!", weighted.get(exact));
    }

    public void testFromSettingsBuildsWorkingCache() throws ExecutionException {
        ParsedFooterCache<String> fromSettings = ParsedFooterCache.fromSettings(Settings.EMPTY, ignored -> ENTRY_WEIGHT);
        FooterByteCache.Key k = key("file.parquet", 1000);
        assertSame("v", fromSettings.getOrLoad(k, ignore -> "v"));
        assertSame("v", fromSettings.get(k));
    }

    private static FooterByteCache.Key key(String path, long length) {
        return new FooterByteCache.Key(path, length);
    }

    private static Thread startHerdThread(String name, AtomicReference<AssertionError> failure, CheckedRunnable<Exception> body) {
        Thread t = new Thread(() -> {
            try {
                body.run();
            } catch (AssertionError e) {
                failure.compareAndSet(null, e);
            } catch (Exception e) {
                failure.compareAndSet(null, new AssertionError("Unexpected exception", e));
            }
        }, name);
        t.start();
        return t;
    }

    /**
     * Wait until every waiter is parked inside {@code getOrLoad}. Releasing the first load before
     * that would let a later caller hit the cache and the test would pass even if concurrent
     * misses were not coalesced.
     */
    private static void awaitBlockedOnInFlight(List<Thread> waiters, AtomicReference<AssertionError> failure) throws Exception {
        assertBusy(() -> {
            for (Thread t : waiters) {
                Thread.State state = t.getState();
                if (state == Thread.State.TERMINATED) {
                    // IllegalStateException is not retried by assertBusy; a finished waiter will never park.
                    throw new IllegalStateException(
                        "waiter " + t.getName() + " finished without joining the in-flight load",
                        failure.get()
                    );
                }
                assertEquals(
                    "waiter " + t.getName() + " should be blocked on the in-flight load, was " + state,
                    Thread.State.WAITING,
                    state
                );
            }
        });
    }

    private static void releaseAndJoinHerd(
        CountDownLatch releaseLoader,
        Thread loaderThread,
        List<Thread> waiters,
        AtomicReference<AssertionError> failure
    ) throws InterruptedException {
        releaseLoader.countDown();
        List<Thread> all = new ArrayList<>(waiters.size() + 1);
        all.add(loaderThread);
        all.addAll(waiters);
        joinHerd(all, failure);
    }

    private static void joinHerd(List<Thread> threads, AtomicReference<AssertionError> failure) throws InterruptedException {
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse("Thread " + t.getName() + " did not finish in time", t.isAlive());
        }
        AssertionError err = failure.get();
        if (err != null) {
            throw err;
        }
    }
}
