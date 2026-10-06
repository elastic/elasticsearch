/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.ExternalIoExecutors;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.hasSize;

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

    /**
     * Every caller of a load that trips a breaker receives the loader's own exception from {@link ParsedFooterCache#getOrLoad}
     * (see {@link #testThunderingHerdPropagatesLoaderFailureAndClearsInFlight}); {@link ParsedFooterCache#rethrowStructural}
     * is where the sharing is broken, so each caller is routed through it and must get its own instance.
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

    public void testWaitersOnAFailedLoadReceiveTheirOwnException() throws Exception {
        FooterByteCache.Key k = key("tripped.parquet", 1000);
        CircuitBreakingException boom = new CircuitBreakingException(
            "[parent] Data too large, data for [parquet reader]",
            1024,
            512,
            CircuitBreaker.Durability.TRANSIENT
        );
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicReference<AssertionError> failure = new AtomicReference<>();
        Queue<CircuitBreakingException> thrown = new ConcurrentLinkedQueue<>();

        Thread loaderThread = startHerdThread("herd-cbe-loader", failure, () -> {
            ExecutionException ex = expectThrows(ExecutionException.class, () -> cache.getOrLoad(k, ignore -> {
                loaderStarted.countDown();
                safeAwait(releaseLoader, LOADER_HOLD_TIMEOUT);
                throw boom;
            }));
            thrown.add(expectThrows(CircuitBreakingException.class, () -> ParsedFooterCache.rethrowStructural(ex)));
        });
        safeAwait(loaderStarted);

        int waiterCount = randomIntBetween(3, 15);
        List<Thread> waiters = new ArrayList<>(waiterCount);
        for (int i = 0; i < waiterCount; i++) {
            waiters.add(startHerdThread("herd-cbe-waiter-" + i, failure, () -> {
                ExecutionException ex = expectThrows(ExecutionException.class, () -> cache.getOrLoad(k, ignore -> {
                    throw new AssertionError("waiter must not load");
                }));
                thrown.add(expectThrows(CircuitBreakingException.class, () -> ParsedFooterCache.rethrowStructural(ex)));
            }));
        }
        try {
            awaitBlockedOnInFlight(waiters, failure);
        } finally {
            releaseAndJoinHerd(releaseLoader, loaderThread, waiters, failure);
        }

        assertThat(thrown, hasSize(waiterCount + 1));
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

    public void testFromSettingsReadsCoalesceSetting() {
        FooterByteCache.Key k = key("file.parquet", 1000);
        ParsedFooterCache<String> coalescing = ParsedFooterCache.fromSettings(Settings.EMPTY, ignored -> ENTRY_WEIGHT);
        AtomicInteger loads = new AtomicInteger();
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        coalescing.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loads.incrementAndGet();
            held.set(l);
        }, leader);
        PlainActionFuture<String> waiter = new PlainActionFuture<>();
        coalescing.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loads.incrementAndGet();
            l.onResponse("no");
        }, waiter);
        held.get().onResponse("v");
        assertEquals(1, loads.get());
        assertSame("v", leader.actionGet(0, TimeUnit.SECONDS));
        assertSame("v", waiter.actionGet(0, TimeUnit.SECONDS));

        ParsedFooterCache<String> threeArg = new ParsedFooterCache<>(8 * ENTRY_WEIGHT, TTL, ignored -> ENTRY_WEIGHT);
        AtomicInteger threeArgLoads = new AtomicInteger();
        AtomicReference<ActionListener<String>> threeArgHeld = new AtomicReference<>();
        PlainActionFuture<String> threeArgLeader = new PlainActionFuture<>();
        threeArg.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            threeArgLoads.incrementAndGet();
            threeArgHeld.set(l);
        }, threeArgLeader);
        PlainActionFuture<String> threeArgWaiter = new PlainActionFuture<>();
        threeArg.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            threeArgLoads.incrementAndGet();
            l.onResponse("no");
        }, threeArgWaiter);
        threeArgHeld.get().onResponse("default-on");
        assertEquals("3-arg constructor defaults coalesce on", 1, threeArgLoads.get());
        assertSame("default-on", threeArgWaiter.actionGet(0, TimeUnit.SECONDS));

        ParsedFooterCache<String> disabled = ParsedFooterCache.fromSettings(
            Settings.builder().put(ExternalSourceCacheSettings.FOOTER_COALESCE.getKey(), false).build(),
            ignored -> ENTRY_WEIGHT
        );
        AtomicInteger disabledLoads = new AtomicInteger();
        List<ActionListener<String>> heldLoads = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            disabled.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
                disabledLoads.incrementAndGet();
                heldLoads.add(l);
            }, new PlainActionFuture<>());
        }
        assertEquals("coalesce off starts one load per caller", 3, disabledLoads.get());
        heldLoads.forEach(l -> l.onResponse("v"));
        assertEquals(0, disabled.asyncInFlightCount());
    }

    /**
     * Fourteen sequential attaches against a held-open loader must share one load and the same
     * instance, and the in-flight map must drain before waiters run.
     */
    public void testAsyncFourteenCallersCoalesceToOneLoad() {
        FooterByteCache.Key k = key("shared.parquet", 5000);
        String expected = "winner";
        AtomicInteger loadCount = new AtomicInteger();
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loadCount.incrementAndGet();
            held.set(l);
        }, leader);

        List<DeterministicTaskQueue> queues = new ArrayList<>();
        List<PlainActionFuture<String>> waiters = new ArrayList<>();
        for (int i = 0; i < 13; i++) {
            DeterministicTaskQueue queue = new DeterministicTaskQueue();
            queues.add(queue);
            PlainActionFuture<String> future = new PlainActionFuture<>();
            waiters.add(future);
            cache.getOrLoadAsync(k, queue::scheduleNow, l -> {
                loadCount.incrementAndGet();
                l.onResponse("should-not-run");
            }, future);
            assertFalse("waiter must not complete before the load", future.isDone());
        }
        assertEquals(1, loadCount.get());
        assertEquals(1, cache.asyncInFlightCount());

        AtomicBoolean sawEmptyMap = new AtomicBoolean();
        PlainActionFuture<String> directWaiter = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loadCount.incrementAndGet();
            l.onResponse("should-not-run");
        }, ActionListener.wrap(v -> {
            assertEquals("remove before notify: DIRECT waiter runs inside flight completion", 0, cache.asyncInFlightCount());
            sawEmptyMap.set(true);
            directWaiter.onResponse(v);
        }, directWaiter::onFailure));

        held.get().onResponse(expected);
        assertSame(expected, leader.actionGet(0, TimeUnit.SECONDS));
        assertSame(expected, directWaiter.actionGet(0, TimeUnit.SECONDS));
        assertTrue(sawEmptyMap.get());
        assertSame("put then remove then notify: cache visible before waiters run", expected, cache.get(k));
        assertEquals(0, cache.asyncInFlightCount());
        for (PlainActionFuture<String> waiter : waiters) {
            assertFalse("waiters complete on their own executor, not inline", waiter.isDone());
        }
        for (int i = 0; i < waiters.size(); i++) {
            queues.get(i).runAllRunnableTasks();
            assertSame(expected, waiters.get(i).actionGet(0, TimeUnit.SECONDS));
        }
        assertEquals(1, loadCount.get());
        assertEquals(0, cache.asyncInFlightCount());
    }

    public void testAsyncRepeeksAndRemovesWhenCacheFilled() {
        FooterByteCache.Key k = key("seeded.parquet", 1000);
        String seeded = "seeded";
        cache.put(k, seeded);
        AtomicInteger loads = new AtomicInteger();
        PlainActionFuture<String> future = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loads.incrementAndGet();
            l.onResponse("loaded");
        }, future);
        assertEquals("re-peek hit must not invoke the loader", 0, loads.get());
        assertSame(seeded, future.actionGet(0, TimeUnit.SECONDS));
        assertEquals("re-peek hit must remove the flight", 0, cache.asyncInFlightCount());
    }

    public void testAsyncOversizedReturnedNotAdmitted() {
        ParsedFooterCache<String> weighted = new ParsedFooterCache<>(1000, TTL, v -> v.length() * 100L);
        FooterByteCache.Key small = key("small.parquet", 1);
        FooterByteCache.Key oversized = key("wide.parquet", 2);
        weighted.put(small, "abc");
        PlainActionFuture<String> future = new PlainActionFuture<>();
        weighted.getOrLoadAsync(oversized, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> l.onResponse("eleven chrs"), future);
        assertEquals("eleven chrs", future.actionGet(0, TimeUnit.SECONDS));
        assertNull("an entry heavier than the budget must not be cached", weighted.get(oversized));
        assertEquals("abc", weighted.get(small));
        assertEquals(0, weighted.asyncInFlightCount());
    }

    public void testAsyncFailureNotCachedWaitersCoalesceIntoOneRetry() {
        FooterByteCache.Key k = key("bad.parquet", 1000);
        RuntimeException boom = new RuntimeException("simulated parse failure");
        String recovered = "recovered";
        AtomicInteger loads = new AtomicInteger();
        List<ActionListener<String>> heldLoads = new ArrayList<>();
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loads.incrementAndGet();
            heldLoads.add(l);
        }, leader);

        List<DeterministicTaskQueue> queues = new ArrayList<>();
        List<PlainActionFuture<String>> waiters = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            DeterministicTaskQueue queue = new DeterministicTaskQueue();
            queues.add(queue);
            PlainActionFuture<String> future = new PlainActionFuture<>();
            waiters.add(future);
            cache.getOrLoadAsync(k, queue::scheduleNow, l -> {
                loads.incrementAndGet();
                heldLoads.add(l);
            }, future);
        }
        heldLoads.get(0).onFailure(boom);
        assertSame(boom, expectThrows(RuntimeException.class, () -> leader.actionGet(0, TimeUnit.SECONDS)));
        assertNull(cache.get(k));
        assertEquals(0, cache.asyncInFlightCount());

        for (DeterministicTaskQueue queue : queues) {
            queue.runAllRunnableTasks();
        }
        assertEquals("waiters coalesce into one retry load", 2, loads.get());
        assertEquals(1, cache.asyncInFlightCount());
        heldLoads.get(1).onResponse(recovered);
        for (DeterministicTaskQueue queue : queues) {
            queue.runAllRunnableTasks();
        }
        for (PlainActionFuture<String> waiter : waiters) {
            assertSame(recovered, waiter.actionGet(0, TimeUnit.SECONDS));
        }
        assertSame(recovered, cache.get(k));
        assertEquals(0, cache.asyncInFlightCount());
    }

    public void testAsyncNextCallAfterFailedLoadLoadsAgain() {
        FooterByteCache.Key k = key("again.parquet", 1000);
        RuntimeException boom = new RuntimeException("failed");
        AtomicInteger loads = new AtomicInteger();
        PlainActionFuture<String> first = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loads.incrementAndGet();
            l.onFailure(boom);
        }, first);
        assertSame(boom, expectThrows(RuntimeException.class, () -> first.actionGet(0, TimeUnit.SECONDS)));
        assertNull("a failed load must not be cached", cache.get(k));
        PlainActionFuture<String> second = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loads.incrementAndGet();
            l.onResponse("recovered");
        }, second);
        assertEquals("next call after a drained failure loads again", 2, loads.get());
        assertEquals("recovered", second.actionGet(0, TimeUnit.SECONDS));
        assertEquals(0, cache.asyncInFlightCount());
    }

    public void testAsyncRetryFlightFailureIsFinalForThoseSubscribersFreshCallerStillRetries() {
        FooterByteCache.Key k = key("retry.parquet", 1000);
        RuntimeException first = new RuntimeException("first");
        RuntimeException retryBoom = new RuntimeException("retry");
        AtomicInteger loads = new AtomicInteger();
        List<ActionListener<String>> heldLoads = new ArrayList<>();
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loads.incrementAndGet();
            heldLoads.add(l);
        }, leader);

        DeterministicTaskQueue waiterQueue = new DeterministicTaskQueue();
        PlainActionFuture<String> waiter = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, waiterQueue::scheduleNow, l -> {
            loads.incrementAndGet();
            heldLoads.add(l);
        }, waiter);

        heldLoads.get(0).onFailure(first);
        waiterQueue.runAllRunnableTasks();
        assertEquals(2, loads.get());
        assertFalse(waiter.isDone());

        DeterministicTaskQueue freshQueue = new DeterministicTaskQueue();
        PlainActionFuture<String> fresh = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, freshQueue::scheduleNow, l -> {
            loads.incrementAndGet();
            heldLoads.add(l);
        }, fresh);

        heldLoads.get(1).onFailure(retryBoom);
        waiterQueue.runAllRunnableTasks();
        assertSame(retryBoom, expectThrows(RuntimeException.class, () -> waiter.actionGet(0, TimeUnit.SECONDS)));

        freshQueue.runAllRunnableTasks();
        assertEquals("fresh caller whose first attach was the retry flight still retries once", 3, loads.get());
        heldLoads.get(2).onResponse("ok");
        freshQueue.runAllRunnableTasks();
        assertEquals("ok", fresh.actionGet(0, TimeUnit.SECONDS));
        assertEquals(0, cache.asyncInFlightCount());
    }

    public void testAsyncCancelledWaiterDoesNotRetry() {
        FooterByteCache.Key k = key("cancel.parquet", 1000);
        RuntimeException boom = new RuntimeException("failed");
        AtomicInteger loads = new AtomicInteger();
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loads.incrementAndGet();
            held.set(l);
        }, leader);

        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        Executor cancelledWaiter = ExternalIoExecutors.restoring(queue::scheduleNow, null, () -> true);
        PlainActionFuture<String> waiter = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, cancelledWaiter, l -> {
            loads.incrementAndGet();
            l.onResponse("retry-should-not-run");
        }, waiter);

        held.get().onFailure(boom);
        queue.runAllRunnableTasks();
        assertEquals("cancelled waiter must not retry", 1, loads.get());
        assertSame(boom, expectThrows(RuntimeException.class, () -> waiter.actionGet(0, TimeUnit.SECONDS)));
        assertEquals(0, cache.asyncInFlightCount());
    }

    public void testAsyncSyncLoaderThrowFailsFlight() {
        FooterByteCache.Key k = key("throw.parquet", 1000);
        RuntimeException boom = new RuntimeException("sync throw");
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> { throw boom; }, leader);
        assertSame(boom, expectThrows(RuntimeException.class, () -> leader.actionGet(0, TimeUnit.SECONDS)));
        assertNull(cache.get(k));
        assertEquals(0, cache.asyncInFlightCount());
    }

    public void testAsyncLoaderErrorLeavesMapEmpty() {
        FooterByteCache.Key k = key("oom.parquet", 1000);
        OutOfMemoryError boom = new OutOfMemoryError("simulated");
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        PlainActionFuture<String> waiter = new PlainActionFuture<>();
        OutOfMemoryError thrown = expectThrows(
            OutOfMemoryError.class,
            () -> cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
                cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, ignored -> fail("waiter must not load"), waiter);
                throw boom;
            }, leader)
        );
        assertSame(boom, thrown);
        assertEquals(0, cache.asyncInFlightCount());
        assertNull(cache.get(k));
        Exception waiterFailure = expectThrows(Exception.class, () -> waiter.actionGet(0, TimeUnit.SECONDS));
        assertThat(waiterFailure.getMessage(), org.hamcrest.Matchers.containsString("parsed footer load failed"));
    }

    /** A weigher/{@code put} throw after a successful load must drain the flight and fail waiters. */
    public void testAsyncPutThrowDrainsFlightAndFailsWaiters() {
        ParsedFooterCache<String> throwing = new ParsedFooterCache<>(8 * ENTRY_WEIGHT, TTL, v -> {
            if ("boom".equals(v)) {
                throw new IllegalStateException("weigher");
            }
            return ENTRY_WEIGHT;
        });
        FooterByteCache.Key k = key("weigher.parquet", 1000);
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        throwing.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> held.set(l), leader);

        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        PlainActionFuture<String> waiter = new PlainActionFuture<>();
        throwing.getOrLoadAsync(k, queue::scheduleNow, l -> l.onResponse("boom"), waiter);

        held.get().onResponse("boom");
        assertThat(
            expectThrows(RuntimeException.class, () -> leader.actionGet(0, TimeUnit.SECONDS)).getMessage(),
            org.hamcrest.Matchers.containsString("weigher")
        );
        queue.runAllRunnableTasks();
        assertThat(
            expectThrows(RuntimeException.class, () -> waiter.actionGet(0, TimeUnit.SECONDS)).getMessage(),
            org.hamcrest.Matchers.containsString("weigher")
        );
        assertNull(throwing.get(k));
        assertEquals(0, throwing.asyncInFlightCount());
    }

    /** Leader CBE belongs to that query; waiters retry once as a coalesced extra GET. */
    public void testAsyncCircuitBreakingExceptionWaitersRetryOnce() {
        FooterByteCache.Key k = key("cbe.parquet", 1000);
        CircuitBreakingException cbe = new CircuitBreakingException("tripped", CircuitBreaker.Durability.TRANSIENT);
        AtomicInteger loads = new AtomicInteger();
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            loads.incrementAndGet();
            held.set(l);
        }, leader);

        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        PlainActionFuture<String> waiter = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, queue::scheduleNow, l -> {
            loads.incrementAndGet();
            l.onResponse("retry-ok");
        }, waiter);

        held.get().onFailure(cbe);
        queue.runAllRunnableTasks();
        assertEquals("waiters retry a leader CBE as one extra load", 2, loads.get());
        assertSame(cbe, expectThrows(CircuitBreakingException.class, () -> leader.actionGet(0, TimeUnit.SECONDS)));
        assertEquals("retry-ok", waiter.actionGet(0, TimeUnit.SECONDS));
        assertEquals(0, cache.asyncInFlightCount());
        assertEquals("retry-ok", cache.get(k));
    }

    /**
     * Waiters are notified before the leader convert, so they do not sit idle while the leader
     * runs {@code buildFooterMetadata}.
     */
    public void testAsyncWaitersNotifiedBeforeLeaderConvert() {
        FooterByteCache.Key k = key("order.parquet", 1000);
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        AtomicBoolean waiterDone = new AtomicBoolean();
        AtomicBoolean leaderSawWaiterDone = new AtomicBoolean();
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> held.set(l), ActionListener.wrap(v -> {
            leaderSawWaiterDone.set(waiterDone.get());
            leader.onResponse(v);
        }, leader::onFailure));

        PlainActionFuture<String> waiter = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> fail("waiter must not load"), ActionListener.wrap(v -> {
            waiterDone.set(true);
            waiter.onResponse(v);
        }, waiter::onFailure));

        held.get().onResponse("footer");
        assertTrue("DIRECT waiter must finish before the leader listener runs", leaderSawWaiterDone.get());
        assertEquals("footer", leader.actionGet(0, TimeUnit.SECONDS));
        assertEquals("footer", waiter.actionGet(0, TimeUnit.SECONDS));
        assertEquals(0, cache.asyncInFlightCount());
    }

    /**
     * Waiters fan out even if the leader convert throws. {@link ActionListener#assertOnce} forbids
     * a throwing {@code onResponse}, so the leader instead parks until the waiter has finished.
     */
    public void testAsyncLeaderConvertThrowStillFansOutWaiters() throws Exception {
        FooterByteCache.Key k = key("leader-throw.parquet", 1000);
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        CountDownLatch waiterDone = new CountDownLatch(1);
        CountDownLatch leaderMayFinish = new CountDownLatch(1);
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> held.set(l), ActionListener.wrap(v -> {
            assertTrue(waiterDone.await(10, TimeUnit.SECONDS));
            leaderMayFinish.countDown();
            leader.onResponse(v);
        }, leader::onFailure));

        PlainActionFuture<String> waiter = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> fail("waiter must not load"), ActionListener.wrap(v -> {
            waiterDone.countDown();
            waiter.onResponse(v);
        }, waiter::onFailure));

        held.get().onResponse("footer");
        assertTrue(leaderMayFinish.await(10, TimeUnit.SECONDS));
        assertEquals("footer", waiter.actionGet(0, TimeUnit.SECONDS));
        assertEquals("footer", leader.actionGet(0, TimeUnit.SECONDS));
        assertEquals(0, cache.asyncInFlightCount());
    }

    public void testAsyncRejectingWaiterExecutorFailsOnlyThatWaiter() {
        FooterByteCache.Key k = key("reject.parquet", 1000);
        String expected = "ok";
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        PlainActionFuture<String> leader = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> held.set(l), leader);

        Executor rejecting = ExternalIoExecutors.preserving(
            command -> { throw new EsRejectedExecutionException("rejected"); },
            Runnable::run
        );
        PlainActionFuture<String> rejected = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, rejecting, l -> { fail("rejecting waiter must not load"); }, rejected);

        DeterministicTaskQueue okQueue = new DeterministicTaskQueue();
        PlainActionFuture<String> ok = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, okQueue::scheduleNow, l -> { fail("ok waiter must not load"); }, ok);

        held.get().onResponse(expected);
        assertSame(expected, leader.actionGet(0, TimeUnit.SECONDS));
        expectThrows(EsRejectedExecutionException.class, () -> rejected.actionGet(0, TimeUnit.SECONDS));
        assertFalse(ok.isDone());
        okQueue.runAllRunnableTasks();
        assertSame(expected, ok.actionGet(0, TimeUnit.SECONDS));
        assertEquals(0, cache.asyncInFlightCount());
    }

    public void testAsyncCoalesceDisabledOneLoadPerCaller() {
        ParsedFooterCache<String> disabled = new ParsedFooterCache<>(8 * ENTRY_WEIGHT, TTL, ignored -> ENTRY_WEIGHT, false);
        FooterByteCache.Key k = key("nocoalesce.parquet", 1000);
        AtomicInteger loads = new AtomicInteger();
        List<ActionListener<String>> held = new ArrayList<>();
        List<PlainActionFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            PlainActionFuture<String> future = new PlainActionFuture<>();
            futures.add(future);
            disabled.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
                loads.incrementAndGet();
                held.add(l);
            }, future);
        }
        assertEquals(4, loads.get());
        assertEquals(0, disabled.asyncInFlightCount());
        for (int i = 0; i < held.size(); i++) {
            held.get(i).onResponse("v" + i);
        }
        assertEquals("v0", futures.get(0).actionGet(0, TimeUnit.SECONDS));
        assertEquals("v3", futures.get(3).actionGet(0, TimeUnit.SECONDS));
    }

    public void testAsyncSyncGetOrLoadRaceLeavesOneCachedValue() throws Exception {
        FooterByteCache.Key k = key("race.parquet", 1000);
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        CountDownLatch asyncStarted = new CountDownLatch(1);
        PlainActionFuture<String> async = new PlainActionFuture<>();
        cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
            held.set(l);
            asyncStarted.countDown();
        }, async);
        safeAwait(asyncStarted);

        String syncValue = "sync";
        String fromSync = cache.getOrLoad(k, ignore -> syncValue);
        held.get().onResponse("async");

        assertEquals("async last writer wins after sync put", "async", cache.get(k));
        assertEquals(syncValue, fromSync);
        assertEquals("async", async.actionGet(0, TimeUnit.SECONDS));
        assertEquals(0, cache.asyncInFlightCount());
    }

    public void testAsyncRandomizedLoadsAtMostCallersAndMapDrains() {
        FooterByteCache.Key k = key("rand.parquet", 1000);
        String value = "v";
        int callers = randomIntBetween(1, 30);
        boolean overlap = randomBoolean();
        AtomicInteger loads = new AtomicInteger();
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        List<PlainActionFuture<String>> futures = new ArrayList<>(callers);
        for (int i = 0; i < callers; i++) {
            PlainActionFuture<String> future = new PlainActionFuture<>();
            futures.add(future);
            cache.getOrLoadAsync(k, EsExecutors.DIRECT_EXECUTOR_SERVICE, l -> {
                loads.incrementAndGet();
                if (overlap && held.compareAndSet(null, l)) {
                    return;
                }
                l.onResponse(value);
            }, future);
        }
        ActionListener<String> pending = held.get();
        if (pending != null) {
            pending.onResponse(value);
        }
        for (PlainActionFuture<String> future : futures) {
            assertSame(value, future.actionGet(0, TimeUnit.SECONDS));
        }
        assertEquals(1, loads.get());
        assertEquals(0, cache.asyncInFlightCount());
    }

    private static FooterByteCache.Key key(String path, long length) {
        return new FooterByteCache.Key(AbstractTestStorageObject.NOOP, path, length);
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
