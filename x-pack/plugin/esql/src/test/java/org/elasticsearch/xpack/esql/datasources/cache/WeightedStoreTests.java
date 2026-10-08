/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * {@link WeightedStore} owns the three things that used to be copied per cache: the slice budget, the
 * per-entry admission ceiling, and the five {@code usageStats} lines. None of that was asserted directly
 * before this suite - it was exercised only through {@code ExternalSourceCacheServiceTests}, where a
 * ceiling change shows up as an eviction rather than as a figure, and where nothing reaches the
 * invalidate-on-oversize branch at all.
 */
public class WeightedStoreTests extends ESTestCase {

    /** A value whose weight is whatever the test says it is, so the ceiling can be driven exactly. */
    private record Sized(long bytes) {}

    private static WeightedStore<String, Sized> storeOf(long budgetBytes) {
        return WeightedStore.of("test_cache", budgetBytes, Sized::bytes, null);
    }

    public void testAnEmptyOrNegativeSliceAdmitsNothing() {
        assertEquals(0L, WeightedStore.perEntryCeiling(0L));
        assertEquals(0L, WeightedStore.perEntryCeiling(-1L));
        assertEquals(0L, WeightedStore.perEntryCeiling(Long.MIN_VALUE));
    }

    /**
     * Below the soft floor the ceiling is the whole slice, not a quarter of it: the regression suites run on
     * tens of kilobytes to force eviction, and a strict quarter there would refuse the entries they need
     * resident. The cap by the slice is what stops the floor admitting an entry the store cannot hold.
     */
    public void testASliceSmallerThanTheFloorAdmitsItsWholeSelfAndNoMore() {
        assertEquals(1L, WeightedStore.perEntryCeiling(1L));
        assertEquals(1000L, WeightedStore.perEntryCeiling(1000L));
        assertEquals(
            WeightedStore.PER_ENTRY_CEILING_FLOOR_BYTES,
            WeightedStore.perEntryCeiling(WeightedStore.PER_ENTRY_CEILING_FLOOR_BYTES)
        );
    }

    /**
     * The crossover, pinned on both sides rather than near it. A quarter of four times the floor IS the
     * floor, so one byte more is the first slice where the quarter governs - which is the only place an
     * off-by-one in either {@code Math.max} would show.
     */
    public void testTheQuarterGovernsOnceItExceedsTheFloor() {
        long floor = WeightedStore.PER_ENTRY_CEILING_FLOOR_BYTES;
        assertEquals("at exactly four floors the quarter equals the floor", floor, WeightedStore.perEntryCeiling(4 * floor));
        assertEquals("one byte past it the quarter wins", floor + 1, WeightedStore.perEntryCeiling(4 * floor + 4));
        assertEquals("and keeps winning", 25 * floor, WeightedStore.perEntryCeiling(100 * floor));
        assertThat("the quarter is never above the slice", WeightedStore.perEntryCeiling(4 * floor + 4), greaterThan(floor));
    }

    public void testTheCeilingNeverExceedsTheSlice() {
        for (long slice : new long[] { 1L, 7L, 1024L, 16 * 1024L, 64 * 1024L, 1024 * 1024L, Long.MAX_VALUE / 4 }) {
            long ceiling = WeightedStore.perEntryCeiling(slice);
            assertThat("ceiling " + ceiling + " must not exceed slice " + slice, slice, greaterThanOrEqualTo(ceiling));
            assertThat("a positive slice admits something", ceiling, greaterThan(0L));
        }
    }

    public void testTheStoreReportsTheBudgetAndCeilingItWasBuiltWith() {
        long budget = 512 * 1024L;
        WeightedStore<String, Sized> store = storeOf(budget);
        assertEquals(budget, store.budgetBytes());
        assertEquals(WeightedStore.perEntryCeiling(budget), store.maxEntryBytes());
    }

    public void testAValueAtTheCeilingIsRetainedAndOneByteOverIsNot() {
        WeightedStore<String, Sized> store = storeOf(1024 * 1024L);
        long ceiling = store.maxEntryBytes();

        store.putIfWithinCeiling("at", new Sized(ceiling));
        assertEquals(new Sized(ceiling), store.get("at"));

        store.putIfWithinCeiling("over", new Sized(ceiling + 1));
        assertThat("an oversized value is not retained", store.get("over"), nullValue());
    }

    /**
     * The branch no service-level test reaches. A value already resident must be dropped when a later
     * oversized value arrives at the same address, so a tightened budget cannot leave behind an entry the
     * ceiling now forbids. Refusing to retain is not refusing to answer - the caller keeps its value either
     * way - which is why this cannot be asserted by reading what the caller got back.
     */
    public void testAnOversizedPutEvictsWhateverWasAlreadyAtThatAddress() {
        WeightedStore<String, Sized> store = storeOf(1024 * 1024L);
        long ceiling = store.maxEntryBytes();

        store.putIfWithinCeiling("k", new Sized(ceiling / 2));
        assertEquals("precondition: the small value is resident", new Sized(ceiling / 2), store.get("k"));

        store.putIfWithinCeiling("k", new Sized(ceiling + 1));
        assertThat("the resident value is gone, not merely un-replaced", store.get("k"), nullValue());
    }

    public void testTheCeilingCheckConsultsTheWeigher() {
        AtomicInteger calls = new AtomicInteger();
        WeightedStore<String, Sized> store = WeightedStore.of("counted", 1024 * 1024L, v -> {
            calls.incrementAndGet();
            return v.bytes();
        }, null);

        store.putIfWithinCeiling("k", new Sized(8));
        assertThat("the ceiling cannot be enforced without weighing the value", calls.get(), greaterThanOrEqualTo(1));
    }

    /**
     * The five lines this store contributes to {@code usageStats}, under its own prefix. The names are a
     * node-level API that dashboards and the warm-path suites read, so they are pinned by name rather than
     * by count.
     */
    public void testReportIntoEmitsItsFiveCountersUnderItsOwnPrefix() {
        WeightedStore<String, Sized> store = storeOf(1024 * 1024L);
        store.putIfWithinCeiling("a", new Sized(16));
        store.get("a");
        store.get("missing");

        Map<String, Object> stats = new HashMap<>();
        store.reportInto(stats);

        assertThat(
            stats.keySet(),
            containsInAnyOrder(
                "test_cache.count",
                "test_cache.weight_bytes",
                "test_cache.hits",
                "test_cache.misses",
                "test_cache.evictions"
            )
        );
        assertEquals(1, (int) (Integer) stats.get("test_cache.count"));
        assertEquals(16L, (long) (Long) stats.get("test_cache.weight_bytes"));
        assertEquals(1L, (long) (Long) stats.get("test_cache.hits"));
        assertEquals(1L, (long) (Long) stats.get("test_cache.misses"));
        assertEquals(0L, (long) (Long) stats.get("test_cache.evictions"));
    }

    public void testInvalidateAndInvalidateAllClearTheStore() {
        WeightedStore<String, Sized> store = storeOf(1024 * 1024L);
        store.putIfWithinCeiling("a", new Sized(8));
        store.putIfWithinCeiling("b", new Sized(8));

        store.invalidate("a");
        assertThat(store.get("a"), nullValue());
        assertEquals(new Sized(8), store.get("b"));

        store.invalidateAll();
        assertThat(store.get("b"), nullValue());
    }

    public void testForEachVisitsEveryResidentEntry() {
        WeightedStore<String, Sized> store = storeOf(1024 * 1024L);
        store.putIfWithinCeiling("a", new Sized(8));
        store.putIfWithinCeiling("b", new Sized(16));

        Map<String, Long> seen = new HashMap<>();
        store.forEach((k, v) -> seen.put(k, v.bytes()));
        assertEquals(Map.of("a", 8L, "b", 16L), seen);
    }
}
