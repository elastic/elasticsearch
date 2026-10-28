/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class LongLongDirectHashTests extends PartitionedHashTestCase {
    private final CircuitBreaker breaker = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(100))
        .withCircuitBreaking()
        .breakerService()
        .getBreaker(CircuitBreaker.REQUEST);

    public void testHitMissAndGet() {
        try (var hash = new LongLongDirectHash(breaker, 1024)) {
            assertThat(hash.add(10, 100), equalTo(0L));
            assertThat(hash.add(20, 200), equalTo(1L));
            assertThat(hash.add(10, 100), equalTo(-1L));
            assertThat(hash.add(20, 200), equalTo(-2L));
            assertThat(hash.size(), equalTo(2L));
            assertThat(hash.getKey1(0), equalTo(10L));
            assertThat(hash.getKey2(0), equalTo(100L));
            assertThat(hash.getKey1(1), equalTo(20L));
            assertThat(hash.find(10, 100), equalTo(0L));
            assertThat(hash.find(30, 300), equalTo(-1L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * With a single slot every key evicts the previous one. The sequence A, B, A must give three distinct ids, and
     * all three ids must stay readable.
     */
    public void testEvictionKeepsIdsAndKeys() {
        try (var hash = new LongLongDirectHash(breaker, 1)) {
            assertThat(hash.add(1, 10), equalTo(0L));
            assertThat(hash.add(2, 20), equalTo(1L));
            assertThat(hash.add(1, 10), equalTo(2L));
            assertThat(hash.add(1, 10), equalTo(-3L));
            assertThat(hash.size(), equalTo(3L));
            assertThat(hash.getKey1(0), equalTo(1L));
            assertThat(hash.getKey1(1), equalTo(2L));
            assertThat(hash.getKey1(2), equalTo(1L));
            assertThat(hash.find(2, 20), equalTo(-1L));
            assertThat(hash.find(1, 10), equalTo(2L));
        }
    }

    public void testGrowsKeysAndClear() {
        try (var hash = new LongLongDirectHash(breaker, 8)) {
            final int count = between(5_000, 20_000);
            for (int i = 0; i < count; i++) {
                assertThat(hash.add(i, -i), equalTo((long) i));
            }
            assertThat(hash.size(), equalTo((long) count));
            for (int i = 0; i < count; i++) {
                assertThat(hash.getKey1(i), equalTo((long) i));
                assertThat(hash.getKey2(i), equalTo((long) -i));
            }
            hash.clear();
            assertThat(hash.size(), equalTo(0L));
            assertThat(hash.find(0, 0), equalTo(-1L));
            assertThat(hash.add(7, 70), equalTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSlotsMustBePowerOfTwo() {
        expectThrows(IllegalArgumentException.class, () -> new LongLongDirectHash(breaker, 1000));
        expectThrows(IllegalArgumentException.class, () -> new LongLongDirectHash(breaker, 0));
    }

    public void testHighHitRateWhenKeysFitInSlots() {
        try (var hash = new LongLongDirectHash(breaker, 1 << 16)) {
            final int distinct = 64;
            for (int i = 0; i < 10_000; i++) {
                hash.add(randomIntBetween(0, distinct - 1) * 1_000_003L, 7);
            }
            // a few of the 64 keys may share a slot, so allow some evictions but far fewer ids than rows
            assertThat(hash.size(), lessThan(1_000L));
            assertThat(hash.size(), greaterThan(0L));
        }
    }

    public void testCombineIsUnsupported() {
        try (var hash = new LongLongDirectHash(breaker, 16)) {
            expectThrows(UnsupportedOperationException.class, () -> hash.combinePartition(null, 0, new int[0]));
        }
    }

    /**
     * Splits direct tables, including evicted duplicates, then combines the partitions into a regular
     * {@link LongLongSwissHash}. The sums per key must match a plain map.
     */
    public void testPartitionAndCombineWithDuplicates() {
        var recycler = new LongLongSwissHashTests.TestRecycler();
        final int slots = 1 << between(0, 10);
        final int flushAt = randomIntBetween(128, 10 * 1024);
        var direct = new LongLongDirectHash(breaker, slots);
        var agg = new SumAgg(breaker);
        var combined = new LongLongSwissHash(recycler, breaker);
        var combinedAgg = new SumAgg(breaker);
        List<PartitionedKeyAndAggs> gens = new ArrayList<>();
        Map<List<Long>, Long> expected = new HashMap<>();
        try {
            for (int block = between(1, 300); block > 0; block--) {
                final boolean fewKeys = randomBoolean();
                for (int v = between(1, 2048); v > 0; v--) {
                    List<Long> key = List.of(
                        fewKeys ? randomLongBetween(-10, 10) : randomLong(),
                        fewKeys ? randomLongBetween(0, 3) : randomLong()
                    );
                    int value = randomIntBetween(-1000, 1000);
                    long id = direct.add(key.get(0), key.get(1));
                    int groupId = Math.toIntExact(id >= 0 ? id : -1 - id);
                    agg.ensureCapacity(Math.toIntExact(direct.size()));
                    agg.add(groupId, value);
                    expected.merge(key, (long) value, Long::sum);
                }
                if (direct.size() >= flushAt) {
                    gens.add(partition(breaker, direct, Math.toIntExact(direct.size()), agg));
                    direct.clear();
                    agg.clear();
                }
            }
            if (direct.size() > 0) {
                gens.add(partition(breaker, direct, Math.toIntExact(direct.size()), agg));
            }
            Map<List<Long>, Long> actual = new HashMap<>();
            int[] mergedIds = new int[0];
            for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                combined.clear();
                combinedAgg.clear();
                int total = 0;
                for (var gen : gens) {
                    total += gen.keys().keysInPartition(p);
                }
                combinedAgg.ensureCapacity(total);
                for (var gen : gens) {
                    int numKeys = gen.keys().keysInPartition(p);
                    if (mergedIds.length < numKeys) {
                        mergedIds = new int[ArrayUtil.oversize(numKeys, Integer.BYTES)];
                    }
                    boolean appendOnly = combined.combinePartition(gen.keys(), p, mergedIds);
                    gen.keys().releasePartition(breaker, p);
                    combinedAgg.combinePartition(gen.aggs(), p, mergedIds, numKeys, appendOnly);
                    gen.aggs().releasePartition(breaker, p);
                }
                for (int id = 0; id < combined.size; id++) {
                    assertNull(actual.put(List.of(combined.getKey1(id), combined.getKey2(id)), (long) combinedAgg.sums[id]));
                }
            }
            assertThat(actual.size(), equalTo(expected.size()));
            for (var e : expected.entrySet()) {
                assertThat("sum for " + e.getKey(), actual.get(e.getKey()), equalTo((long) (int) (long) e.getValue()));
            }
        } finally {
            Releasables.close(direct, agg, combined, combinedAgg);
            gens.forEach(g -> g.release(breaker));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }
}
