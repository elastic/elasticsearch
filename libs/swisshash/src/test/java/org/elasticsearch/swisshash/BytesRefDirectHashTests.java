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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class BytesRefDirectHashTests extends PartitionedHashTestCase {
    private final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(200))
        .withCircuitBreaking();
    private final CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);

    private static BytesRef bytes(String s) {
        return new BytesRef(s);
    }

    public void testHitMissAndGet() {
        try (var hash = new BytesRefDirectHash(breaker, bigArrays, 1024)) {
            assertThat(hash.add(bytes("a")), equalTo(0L));
            assertThat(hash.add(bytes("b")), equalTo(1L));
            assertThat(hash.add(bytes("a")), equalTo(-1L));
            assertThat(hash.size(), equalTo(2L));
            assertThat(hash.get(1, new BytesRef()), equalTo(bytes("b")));
            assertThat(hash.find(bytes("a")), equalTo(0L));
            assertThat(hash.find(bytes("c")), equalTo(-1L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testEvictionKeepsIdsAndKeys() {
        try (var hash = new BytesRefDirectHash(breaker, bigArrays, 1)) {
            assertThat(hash.add(bytes("a")), equalTo(0L));
            assertThat(hash.add(bytes("b")), equalTo(1L));
            assertThat(hash.add(bytes("a")), equalTo(2L));
            assertThat(hash.size(), equalTo(3L));
            assertThat(hash.get(0, new BytesRef()), equalTo(bytes("a")));
            assertThat(hash.get(2, new BytesRef()), equalTo(bytes("a")));
            assertThat(hash.find(bytes("b")), equalTo(-1L));
        }
    }

    public void testClear() {
        try (var hash = new BytesRefDirectHash(breaker, bigArrays, 16)) {
            for (int i = 0; i < 5000; i++) {
                assertThat(hash.add(bytes("key-" + i)), equalTo((long) i));
            }
            hash.clear();
            assertThat(hash.size(), equalTo(0L));
            assertThat(hash.find(bytes("key-0")), equalTo(-1L));
            assertThat(hash.add(bytes("x")), equalTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testPartitionAndCombineFlatWithDuplicates() {
        runPartitionTest(false);
    }

    public void testPartitionAndCombineFixedLengthWithDuplicates() {
        runPartitionTest(true);
    }

    /**
     * Splits direct tables, including evicted duplicates, then combines the partitions into a regular
     * {@link BytesRefSwissHash}. The sums per key must match a plain map.
     */
    private void runPartitionTest(boolean fixedLength) {
        var recycler = new BytesRefSwissHashTests.TestRecycler();
        final int keyLength = randomIntBetween(1, 16);
        final int slots = 1 << between(0, 10);
        final int flushAt = randomIntBetween(128, 10 * 1024);
        var direct = new BytesRefDirectHash(breaker, bigArrays, slots);
        var agg = new SumAgg(breaker);
        var combined = new BytesRefSwissHash(recycler, breaker, bigArrays);
        var combinedAgg = new SumAgg(breaker);
        List<PartitionedKeyAndAggs> gens = new ArrayList<>();
        Map<BytesRef, Long> expected = new HashMap<>();
        try {
            for (int block = between(1, 300); block > 0; block--) {
                final boolean fewKeys = randomBoolean();
                for (int v = between(1, 2048); v > 0; v--) {
                    final int len = fixedLength ? keyLength : fewKeys ? randomIntBetween(1, 2) : randomIntBetween(1, 20);
                    final byte[] b = new byte[len];
                    if (fewKeys) {
                        random().nextBytes(b);
                        for (int i = 0; i < len; i++) {
                            b[i] = (byte) (b[i] & 3);
                        }
                    } else {
                        random().nextBytes(b);
                    }
                    BytesRef key = new BytesRef(b);
                    int value = randomIntBetween(-1000, 1000);
                    long id = direct.add(key);
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
            Map<BytesRef, Long> actual = new HashMap<>();
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
                    BytesRef key = BytesRef.deepCopyOf(combined.get(id, new BytesRef()));
                    assertNull(actual.put(key, (long) combinedAgg.sums[id]));
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
