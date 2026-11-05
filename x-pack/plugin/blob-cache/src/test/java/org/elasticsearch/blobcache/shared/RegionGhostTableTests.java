/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.blobcache.BlobCacheMetrics.RegionFillOrigin;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RegionGhostTableTests extends ESTestCase {

    public void testAddAndRemove() {
        final var table = new RegionGhostTable(4);
        table.add(1L, 100L, RegionFillOrigin.Demand, RegionFillOrigin.Speculative);
        table.add(2L, 200L, RegionFillOrigin.ForcedSpeculative, RegionFillOrigin.Demand);
        assertThat(table.size(), is(2));

        final var ghost = table.remove(1L);
        assertThat(ghost, notNullValue());
        assertThat(ghost.evictedAtMillis(), is(100L));
        assertThat(ghost.evictionsSince(), is(1L));
        assertThat(ghost.evictedBy(), is(RegionFillOrigin.Demand));
        assertThat(ghost.victimOrigin(), is(RegionFillOrigin.Speculative));
        assertThat(table.size(), is(1));
        // a ghost is consumed by the lookup that finds it
        assertThat(table.remove(1L), nullValue());
        assertThat(table.remove(3L), nullValue());
    }

    public void testOldestEvictionIsForgottenWhenFull() {
        final var table = new RegionGhostTable(3);
        for (long key = 0; key < 5; key++) {
            table.add(key, key, RegionFillOrigin.Demand, RegionFillOrigin.Demand);
        }
        assertThat(table.size(), is(3));
        assertThat(table.remove(0L), nullValue());
        assertThat(table.remove(1L), nullValue());
        assertThat(table.remove(2L).evictionsSince(), is(2L));
        assertThat(table.remove(4L).evictionsSince(), is(0L));
    }

    public void testReAddingAKeyKeepsTheLatestEviction() {
        final var table = new RegionGhostTable(8);
        table.add(7L, 1L, RegionFillOrigin.Demand, RegionFillOrigin.Demand);
        table.add(8L, 2L, RegionFillOrigin.Demand, RegionFillOrigin.Demand);
        table.add(7L, 3L, RegionFillOrigin.Speculative, RegionFillOrigin.Demand);
        assertThat(table.size(), is(2));
        final var ghost = table.remove(7L);
        assertThat(ghost.evictedAtMillis(), is(3L));
        assertThat(ghost.evictedBy(), is(RegionFillOrigin.Speculative));
        assertThat(ghost.evictionsSince(), is(0L));
    }

    public void testFingerprintDistinguishesRegionsOfTheSameFile() {
        final int fileKeyHash = randomInt();
        assertThat(RegionGhostTable.fingerprint(fileKeyHash, 0), equalTo(RegionGhostTable.fingerprint(fileKeyHash, 0)));
        assertNotEquals(RegionGhostTable.fingerprint(fileKeyHash, 0), RegionGhostTable.fingerprint(fileKeyHash, 1));
        assertNotEquals(RegionGhostTable.fingerprint(fileKeyHash, 0), RegionGhostTable.fingerprint(fileKeyHash + 1, 0));
    }

    public void testMaxCapacityForHeapBoundsWorstCaseFootprint() {
        assertThat(RegionGhostTable.maxCapacityForHeap(0L), equalTo(0L));
        // 1% of 3.5kb is 35 bytes, i.e. exactly one entry in the worst case
        assertThat(RegionGhostTable.maxCapacityForHeap(3_500L), equalTo(1L));
        final long heapBytes = randomLongBetween(1L << 20, 1L << 40);
        final long capacity = RegionGhostTable.maxCapacityForHeap(heapBytes);
        assertThat(capacity <= RegionGhostTable.MAX_CAPACITY, is(true));
        assertThat(capacity * RegionGhostTable.MAX_BYTES_PER_ENTRY <= heapBytes * RegionGhostTable.MAX_HEAP_FRACTION, is(true));
        assertThat(RegionGhostTable.maxCapacityForHeap(Long.MAX_VALUE), equalTo((long) RegionGhostTable.MAX_CAPACITY));
    }

    public void testRejectsInvalidCapacity() {
        expectThrows(IllegalArgumentException.class, () -> new RegionGhostTable(0));
        expectThrows(IllegalArgumentException.class, () -> new RegionGhostTable(RegionGhostTable.MAX_CAPACITY + 1));
    }

    /**
     * Applies random adds and removes, over a small key space so that keys collide in the index and are re-added often,
     * and checks every result against a straightforward model: an entry lives until it is removed, re-added, or until
     * {@code capacity} more adds have happened.
     */
    public void testMatchesModel() {
        final int capacity = randomIntBetween(1, 64);
        final int keySpace = randomIntBetween(1, capacity * 4);
        final var table = new RegionGhostTable(capacity);
        final Map<Long, Long> modelSeqByKey = new HashMap<>();
        final Map<Long, Long> modelTimeByKey = new HashMap<>();
        long insertions = 0;
        for (int i = 0; i < 10_000; i++) {
            final long key = randomLongBetween(0, keySpace - 1) * 0x9E3779B97F4A7C15L;
            if (randomBoolean()) {
                final long now = randomNonNegativeLong();
                table.add(key, now, RegionFillOrigin.Demand, RegionFillOrigin.Demand);
                modelSeqByKey.remove(key);
                final long expired = insertions - capacity;
                modelSeqByKey.values().removeIf(seq -> seq == expired);
                modelSeqByKey.put(key, insertions);
                modelTimeByKey.put(key, now);
                insertions++;
            } else {
                final var ghost = table.remove(key);
                final Long seq = modelSeqByKey.remove(key);
                if (seq == null) {
                    assertThat(ghost, nullValue());
                } else {
                    assertThat(ghost, notNullValue());
                    assertThat(ghost.evictionsSince(), is(insertions - 1 - seq));
                    assertThat(ghost.evictedAtMillis(), is(modelTimeByKey.get(key)));
                }
            }
            assertThat(table.size(), is(modelSeqByKey.size()));
        }
    }
}
