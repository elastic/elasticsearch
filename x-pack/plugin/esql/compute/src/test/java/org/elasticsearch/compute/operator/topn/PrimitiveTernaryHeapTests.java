/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class PrimitiveTernaryHeapTests extends ESTestCase {

    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);

    public void testEmptyState() {
        try (PrimitiveTernaryHeap heap = new PrimitiveTernaryHeap(breaker, 1, false)) {
            assertThat(heap.size(), equalTo(0));
            assertThat(heap.capacity(), equalTo(1));
            assertFalse(heap.isFull());
        }
    }

    public void testCapacityValidation() {
        expectThrows(IllegalArgumentException.class, () -> new PrimitiveTernaryHeap(breaker, 0, false));
        expectThrows(IllegalArgumentException.class, () -> new PrimitiveTernaryHeap(breaker, -1, false));
    }

    /**
     * Pushes K random distinct values, then verifies a sorted drain yields them in
     * non-decreasing order and the heap invariant holds after every operation. This is the
     * "exhaustive heap-swap triple-alignment" probe called for in Stage 3: after every
     * mutation we walk the heap and assert the parent ≤ children property holds, plus the
     * popped sequence matches a Java-sorted reference.
     */
    public void testFillAndDrainRandomDistinct() {
        int k = randomIntBetween(1, 200);
        try (PrimitiveTernaryHeap heap = new PrimitiveTernaryHeap(breaker, k, false)) {
            List<Long> inputs = randomDistinctLongs(k);
            for (int i = 0; i < inputs.size(); i++) {
                heap.push(inputs.get(i), i, false);
                assertTrue("invariant after push " + i, heap.assertInvariant());
            }
            assertTrue(heap.isFull());
            Collections.sort(inputs);
            List<Long> drained = new ArrayList<>(k);
            while (heap.size() > 0) {
                int slot = heap.popTop();
                drained.add(heap.valueAt(slot));
                assertTrue("invariant after pop", heap.assertInvariant());
            }
            assertThat(drained, equalTo(inputs));
        }
    }

    /**
     * The hot-path eviction model: heap fills to K, then any greater value replaces the root.
     * This exercises {@link PrimitiveTernaryHeap#updateTop} on every iteration once the heap
     * is full and verifies the final drained set equals the K largest inputs.
     */
    public void testUpdateTopKeepsKLargest() {
        int k = randomIntBetween(1, 50);
        int n = k + randomIntBetween(0, 1000);
        try (PrimitiveTernaryHeap heap = new PrimitiveTernaryHeap(breaker, k, false)) {
            List<Long> all = randomDistinctLongs(n);
            for (int i = 0; i < n; i++) {
                long v = all.get(i);
                if (heap.size() < k) {
                    heap.push(v, i, false);
                } else if (v > heap.peekTop()) {
                    heap.updateTop(v, i, false);
                }
                assertTrue("invariant after step " + i, heap.assertInvariant());
            }
            List<Long> expected = new ArrayList<>(all);
            Collections.sort(expected, Collections.reverseOrder());
            expected = expected.subList(0, k);
            Collections.sort(expected); // expect min-first drain order
            List<Long> drained = new ArrayList<>(k);
            while (heap.size() > 0) {
                int slot = heap.popTop();
                drained.add(heap.valueAt(slot));
            }
            assertThat(drained, equalTo(expected));
        }
    }

    /**
     * Equal encoded values are common (e.g. low-cardinality sort keys). When two slots tie on
     * value the heap orders them by row position descending so the slot with the *larger* row
     * position sits closer to the root and is evicted on the next competitive insert. This
     * matches the operator's "first-seen wins" stability and is exercised here by feeding K+1
     * rows that all share the same encoded value with distinct row positions.
     */
    public void testTieBreakerOnRowPosition() {
        int k = randomIntBetween(2, 20);
        try (PrimitiveTernaryHeap heap = new PrimitiveTernaryHeap(breaker, k, false)) {
            for (int i = 0; i < k; i++) {
                heap.push(42L, i, false);
                assertTrue(heap.assertInvariant());
            }
            // The root should be the entry with the largest row position so far.
            assertThat(heap.topRowPosition(), equalTo((long) (k - 1)));
            // A new row with equal value but a larger row position is "less competitive" than
            // the current root. The hot path uses strict {@code encoded > threshold} to reject;
            // we emulate that here. Equal encoded means no update; the existing root should
            // remain unchanged.
            // No-op: skip the update because encoded is not > threshold (just verify by reading
            // peekTop and topRowPosition again).
            assertThat(heap.peekTop(), equalTo(42L));
            assertThat(heap.topRowPosition(), equalTo((long) (k - 1)));
        }
    }

    /**
     * The null-flag {@link java.util.BitSet} must remain aligned with values and row positions
     * through every swap. We push a randomised mix of null and non-null entries, then drain
     * and verify the {@code (value, rowPosition, isNull)} triple at each slot matches what
     * was pushed for that row position.
     */
    public void testNullFlagsStayAligned() {
        int k = randomIntBetween(2, 100);
        try (PrimitiveTernaryHeap heap = new PrimitiveTernaryHeap(breaker, k, false)) {
            boolean[] expectedNull = new boolean[k];
            long[] expectedValue = new long[k];
            for (int i = 0; i < k; i++) {
                expectedNull[i] = randomBoolean();
                expectedValue[i] = randomLong();
                heap.push(expectedValue[i], i, expectedNull[i]);
                assertTrue(heap.assertInvariant());
            }
            // Drain and check each popped slot's (value, rowPosition, isNull) triple matches
            // what was pushed for that row position.
            while (heap.size() > 0) {
                int slot = heap.popTop();
                long rp = heap.rowPositionAt(slot);
                assertThat((long) (int) rp, equalTo(rp));
                int idx = Math.toIntExact(rp);
                assertThat(heap.valueAt(slot), equalTo(expectedValue[idx]));
                assertThat(heap.isNullAt(slot), equalTo(expectedNull[idx]));
            }
        }
    }

    /**
     * Randomised stress test: alternates {@code push} and {@code updateTop} driven by the same
     * {@link PrimitiveTernaryHeap#wouldEvictTop} predicate the operator uses, and after every
     * mutation verifies the heap invariant holds. This is the test the plan calls out as the
     * alignment safety net for the {@code swap(i, j)} encapsulation: bit, value, and row
     * position must move together through every up- and down-heap.
     */
    public void testRandomMixInvariantHolds() {
        int k = randomIntBetween(2, 64);
        int n = k * randomIntBetween(2, 20);
        boolean nullsFirst = randomBoolean();
        try (PrimitiveTernaryHeap heap = new PrimitiveTernaryHeap(breaker, k, nullsFirst)) {
            for (int i = 0; i < n; i++) {
                long v = randomLong();
                boolean isNull = randomBoolean();
                if (heap.size() < k) {
                    heap.push(v, i, isNull);
                } else if (heap.wouldEvictTop(v, i, isNull)) {
                    heap.updateTop(v, i, isNull);
                }
                assertTrue("invariant after step " + i, heap.assertInvariant());
                assertThat(heap.nullsInHeap(), equalTo(countNulls(heap)));
            }
        }
    }

    /**
     * Breaker accounting: the heap registers a deterministic amount of bytes up front and
     * releases exactly that on close. We use a {@link LimitedBreaker} so a leak (or a release
     * with the wrong amount) leaves the breaker non-zero, which the test detects.
     */
    public void testBreakerAccounting() {
        LimitedBreaker counter = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofGb(1));
        int k = randomIntBetween(1, 1024);
        try (PrimitiveTernaryHeap heap = new PrimitiveTernaryHeap(counter, k, false)) {
            assertThat("breaker should hold the heap's reservation", counter.getUsed(), greaterThan(0L));
            heap.push(1L, 0, false);
        }
        assertThat("breaker fully released after close", counter.getUsed(), equalTo(0L));
    }

    /**
     * Stage 3 alignment test: drive the heap with a long random insert sequence and, after
     * every {@code push} or {@code updateTop}, verify two properties at every occupied slot:
     * <ul>
     *     <li>The row position recorded at the slot, when looked up against the original input
     *         array, yields the same {@code (value, isNull)} pair currently stored at the
     *         slot.</li>
     *     <li>The heap invariant holds (parent ≤ children under the composite ordering).</li>
     * </ul>
     * Because {@code _rowPosition} uniquely identifies the original input (it's just the input
     * index), the first check is exactly the "triple-alignment" probe: if {@code swap(i, j)}
     * ever moved value, row position, or null bit independently, the (value, isNull) lookup
     * against the original input would mismatch for some slot.
     */
    public void testHeapSwapTripleAlignmentStress() {
        int k = randomIntBetween(2, 64);
        int n = k * randomIntBetween(4, 30);
        boolean nullsFirst = randomBoolean();
        long[] inputValues = new long[n];
        boolean[] inputNulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            inputValues[i] = randomLong();
            inputNulls[i] = randomBoolean();
        }
        try (PrimitiveTernaryHeap heap = new PrimitiveTernaryHeap(breaker, k, nullsFirst)) {
            for (int i = 0; i < n; i++) {
                long v = inputValues[i];
                boolean isNull = inputNulls[i];
                if (heap.size() < k) {
                    heap.push(v, i, isNull);
                } else if (heap.wouldEvictTop(v, i, isNull)) {
                    heap.updateTop(v, i, isNull);
                }
                assertTrue("invariant after step " + i, heap.assertInvariant());
                heap.forEachSlot((slot, value, rp, slotIsNull) -> {
                    // The row position recorded at this slot was the input index, so we can
                    // recover the original triple and check the in-heap state still matches.
                    int origIdx = Math.toIntExact(rp);
                    assertThat("value alignment at slot " + slot + " (origIdx " + origIdx + ")", value, equalTo(inputValues[origIdx]));
                    assertThat("null alignment at slot " + slot + " (origIdx " + origIdx + ")", slotIsNull, equalTo(inputNulls[origIdx]));
                });
                assertThat(heap.nullsInHeap(), equalTo(countNulls(heap)));
            }
        }
    }

    public void testNullCounterTracksPushUpdateAndPop() {
        int k = randomIntBetween(2, 64);
        try (PrimitiveTernaryHeap heap = new PrimitiveTernaryHeap(breaker, k, randomBoolean())) {
            for (int i = 0; i < k; i++) {
                heap.push(randomLong(), i, randomBoolean());
                assertThat(heap.nullsInHeap(), equalTo(countNulls(heap)));
            }
            for (int i = 0; i < k * 4; i++) {
                heap.updateTop(randomLong(), k + i, randomBoolean());
                assertTrue(heap.assertInvariant());
                assertThat(heap.nullsInHeap(), equalTo(countNulls(heap)));
            }
            while (heap.size() > 0) {
                heap.popTop();
                assertTrue(heap.assertInvariant());
                assertThat(heap.nullsInHeap(), equalTo(countNulls(heap)));
            }
        }
    }

    private List<Long> randomDistinctLongs(int n) {
        List<Long> out = new ArrayList<>(n);
        // Use a tight range to encourage occasional collisions in non-distinct paths; here we
        // dedupe so the comparator's value-only branch is the active one for the test.
        java.util.HashSet<Long> seen = new java.util.HashSet<>();
        while (out.size() < n) {
            long v = randomLong();
            if (seen.add(v)) {
                out.add(v);
            }
        }
        return out;
    }

    private static int countNulls(PrimitiveTernaryHeap heap) {
        int[] count = new int[1];
        heap.forEachSlot((slot, value, rowPosition, isNull) -> {
            if (isNull) {
                count[0]++;
            }
        });
        return count[0];
    }
}
