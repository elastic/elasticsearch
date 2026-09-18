/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end tests for {@code @allocates} estimators on collection materialization and copying ({@code toArray} and the
 * copy constructors), whose result scales with the source collection's size.
 */
public class AllocationCollectionCopyTests extends AllocationTestCase {

    public void testToArrayCharged() {
        // new ArrayList() charges 40; toArray() on the empty list charges a new Object[0].
        long expected = 40L + AllocSizes.arrayBytes(0, AllocSizes.REFERENCE_SIZE);
        assertEquals(expected, allocatedBytes("new ArrayList().toArray(); return \"x\";"));
    }

    public void testToArrayTripsLimit() {
        assertTripsLimit("new ArrayList().toArray(); return \"x\";");
    }

    public void testMapCopyEmptyCharged() {
        // inner new HashMap() = 64; outer copy of the empty map = the map shell only.
        long expected = 64L + AllocationEstimators.mapCopyBytes(new HashMap<>());
        assertEquals(expected, allocatedBytes("new HashMap(new HashMap()); return \"x\";"));
    }

    public void testMapCopyScalesWithSize() {
        // inner map populated with one entry (put is not charged), then copied: charge scales with the source size.
        Map<String, String> one = new HashMap<>();
        one.put("a", "b");
        long expected = 64L + AllocationEstimators.mapCopyBytes(one);
        assertEquals(expected, allocatedBytes("Map m = new HashMap(); m.put(\"a\", \"b\"); new HashMap(m); return \"x\";"));
    }

    public void testSetCopyEmptyCharged() {
        // inner new ArrayList() = 40; outer HashSet copy of the empty collection = the set shell only.
        long expected = 40L + AllocationEstimators.setCopyBytes(new java.util.ArrayList<>());
        assertEquals(expected, allocatedBytes("new HashSet(new ArrayList()); return \"x\";"));
    }

    public void testLinkedListCopyEmptyCharged() {
        long expected = 40L + AllocationEstimators.linkedListCopyBytes(new java.util.ArrayList<>());
        assertEquals(expected, allocatedBytes("new LinkedList(new ArrayList()); return \"x\";"));
    }

    public void testCopyConstructorTripsLimit() {
        assertTripsLimit("new HashSet(new ArrayList()); return \"x\";");
    }

    // ---- sized and copying constructors ----

    public void testBitSetSizedCharged() {
        assertEquals(AllocationEstimators.bitSetBytes(200), allocatedBytes("new BitSet(200); return \"x\";"));
    }

    public void testBitSetSizedScalesWithBits() {
        // 200 bits fit in 4 words; 20,000 need 313. The charge follows the word array, not the object.
        assertThat(AllocationEstimators.bitSetBytes(20_000), greaterThan(AllocationEstimators.bitSetBytes(200)));
        assertEquals(AllocationEstimators.bitSetBytes(20_000), allocatedBytes("new BitSet(20000); return \"x\";"));
    }

    public void testBitSetSizedTripsLimit() {
        assertTripsLimit("new BitSet(1000000); return \"x\";", "1kb");
    }

    public void testArrayDequeCopyCharged() {
        // inner new ArrayList() = 40; the deque copy sizes its array to the source plus one slot.
        long expected = 40L + AllocationEstimators.arrayDequeCollectionBytes(new ArrayList<>());
        assertEquals(expected, allocatedBytes("new ArrayDeque(new ArrayList()); return \"x\";"));
    }

    public void testHashtableCopyScalesWithSize() {
        Map<String, String> one = new HashMap<>();
        one.put("a", "b");
        long expected = 64L + AllocationEstimators.hashtableCopyBytes(one);
        assertEquals(expected, allocatedBytes("Map m = new HashMap(); m.put(\"a\", \"b\"); new Hashtable(m); return \"x\";"));
        assertThat(AllocationEstimators.hashtableCopyBytes(one), greaterThan(AllocationEstimators.hashtableCopyBytes(new HashMap<>())));
    }

    public void testIdentityHashMapCopyCharged() {
        Map<String, String> one = new HashMap<>();
        one.put("a", "b");
        long expected = 64L + AllocationEstimators.identityHashMapCopyBytes(one);
        assertEquals(expected, allocatedBytes("Map m = new HashMap(); m.put(\"a\", \"b\"); new IdentityHashMap(m); return \"x\";"));
    }

    public void testIdentityHashMapCopyCoversTheTableTheJdkPicks() {
        // The JDK expects 1.1 * (size + 1) mappings, rounds to a power-of-two capacity, and stores two references per slot.
        // The estimate takes the top of the rounding range, so it must cover the real table at every size.
        for (int size : new int[] { 0, 1, 2, 3, 5, 10, 21, 22, 43, 100, 1000, 12345 }) {
            int expectedMaxSize = (int) ((1 + size) * 1.1);
            int capacity = Math.max(4, Integer.highestOneBit(expectedMaxSize * 3));
            long jdkTable = AllocSizes.arrayBytes(2L * capacity, AllocSizes.REFERENCE_SIZE);
            Map<Integer, Integer> source = new HashMap<>();
            for (int i = 0; i < size; i++) {
                source.put(i, i);
            }
            assertThat("size " + size, AllocationEstimators.identityHashMapCopyBytes(source), greaterThanOrEqualTo(32L + jdkTable));
        }
    }

    public void testCopyConstructorsTripLimit() {
        assertTripsLimit("new ArrayDeque(new ArrayList()); return \"x\";");
        assertTripsLimit("new Hashtable(new HashMap()); return \"x\";");
        assertTripsLimit("new IdentityHashMap(new HashMap()); return \"x\";");
    }

    // ---- views and list-building augmentations ----

    public void testSubListCharged() {
        long expected = 40L + AllocationEstimators.subListBytes(List.of(), 0, 0);
        assertEquals(expected, allocatedBytes("List l = new ArrayList(); l.add(\"a\"); l.add(\"b\"); l.subList(0, 1); return \"x\";"));
    }

    /**
     * A lambda with no user captures still allocates a capture object, charged where the lambda is built, on the typed and
     * the def path alike. Allocation tracking injects one synthetic {@code #scriptThis} capture so the body can reach the
     * counter, hence one slot.
     */
    private static final long LAMBDA_BYTES = AllocSizes.captureSize(1);

    public void testCollectCharged() {
        // new ArrayList() = 40, the lambda's capture object, then the collect result sized to the two elements.
        long expected = 40L + LAMBDA_BYTES + AllocationEstimators.collectBytes(null, List.of("a", "b"), null);
        assertEquals(expected, allocatedBytes("List l = new ArrayList(); l.add(\"a\"); l.add(\"b\"); l.collect(x -> x); return \"x\";"));
    }

    public void testCollectChargedThroughDef() {
        // First @allocates on a @script_aware augmentation reached through def with an inline lambda: the def lookup must charge
        // on the lambda-argument path (previously a known gap) and line the script slot up with the estimator's. The def
        // lambda's capture object is charged like a typed one.
        long expected = 40L + LAMBDA_BYTES + AllocationEstimators.collectBytes(null, List.of("a"), null);
        assertEquals(expected, allocatedBytes("def l = new ArrayList(); l.add(\"a\"); l.collect(x -> x); return \"x\";"));
    }

    public void testSplitChargedThroughDef() {
        // A def lambda returns def, so its boolean result is boxed on the way out and that box is charged like any other.
        long expected = 40L + LAMBDA_BYTES + AllocSizes.boxSize(boolean.class) + AllocationEstimators.splitBytes(null, List.of("a"), null);
        assertEquals(expected, allocatedBytes("def l = new ArrayList(); l.add(\"a\"); l.split(x -> true); return \"x\";"));
    }

    public void testCollectThroughDefTripsLimit() {
        assertTripsLimit("def l = new ArrayList(); l.collect(x -> x); return \"x\";", "100b");
    }

    public void testMapCollectCharged() {
        long expected = 64L + LAMBDA_BYTES + AllocationEstimators.collectBytes(null, Map.of("a", "b"), null);
        assertEquals(expected, allocatedBytes("Map m = new HashMap(); m.put(\"a\", \"b\"); m.collect((k, v) -> k); return \"x\";"));
    }

    public void testSplitCharged() {
        long expected = 40L + LAMBDA_BYTES + AllocationEstimators.splitBytes(null, List.of("a"), null);
        assertEquals(expected, allocatedBytes("List l = new ArrayList(); l.add(\"a\"); l.split(x -> true); return \"x\";"));
    }

    public void testCollectTripsLimit() {
        // The list shell and the lambda fit under 100b; the collect result does not.
        assertTripsLimit("List l = new ArrayList(); l.collect(x -> x); return \"x\";", "100b");
    }
}
