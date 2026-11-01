/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end tests for {@code @allocates} estimators on collection materialization and copying ({@code toArray} and the
 * copy constructors), whose result scales with the source collection's size.
 */
public class AllocationCollectionCopyTests extends AllocationTestCase {

    /** One list add. */
    private static final long ADD = AllocationEstimators.collectionAddBytes(new ArrayList<>(), null);

    /** One map put. */
    private static final long PUT = AllocationEstimators.mapPutBytes(new HashMap<>(), null, null);

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
        // one put, then a copy sized from the source.
        Map<String, String> one = new HashMap<>();
        one.put("a", "b");
        long expected = 64L + PUT + AllocationEstimators.mapCopyBytes(one);
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
        // 200 bits need 4 words; 20,000 need 313. The charge grows with the word array, not the object.
        assertThat(AllocationEstimators.bitSetBytes(20_000), greaterThan(AllocationEstimators.bitSetBytes(200)));
        assertEquals(AllocationEstimators.bitSetBytes(20_000), allocatedBytes("new BitSet(20000); return \"x\";"));
    }

    public void testBitSetSizedTripsLimit() {
        assertTripsLimit("new BitSet(1000000); return \"x\";", "1kb");
    }

    public void testArrayDequeCopyCharged() {
        // The inner new ArrayList() costs 40. The deque copy makes an array one slot bigger than the source.
        long expected = 40L + AllocationEstimators.arrayDequeCollectionBytes(new ArrayList<>());
        assertEquals(expected, allocatedBytes("new ArrayDeque(new ArrayList()); return \"x\";"));
    }

    public void testHashtableCopyScalesWithSize() {
        Map<String, String> one = new HashMap<>();
        one.put("a", "b");
        long expected = 64L + PUT + AllocationEstimators.hashtableCopyBytes(one);
        assertEquals(expected, allocatedBytes("Map m = new HashMap(); m.put(\"a\", \"b\"); new Hashtable(m); return \"x\";"));
        assertThat(AllocationEstimators.hashtableCopyBytes(one), greaterThan(AllocationEstimators.hashtableCopyBytes(new HashMap<>())));
    }

    public void testIdentityHashMapCopyCharged() {
        Map<String, String> one = new HashMap<>();
        one.put("a", "b");
        long expected = 64L + PUT + AllocationEstimators.identityHashMapCopyBytes(one);
        assertEquals(expected, allocatedBytes("Map m = new HashMap(); m.put(\"a\", \"b\"); new IdentityHashMap(m); return \"x\";"));
    }

    public void testIdentityHashMapCopyCoversTheTableTheJdkPicks() {
        // The JDK plans for 1.1 * (size + 1) entries, rounds up to a power of two, and uses two references per slot.
        // The estimate takes the largest value that rounding can give, so it must be at least the real table at every size.
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
        long expected = 40L + 2 * ADD + AllocationEstimators.subListBytes(List.of(), 0, 0);
        assertEquals(expected, allocatedBytes("List l = new ArrayList(); l.add(\"a\"); l.add(\"b\"); l.subList(0, 1); return \"x\";"));
    }

    /**
     * A lambda with no user captures still allocates a capture object. It is charged where the lambda is built, on both the
     * typed and the def path. Allocation tracking adds one hidden {@code #scriptThis} capture so the body can reach the
     * counter, so the object has one slot.
     */
    private static final long LAMBDA_BYTES = AllocSizes.captureSize(1);

    public void testCollectCharged() {
        // the list, two adds, the lambda's capture object, and the collect result.
        long expected = 40L + 2 * ADD + LAMBDA_BYTES + AllocationEstimators.collectBytes(null, List.of("a", "b"), null);
        assertEquals(expected, allocatedBytes("List l = new ArrayList(); l.add(\"a\"); l.add(\"b\"); l.collect(x -> x); return \"x\";"));
    }

    public void testCollectChargedThroughDef() {
        // A @script_aware augmentation with @allocates, called through def with an inline lambda. The def lookup has to charge
        // on its lambda-argument path, which used to be a known gap, and put the script slot where the estimator expects it.
        // The def lambda's capture object is charged the same as a typed one.
        long expected = 40L + ADD + LAMBDA_BYTES + AllocationEstimators.collectBytes(null, List.of("a"), null);
        assertEquals(expected, allocatedBytes("def l = new ArrayList(); l.add(\"a\"); l.collect(x -> x); return \"x\";"));
    }

    public void testSplitChargedThroughDef() {
        // A def lambda returns def, so its boolean result gets boxed, and that box is charged like any other.
        long expected = 40L + ADD + LAMBDA_BYTES + AllocSizes.boxSize(boolean.class) + AllocationEstimators.splitBytes(
            null,
            List.of("a"),
            null
        );
        assertEquals(expected, allocatedBytes("def l = new ArrayList(); l.add(\"a\"); l.split(x -> true); return \"x\";"));
    }

    public void testCollectThroughDefTripsLimit() {
        assertTripsLimit("def l = new ArrayList(); l.collect(x -> x); return \"x\";", "100b");
    }

    public void testMapCollectCharged() {
        long expected = 64L + PUT + LAMBDA_BYTES + AllocationEstimators.collectBytes(null, Map.of("a", "b"), null);
        assertEquals(expected, allocatedBytes("Map m = new HashMap(); m.put(\"a\", \"b\"); m.collect((k, v) -> k); return \"x\";"));
    }

    public void testSplitCharged() {
        long expected = 40L + ADD + LAMBDA_BYTES + AllocationEstimators.splitBytes(null, List.of("a"), null);
        assertEquals(expected, allocatedBytes("List l = new ArrayList(); l.add(\"a\"); l.split(x -> true); return \"x\";"));
    }

    public void testCollectTripsLimit() {
        // The list object and the lambda together are under 100 bytes. The collect result is not.
        assertTripsLimit("List l = new ArrayList(); l.collect(x -> x); return \"x\";", "100b");
    }

    // ---- add, put and friends: a fixed cost per element ----

    public void testListAddChargedPerElement() {
        assertEquals(40L + 2 * ADD, allocatedBytes("List l = new ArrayList(); l.add(\"a\"); l.add(\"b\"); return \"x\";"));
    }

    public void testSetAddChargedAsMapEntry() {
        // A set is a map underneath, so an add costs a put, more than a list slot.
        assertEquals(PUT, AllocationEstimators.collectionAddBytes(new HashSet<>(), null));
        assertThat(PUT, greaterThan(ADD));
        assertEquals(AllocationEstimators.hashSetShellBytes() + PUT, allocatedBytes("Set s = new HashSet(); s.add(\"a\"); return \"x\";"));
    }

    public void testAddAllChargesOneAddPerSourceElement() {
        String build = "List l = new ArrayList(); l.add(\"a\"); l.add(\"b\"); l.add(\"c\"); ";
        long list = allocatedBytes(build + "return \"x\";");
        long withAddAll = allocatedBytes(build + "l.addAll(l); return \"x\";");

        assertEquals(3 * ADD, withAddAll - list);
        assertEquals(3 * ADD, AllocationEstimators.collectionAddAllBytes(new ArrayList<>(), List.of("a", "b", "c")));
    }

    public void testMapPutAndPutAllCharged() {
        String build = "Map m = new HashMap(); m.put(\"a\", 1); m.put(\"b\", 2); ";
        long boxes = 2 * AllocSizes.boxSize(int.class);
        long map = allocatedBytes(build + "return \"x\";");
        long withPutAll = allocatedBytes(build + "new HashMap().putAll(m); return \"x\";");

        assertEquals(64L + 2 * PUT + boxes, map);
        assertEquals(64L + 2 * PUT, withPutAll - map);
    }

    public void testDequeAddsChargedThroughInheritedMethods() {
        // ArrayDeque inherits its adds from Deque.
        long each = AllocationEstimators.dequeAddBytes(new ArrayDeque<>(), null);
        assertEquals(
            AllocationEstimators.arrayDequeShellBytes() + 3 * each,
            allocatedBytes("ArrayDeque d = new ArrayDeque(); d.addFirst(\"a\"); d.offerLast(\"b\"); d.push(\"c\"); return \"x\";")
        );
    }

    public void testAddChargedThroughDef() {
        // Same charge as a typed call. The int is boxed inside the method handle, so that box is not charged.
        assertEquals(40L + ADD, allocatedBytes("def l = new ArrayList(); l.add(1); return \"x\";"));
    }

    public void testStringJoinerCharged() {
        long expected = AllocationEstimators.stringJoinerBytes(",") + AllocationEstimators.stringJoinerAddBytes(null, "a")
            + AllocationEstimators.stringJoinerAddBytes(null, "bb");
        assertEquals(expected, allocatedBytes("StringJoiner j = new StringJoiner(\",\"); j.add(\"a\"); j.add(\"bb\"); return \"x\";"));
    }

    // ---- BitSet grows only when a bit lands past its words ----

    public void testBitSetSetChargesOnlyWhenItGrows() {
        BitSet one = new BitSet();
        one.set(1);

        assertEquals(0L, AllocationEstimators.bitSetGrowBytes(new BitSet(), 1));
        assertThat(AllocationEstimators.bitSetGrowBytes(one, 1000), greaterThan(0L));
        assertEquals(
            AllocationEstimators.bitSetShellBytes() + AllocationEstimators.bitSetGrowBytes(one, 1000),
            allocatedBytes("BitSet b = new BitSet(); b.set(1); b.set(1000); return \"x\";")
        );
    }

    public void testBitSetHugeIndexTripsLimit() {
        // One call asks for a 256mb array. The pre-check stops it first.
        assertTripsLimit("BitSet b = new BitSet(); b.set(Integer.MAX_VALUE - 1); return \"x\";", "1mb");
    }
}
