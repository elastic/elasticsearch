/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.util;

import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

public class ObjectArrayPriorityQueueTests extends ESTestCase {

    private static BigArrays randombigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private static class IntegerQueue extends ObjectArrayPriorityQueue<Integer> {
        IntegerQueue(int count, BigArrays bigArrays) {
            super(count, bigArrays);
        }

        @Override
        protected boolean lessThan(Integer a, Integer b) {
            return (a < b);
        }

        protected final void checkValidity() {
            for (int i = 1; i <= size(); i++) {
                int parent = i >>> 1;
                if (parent > 1) {
                    if (lessThan(heap.get(parent), heap.get(i)) == false) {
                        assertThat(heap.get(i), Matchers.equalTo(heap.get(parent)));
                    }
                }
            }
        }
    }

    public void testZeroSizedQueue() {
        try (ObjectArrayPriorityQueue<Integer> pq = new IntegerQueue(0, randombigArrays())) {
            assertEquals((Object) 1, pq.insertWithOverflow(1));
            assertEquals(0, pq.size());

            // should fail, but passes and modifies the top...
            pq.add(1);
            assertEquals((Object) 1, pq.top());
        }
    }

    public void testNoExtraWorkOnEqualElements() {
        class Value {
            private final int index;
            private final int value;

            Value(int index, int value) {
                this.index = index;
                this.value = value;
            }
        }

        try (ObjectArrayPriorityQueue<Value> pq = new ObjectArrayPriorityQueue<>(5, randombigArrays()) {
            @Override
            protected boolean lessThan(Value a, Value b) {
                return a.value < b.value;
            }
        }) {

            // Make all elements equal but record insertion order.
            for (int i = 0; i < 100; i++) {
                pq.insertWithOverflow(new Value(i, 0));
            }

            ArrayList<Integer> indexes = new ArrayList<>();
            for (Value e : pq) {
                indexes.add(e.index);
            }

            // All elements are "equal" so we should have exactly the indexes of those elements that were
            // added first.
            MatcherAssert.assertThat(indexes, Matchers.containsInAnyOrder(0, 1, 2, 3, 4));
        }
    }

    public void testPQ() {
        testPQ(atLeast(10000), random());
    }

    public static void testPQ(int count, Random gen) {
        try (ObjectArrayPriorityQueue<Integer> pq = new IntegerQueue(count, randombigArrays())) {
            int sum = 0, sum2 = 0;

            for (int i = 0; i < count; i++) {
                int next = gen.nextInt();
                sum += next;
                pq.add(next);
            }

            int last = Integer.MIN_VALUE;
            for (int i = 0; i < count; i++) {
                Integer next = pq.pop();
                assertTrue(next.intValue() >= last);
                last = next.intValue();
                sum2 += last;
            }

            assertEquals(sum, sum2);
        }
    }

    public void testFixedSize() {
        try (ObjectArrayPriorityQueue<Integer> pq = new IntegerQueue(3, randombigArrays())) {
            pq.insertWithOverflow(2);
            pq.insertWithOverflow(3);
            pq.insertWithOverflow(1);
            pq.insertWithOverflow(5);
            pq.insertWithOverflow(7);
            pq.insertWithOverflow(1);
            assertEquals(3, pq.size());
            assertEquals((Integer) 3, pq.top());
        }
    }

    public void testInsertWithOverflow() {
        int size = 4;
        try (ObjectArrayPriorityQueue<Integer> pq = new IntegerQueue(size, randombigArrays())) {
            Integer i1 = 2;
            Integer i2 = 3;
            Integer i3 = 1;
            Integer i4 = 5;
            Integer i5 = 7;
            Integer i6 = 1;

            assertNull(pq.insertWithOverflow(i1));
            assertNull(pq.insertWithOverflow(i2));
            assertNull(pq.insertWithOverflow(i3));
            assertNull(pq.insertWithOverflow(i4));
            assertSame(pq.insertWithOverflow(i5), i3); // i3 should have been dropped
            assertSame(pq.insertWithOverflow(i6), i6); // i6 should not have been inserted
            assertEquals(size, pq.size());
            assertEquals((Integer) 2, pq.top());
        }
    }

    public void testAddAllToEmptyQueue() {
        Random random = random();
        int size = 10;
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(random.nextInt());
        }
        try (IntegerQueue pq = new IntegerQueue(size, randombigArrays())) {
            pq.addAll(list);
            pq.checkValidity();
            assertOrderedWhenDrained(pq, list);
        }
    }

    public void testAddAllToPartiallyFilledQueue() {
        try (IntegerQueue pq = new IntegerQueue(20, randombigArrays())) {
            List<Integer> oneByOne = new ArrayList<>();
            List<Integer> bulkAdded = new ArrayList<>();
            Random random = random();
            for (int i = 0; i < 10; i++) {
                bulkAdded.add(random.nextInt());

                int x = random.nextInt();
                pq.add(x);
                oneByOne.add(x);
            }

            pq.addAll(bulkAdded);
            pq.checkValidity();

            oneByOne.addAll(bulkAdded); // Gather all "reference" data.
            assertOrderedWhenDrained(pq, oneByOne);
        }
    }

    public void testAddAllDoesNotFitIntoQueue() {
        try (IntegerQueue pq = new IntegerQueue(20, randombigArrays())) {
            List<Integer> list = new ArrayList<>();
            Random random = random();
            for (int i = 0; i < 11; i++) {
                list.add(random.nextInt());
                pq.add(random.nextInt());
            }

            assertThrows(
                "Cannot add 11 elements to a queue with remaining capacity: 9",
                ArrayIndexOutOfBoundsException.class,
                () -> pq.addAll(list)
            );
        }
    }

    public void testRemovalsAndInsertions() {
        Random random = random();
        int numDocsInPQ = TestUtil.nextInt(random, 1, 100);
        try (IntegerQueue pq = new IntegerQueue(numDocsInPQ, randombigArrays())) {
            Integer lastLeast = null;

            // Basic insertion of new content
            ArrayList<Integer> sds = new ArrayList<Integer>(numDocsInPQ);
            for (int i = 0; i < numDocsInPQ * 10; i++) {
                Integer newEntry = Math.abs(random.nextInt());
                sds.add(newEntry);
                Integer evicted = pq.insertWithOverflow(newEntry);
                pq.checkValidity();
                if (evicted != null) {
                    assertTrue(sds.remove(evicted));
                    if (evicted != newEntry) {
                        assertSame(evicted, lastLeast);
                    }
                }
                Integer newLeast = pq.top();
                if ((lastLeast != null) && (newLeast != newEntry) && (newLeast != lastLeast)) {
                    // If there has been a change of least entry and it wasn't our new
                    // addition we expect the scores to increase
                    assertTrue(newLeast <= newEntry);
                    assertTrue(newLeast >= lastLeast);
                }
                lastLeast = newLeast;
            }

            // Try many random additions to existing entries - we should always see
            // increasing scores in the lowest entry in the PQ
            for (int p = 0; p < 500000; p++) {
                int element = (int) (random.nextFloat() * (sds.size() - 1));
                Integer objectToRemove = sds.get(element);
                assertSame(sds.remove(element), objectToRemove);
                assertTrue(pq.remove(objectToRemove));
                pq.checkValidity();
                Integer newEntry = Math.abs(random.nextInt());
                sds.add(newEntry);
                assertNull(pq.insertWithOverflow(newEntry));
                pq.checkValidity();
                Integer newLeast = pq.top();
                if ((objectToRemove != lastLeast) && (lastLeast != null) && (newLeast != newEntry)) {
                    // If there has been a change of least entry and it wasn't our new
                    // addition or the loss of our randomly removed entry we expect the
                    // scores to increase
                    assertTrue(newLeast <= newEntry);
                    assertTrue(newLeast >= lastLeast);
                }
                lastLeast = newLeast;
            }
        }
    }

    public void testIteratorEmpty() {
        try (IntegerQueue queue = new IntegerQueue(3, randombigArrays())) {
            Iterator<Integer> it = queue.iterator();
            assertFalse(it.hasNext());
            expectThrows(NoSuchElementException.class, () -> { it.next(); });
        }
    }

    public void testIteratorOne() {
        try (IntegerQueue queue = new IntegerQueue(3, randombigArrays())) {
            queue.add(1);
            Iterator<Integer> it = queue.iterator();
            assertTrue(it.hasNext());
            assertEquals(Integer.valueOf(1), it.next());
            assertFalse(it.hasNext());
            expectThrows(NoSuchElementException.class, () -> { it.next(); });
        }
    }

    public void testIteratorTwo() {
        try (IntegerQueue queue = new IntegerQueue(3, randombigArrays())) {
            queue.add(1);
            queue.add(2);
            Iterator<Integer> it = queue.iterator();
            assertTrue(it.hasNext());
            assertEquals(Integer.valueOf(1), it.next());
            assertTrue(it.hasNext());
            assertEquals(Integer.valueOf(2), it.next());
            assertFalse(it.hasNext());
            expectThrows(NoSuchElementException.class, () -> { it.next(); });
        }
    }

    public void testIteratorRandom() {
        final int maxSize = TestUtil.nextInt(random(), 1, 20);
        try (IntegerQueue queue = new IntegerQueue(maxSize, randombigArrays())) {
            final int iters = atLeast(100);
            final List<Integer> expected = new ArrayList<>();
            for (int iter = 0; iter < iters; ++iter) {
                if (queue.size() == 0 || (queue.size() < maxSize && random().nextBoolean())) {
                    final Integer value = random().nextInt(10);
                    queue.add(value);
                    expected.add(value);
                } else {
                    expected.remove(queue.pop());
                }
                List<Integer> actual = new ArrayList<>();
                for (Integer value : queue) {
                    actual.add(value);
                }
                CollectionUtil.introSort(expected);
                CollectionUtil.introSort(actual);
                assertEquals(expected, actual);
            }
        }
    }

    public void testMaxIntSize() {
        expectThrows(IllegalArgumentException.class, () -> {
            new ObjectArrayPriorityQueue<Boolean>(Long.MAX_VALUE, randombigArrays()) {
                @Override
                public boolean lessThan(Boolean a, Boolean b) {
                    // uncalled
                    return true;
                }
            };
        });
    }

    private void assertOrderedWhenDrained(IntegerQueue pq, List<Integer> referenceDataList) {
        Collections.sort(referenceDataList);
        int i = 0;
        while (pq.size() > 0) {
            assertEquals(pq.pop(), referenceDataList.get(i));
            i++;
        }
    }
}
