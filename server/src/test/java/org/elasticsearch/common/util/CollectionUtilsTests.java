/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopyNoNullElements;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.common.util.CollectionUtils.eagerPartition;
import static org.elasticsearch.common.util.CollectionUtils.ensureNoSelfReferences;
import static org.elasticsearch.common.util.CollectionUtils.limitSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class CollectionUtilsTests extends ESTestCase {
    public void testRotateEmpty() {
        assertTrue(CollectionUtils.rotate(List.of(), randomInt()).isEmpty());
    }

    public void testRotate() {
        final int iters = scaledRandomIntBetween(10, 100);
        for (int k = 0; k < iters; ++k) {
            final int size = randomIntBetween(1, 100);
            final int distance = randomInt();
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < size; ++i) {
                list.add(new Object());
            }
            final List<Object> rotated = CollectionUtils.rotate(list, distance);
            // check content is the same
            assertEquals(rotated.size(), list.size());
            assertEquals(rotated.size(), list.size());
            assertEquals(new HashSet<>(rotated), new HashSet<>(list));
            // check stability
            for (int j = randomInt(4); j >= 0; --j) {
                assertEquals(rotated, CollectionUtils.rotate(list, distance));
            }
            // reverse
            if (distance != Integer.MIN_VALUE) {
                assertEquals(list, CollectionUtils.rotate(CollectionUtils.rotate(list, distance), -distance));
            }
        }
    }

    private <T> void assertUniquify(List<T> list, Comparator<T> cmp, int size) {
        for (List<T> listCopy : List.of(new ArrayList<T>(list), new LinkedList<T>(list))) {
            CollectionUtils.uniquify(listCopy, cmp);
            for (int i = 0; i < listCopy.size() - 1; ++i) {
                assertThat(listCopy.get(i) + " < " + listCopy.get(i + 1), cmp.compare(listCopy.get(i), listCopy.get(i + 1)), lessThan(0));
            }
            assertThat(listCopy.size(), equalTo(size));
        }
    }

    public void testUniquify() {
        assertUniquify(List.<Integer>of(), Comparator.naturalOrder(), 0);
        assertUniquify(List.of(1), Comparator.naturalOrder(), 1);
        assertUniquify(List.of(1, 2, 3), Comparator.naturalOrder(), 3);
        assertUniquify(List.of(1, 1, 3), Comparator.naturalOrder(), 2);
        assertUniquify(List.of(1, 1, 1), Comparator.naturalOrder(), 1);
        assertUniquify(List.of(1, 2, 2, 3), Comparator.naturalOrder(), 3);
        assertUniquify(List.of(1, 2, 2, 2), Comparator.naturalOrder(), 2);
        assertUniquify(List.of(1, 2, 2, 3, 3, 5), Comparator.naturalOrder(), 4);

        for (int i = 0; i < 10; ++i) {
            int uniqueItems = randomIntBetween(1, 10);
            var list = new ArrayList<Integer>();
            int next = 1;
            for (int j = 0; j < uniqueItems; ++j) {
                int occurences = randomIntBetween(1, 10);
                while (occurences-- > 0) {
                    list.add(next);
                }
                next++;
            }
            assertUniquify(list, Comparator.naturalOrder(), uniqueItems);
        }
    }

    public void testEmptyPartition() {
        assertEquals(List.of(), eagerPartition(List.of(), 1));
    }

    public void testSimplePartition() {
        assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5)), eagerPartition(List.of(1, 2, 3, 4, 5), 2));
    }

    public void testSingletonPartition() {
        assertEquals(List.of(List.of(1), List.of(2), List.of(3), List.of(4), List.of(5)), eagerPartition(List.of(1, 2, 3, 4, 5), 1));
    }

    public void testOversizedPartition() {
        assertEquals(List.of(List.of(1, 2, 3, 4, 5)), eagerPartition(List.of(1, 2, 3, 4, 5), 15));
    }

    public void testPerfectPartition() {
        assertEquals(
            List.of(List.of(1, 2, 3, 4, 5, 6), List.of(7, 8, 9, 10, 11, 12)),
            eagerPartition(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 6)
        );
    }

    public void testEnsureNoSelfReferences() {
        ensureNoSelfReferences("string", "test with a string");
        ensureNoSelfReferences(2, "test with a number");
        ensureNoSelfReferences(true, "test with a boolean");
        ensureNoSelfReferences(Map.of(), "test with an empty map");
        ensureNoSelfReferences(Set.of(), "test with an empty set");
        ensureNoSelfReferences(List.of(), "test with an empty list");
        ensureNoSelfReferences(new Object[0], "test with an empty array");
        ensureNoSelfReferences((Iterable<?>) Collections::emptyIterator, "test with an empty iterable");
    }

    public void testEnsureNoSelfReferencesMap() {
        // map value
        {
            Map<String, Object> map = new HashMap<>();
            map.put("field", map);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ensureNoSelfReferences(map, "test with self ref value")
            );
            assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref value)"));
        }
        // map key
        {
            Map<Object, Object> map = new HashMap<>();
            map.put(map, 1);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ensureNoSelfReferences(map, "test with self ref key")
            );
            assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref key)"));
        }
        // nested map value
        {
            Map<String, Object> map = new HashMap<>();
            map.put("field", Set.of(List.of((Iterable<?>) () -> Iterators.single(new Object[] { map }))));

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ensureNoSelfReferences(map, "test with self ref nested value")
            );
            assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref nested value)"));
        }
    }

    public void testEnsureNoSelfReferencesSet() {
        Set<Object> set = new HashSet<>();
        set.add("foo");
        set.add(set);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ensureNoSelfReferences(set, "test with self ref set")
        );
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref set)"));
    }

    public void testEnsureNoSelfReferencesList() {
        List<Object> list = new ArrayList<>();
        list.add("foo");
        list.add(list);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ensureNoSelfReferences(list, "test with self ref list")
        );
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref list)"));
    }

    public void testEnsureNoSelfReferencesArray() {
        Object[] array = new Object[2];
        array[0] = "foo";
        array[1] = array;

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ensureNoSelfReferences(array, "test with self ref array")
        );
        assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref array)"));
    }

    public void testLimitSizeOfShortList() {
        var shortList = randomList(0, 10, () -> "item");
        assertThat(limitSize(shortList, 10), equalTo(shortList));
    }

    public void testLimitSizeOfLongList() {
        var longList = randomList(10, 100, () -> "item");
        assertThat(limitSize(longList, 10), equalTo(longList.subList(0, 10)));
    }

    public void testAppendToCopyNoNullElements() {
        final List<String> oldList = randomList(3, () -> randomAlphaOfLength(10));
        final String[] extraElements = randomArray(2, 4, String[]::new, () -> randomAlphaOfLength(10));
        final List<String> newList = appendToCopyNoNullElements(oldList, extraElements);
        assertThat(newList, equalTo(concatLists(oldList, List.of(extraElements))));
    }
}
