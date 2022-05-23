/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.CollectionUtils.eagerPartition;
import static org.elasticsearch.common.util.CollectionUtils.limitSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class CollectionUtilsTests extends ESTestCase {
    public void testRotateEmpty() {
        assertTrue(CollectionUtils.rotate(Collections.emptyList(), randomInt()).isEmpty());
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
                assertThat(cmp.compare(listCopy.get(i), listCopy.get(i + 1)), lessThan(0));
            }
            assertThat(listCopy.size(), equalTo(size));
        }
    }

    public void testUniquify() {
        assertUniquify(List.<Integer>of(), Comparator.naturalOrder(), 0);
        assertUniquify(List.of(1), Comparator.naturalOrder(), 1);
        assertUniquify(List.of(1, 2, 3), Comparator.naturalOrder(), 3);
        assertUniquify(List.of(1, 1, 1), Comparator.naturalOrder(), 1);
        assertUniquify(List.of(1, 2, 2, 3), Comparator.naturalOrder(), 3);
        assertUniquify(List.of(1, 2, 2, 2), Comparator.naturalOrder(), 2);
    }

    public void testEmptyPartition() {
        assertEquals(Collections.emptyList(), eagerPartition(Collections.emptyList(), 1));
    }

    public void testSimplePartition() {
        assertEquals(
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4), Arrays.asList(5)),
            eagerPartition(Arrays.asList(1, 2, 3, 4, 5), 2)
        );
    }

    public void testSingletonPartition() {
        assertEquals(
            Arrays.asList(Arrays.asList(1), Arrays.asList(2), Arrays.asList(3), Arrays.asList(4), Arrays.asList(5)),
            eagerPartition(Arrays.asList(1, 2, 3, 4, 5), 1)
        );
    }

    public void testOversizedPartition() {
        assertEquals(Arrays.asList(Arrays.asList(1, 2, 3, 4, 5)), eagerPartition(Arrays.asList(1, 2, 3, 4, 5), 15));
    }

    public void testPerfectPartition() {
        assertEquals(
            Arrays.asList(Arrays.asList(1, 2, 3, 4, 5, 6), Arrays.asList(7, 8, 9, 10, 11, 12)),
            eagerPartition(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 6)
        );
    }

    public void testEnsureNoSelfReferences() {
        CollectionUtils.ensureNoSelfReferences(emptyMap(), "test with empty map");
        CollectionUtils.ensureNoSelfReferences(null, "test with null");

        {
            Map<String, Object> map = new HashMap<>();
            map.put("field", map);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> CollectionUtils.ensureNoSelfReferences(map, "test with self ref value")
            );
            assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref value)"));
        }
        {
            Map<Object, Object> map = new HashMap<>();
            map.put(map, 1);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> CollectionUtils.ensureNoSelfReferences(map, "test with self ref key")
            );
            assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref key)"));
        }

    }

    public void testLimitSizeOfShortList() {
        var shortList = randomList(0, 10, () -> "item");
        assertThat(limitSize(shortList, 10), equalTo(shortList));
    }

    public void testLimitSizeOfLongList() {
        var longList = randomList(10, 100, () -> "item");
        assertThat(limitSize(longList, 10), equalTo(longList.subList(0, 10)));
    }
}
