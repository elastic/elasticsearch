/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.iterable;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.object.HasToString.hasToString;

public class IterablesTests extends ESTestCase {
    public void testGetOverList() {
        test(Arrays.asList("a", "b", "c"));
    }

    public void testGetOverIterable() {
        Iterable<String> iterable = () -> new Iterator<String>() {
            private int position = 0;

            @Override
            public boolean hasNext() {
                return position < 3;
            }

            @Override
            public String next() {
                if (position < 3) {
                    String s = position == 0 ? "a" : position == 1 ? "b" : "c";
                    position++;
                    return s;
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
        test(iterable);
    }

    public void testFlatten() {
        List<List<Integer>> list = new ArrayList<>();
        list.add(new ArrayList<>());

        Iterable<Integer> allInts = Iterables.flatten(list);
        int count = 0;
        for (@SuppressWarnings("unused")
        int x : allInts) {
            count++;
        }
        assertEquals(0, count);
        list.add(new ArrayList<>());
        list.get(1).add(0);

        // changes to the outer list are not seen since flatten pre-caches outer list on init:
        count = 0;
        for (@SuppressWarnings("unused")
        int x : allInts) {
            count++;
        }
        assertEquals(0, count);

        // but changes to the original inner lists are seen:
        list.get(0).add(0);
        for (@SuppressWarnings("unused")
        int x : allInts) {
            count++;
        }
        assertEquals(1, count);
    }

    public void testIndexOf() {
        final List<String> list = Stream.generate(() -> randomAlphaOfLengthBetween(3, 9))
            .limit(randomIntBetween(10, 30))
            .distinct()
            .toList();
        for (int i = 0; i < list.size(); i++) {
            final String val = list.get(i);
            assertThat(Iterables.indexOf(list, val::equals), is(i));
        }
        assertThat(Iterables.indexOf(list, s -> false), is(-1));
        assertThat(Iterables.indexOf(list, s -> true), is(0));
    }

    private void test(Iterable<String> iterable) {
        try {
            Iterables.get(iterable, -1);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e, hasToString("java.lang.IllegalArgumentException: position >= 0"));
        }
        assertEquals("a", Iterables.get(iterable, 0));
        assertEquals("b", Iterables.get(iterable, 1));
        assertEquals("c", Iterables.get(iterable, 2));
        try {
            Iterables.get(iterable, 3);
            fail("expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            assertThat(e, hasToString("java.lang.IndexOutOfBoundsException: 3"));
        }
    }
}
