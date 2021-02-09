/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.test.ESTestCase;

import java.util.NoSuchElementException;

public class PersistentStackTests extends ESTestCase {

    public void testEmptyStack() {
        PersistentStack<String> empty = PersistentStack.empty();
        assertEquals(0, empty.size());
        assertEquals(empty, empty.tail());
        expectThrows(NoSuchElementException.class, empty::head);
        assertFalse(empty.iterator().hasNext());
        expectThrows(NoSuchElementException.class, empty.iterator()::next);
    }

    public void testStack() {
        PersistentStack<String> stack = PersistentStack.<String>empty().push("a").push("b").push("c");
        String[] expected = new String[]{"c", "b", "a"};
        int i = 0;
        for (String actual : stack) {
            assertEquals(expected[i++], actual);
        }

        assertEquals(3, stack.size());
        assertEquals(2, stack.tail().size());
        assertEquals(1, stack.tail().tail().size());
        assertEquals(0, stack.tail().tail().tail().size());

        assertEquals("c", stack.head());
        assertEquals("b", stack.tail().head());
        assertEquals("a", stack.tail().tail().head());
    }

    public void testLongIteration() {
        PersistentStack<Integer> stack = PersistentStack.empty();
        for (int i = 0; i < 1000000; i++) {
            stack = stack.push(i);
        }
        int i = 999999;
        for (Integer integer : stack) {
            assertEquals(i--, (int) integer);
        }
        assertEquals(-1, i);
    }
}
