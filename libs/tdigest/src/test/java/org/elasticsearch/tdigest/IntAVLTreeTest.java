/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class IntAVLTreeTest extends AbstractTest {

    static class IntBag extends IntAVLTree {

        int value;
        int[] values;
        int[] counts;

        IntBag() {
            values = new int[capacity()];
            counts = new int[capacity()];
        }

        @SuppressWarnings("WeakerAccess")
        public boolean addValue(int value) {
            this.value = value;
            return super.add();
        }

        @SuppressWarnings("WeakerAccess")
        public boolean removeValue(int value) {
            this.value = value;
            final int node = find();
            if (node == NIL) {
                return false;
            } else {
                super.remove(node);
                return true;
            }
        }

        @Override
        protected void resize(int newCapacity) {
            super.resize(newCapacity);
            values = Arrays.copyOf(values, newCapacity);
            counts = Arrays.copyOf(counts, newCapacity);
        }

        @Override
        protected int compare(int node) {
            return value - values[node];
        }

        @Override
        protected void copy(int node) {
            values[node] = value;
            counts[node] = 1;
        }

        @Override
        protected void merge(int node) {
            values[node] = value;
            counts[node]++;
        }

    }

    @Test
    public void dualAdd() {
        Random r = new Random(0);
        TreeMap<Integer, Integer> map = new TreeMap<>();
        IntBag bag = new IntBag();
        for (int i = 0; i < 100000; ++i) {
            final int v = r.nextInt(100000);
            if (map.containsKey(v)) {
                map.put(v, map.get(v) + 1);
                assertFalse(bag.addValue(v));
            } else {
                map.put(v, 1);
                assertTrue(bag.addValue(v));
            }
        }
        Iterator<Map.Entry<Integer, Integer>> it = map.entrySet().iterator();
        for (int node = bag.first(bag.root()); node != IntAVLTree.NIL; node = bag.next(node)) {
            final Map.Entry<Integer, Integer> next = it.next();
            assertEquals(next.getKey().intValue(), bag.values[node]);
            assertEquals(next.getValue().intValue(), bag.counts[node]);
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void dualAddRemove() {
        Random r = new Random(0);
        TreeMap<Integer, Integer> map = new TreeMap<>();
        IntBag bag = new IntBag();
        for (int i = 0; i < 100000; ++i) {
            final int v = r.nextInt(1000);
            if (r.nextBoolean()) {
                // add
                if (map.containsKey(v)) {
                    map.put(v, map.get(v) + 1);
                    assertFalse(bag.addValue(v));
                } else {
                    map.put(v, 1);
                    assertTrue(bag.addValue(v));
                }
            } else {
                // remove
                assertEquals(map.remove(v) != null, bag.removeValue(v));
            }
        }
        Iterator<Map.Entry<Integer, Integer>> it = map.entrySet().iterator();
        for (int node = bag.first(bag.root()); node != IntAVLTree.NIL; node = bag.next(node)) {
            final Map.Entry<Integer, Integer> next = it.next();
            assertEquals(next.getKey().intValue(), bag.values[node]);
            assertEquals(next.getValue().intValue(), bag.counts[node]);
        }
        assertFalse(it.hasNext());
    }

}
