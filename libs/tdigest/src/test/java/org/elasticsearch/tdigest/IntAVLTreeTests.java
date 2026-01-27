/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.elasticsearch.tdigest.arrays.TDigestArrays;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class IntAVLTreeTests extends TDigestTestCase {

    static class IntegerBag extends IntAVLTree {

        int value;
        int[] values;
        int[] counts;

        IntegerBag(TDigestArrays arrays) {
            super(arrays);
            // We adjust the breaker after creation as this is just a test class
            arrays.adjustBreaker(IntAVLTree.SHALLOW_SIZE);
            values = new int[capacity()];
            counts = new int[capacity()];
        }

        public boolean addValue(int value) {
            this.value = value;
            return super.add();
        }

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

    public void testDualAdd() {
        Random r = random();
        TreeMap<Integer, Integer> map = new TreeMap<>();
        try (IntegerBag bag = new IntegerBag(arrays())) {
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
    }

    public void testDualAddRemove() {
        Random r = random();
        TreeMap<Integer, Integer> map = new TreeMap<>();
        try (IntegerBag bag = new IntegerBag(arrays())) {
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
}
