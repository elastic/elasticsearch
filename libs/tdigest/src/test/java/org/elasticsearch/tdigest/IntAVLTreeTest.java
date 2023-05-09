/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.tdigest;

import org.junit.Test;

import java.util.*;


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
