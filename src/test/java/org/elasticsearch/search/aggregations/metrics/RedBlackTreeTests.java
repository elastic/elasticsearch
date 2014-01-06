/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.RedBlackTree;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RedBlackTreeTests extends ElasticsearchTestCase {

    private static class IntRedBlackTree extends RedBlackTree {

        private int tmpValue;
        private int[] values;
        private int[] counts;
        
        IntRedBlackTree() {
            super(10);
            values = new int[5];
            counts = new int[5];
        }
        
        @Override
        protected int compare(int node) {
            return Ints.compare(tmpValue, values[node]);
        }

        @Override
        protected void merge(int node) {
            counts[node] += 1;
        }

        @Override
        protected void copy(int node) {
            values[node] = tmpValue;
            counts[node] = 1;
        }

        @Override
        protected int newNode() {
            final int newNode = super.newNode();
            if (values.length <= newNode) {
                values = ArrayUtil.grow(values, newNode + 1);
                counts = Arrays.copyOf(counts, values.length);
            }
            return newNode;
        }

        public boolean add(int value) {
            tmpValue = value;
            return super.addNode();
        }

        public boolean remove(int value) {
            tmpValue = value;
            final int nodeToRemove = getNode();
            if (nodeToRemove == NIL) {
                return false;
            } else {
                super.removeNode(nodeToRemove);
                return true;
            }
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append("{ ");
            for (IntCursor cursor : this) {
                b.append(values[cursor.value] + "(" + counts[cursor.value] + "), ");
            }
            b.setLength(b.length() - 2);
            b.append(" } ");
            return b.toString();
        }
        
    }

    public void testAdd() {
        Map<Integer, Integer> map = Maps.newHashMap();
        IntRedBlackTree tree = new IntRedBlackTree();
        final int iters = atLeast(1000);
        for (int i = 0; i < iters; ++i) {
            final int value = randomInt(200);
            final boolean added = tree.add(value);
            tree.assertConsistent();
            assertEquals(!map.containsKey(value), added);
            if (map.containsKey(value)) {
                map.put(value, map.get(value) + 1);
            } else {
                map.put(value, 1);
            }
            assertEquals(map.size(), tree.size());
        }

        int size = 0;
        int previousValue = Integer.MIN_VALUE;
        for (IntCursor cursor : tree) {
            ++size;
            final int value = tree.values[cursor.value];
            assertTrue(previousValue < value);
            assertEquals(map.get(value).intValue(), tree.counts[cursor.value]);
            previousValue = value;
        }
        assertEquals(map.size(), size);
    }

    public void testRemove() {
        final int numValues = atLeast(200);
        final FixedBitSet values = new FixedBitSet(numValues);
        values.set(0, numValues);
        IntRedBlackTree tree = new IntRedBlackTree();
        for (int i = 0; i < numValues; ++i) {
            tree.add(i);
        }
        
        final int iters = atLeast(300);
        for (int i = 0; i < iters; ++i) {
            final int value = randomInt(numValues - 1);
            final boolean removed = tree.remove(value);
            assertEquals(removed, values.get(value));
            values.clear(value);
            assertEquals(values.cardinality(), tree.size());
            tree.assertConsistent();
        }

        int size = 0;
        int previousValue = Integer.MIN_VALUE;
        for (IntCursor cursor : tree) {
            ++size;
            final int value = tree.values[cursor.value];
            assertTrue(previousValue < value);
            assertTrue(values.get(value));
            previousValue = value;
        }
        assertEquals(values.cardinality(), size);
    }

    public void testReverse() {
        IntRedBlackTree tree = new IntRedBlackTree();
        final int iters = atLeast(1000);
        for (int i = 0; i < iters; ++i) {
            final int value = randomInt(2000);
            tree.add(value);
        }
        List<Integer> sortedNodes = Lists.newArrayList();
        for (IntCursor cursor : tree) {
            sortedNodes.add(cursor.value);
        }
        List<Integer> reverseNodes = Lists.newArrayList();
        for (IntCursor cursor : tree.reverseSet()) {
            reverseNodes.add(cursor.value);
        }
        Collections.reverse(sortedNodes);
        assertEquals(sortedNodes, reverseNodes);
    }
    
}
