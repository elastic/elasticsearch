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

import org.elasticsearch.tdigest.arrays.TDigestIntArray;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SortTests extends TDigestTestCase {
    public void testReverse() {
        TDigestIntArray x = arrays().newIntArray(0);

        // don't crash with no input
        Sort.reverse(x, 0, x.size());

        // reverse stuff!
        x = arrays().newIntArray(new int[] { 1, 2, 3, 4, 5 });
        Sort.reverse(x, 0, x.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(5 - i, x.get(i));
        }

        // reverse some stuff back
        Sort.reverse(x, 1, 3);
        assertEquals(5, x.get(0));
        assertEquals(2, x.get(1));
        assertEquals(3, x.get(2));
        assertEquals(4, x.get(3));
        assertEquals(1, x.get(4));

        // another no-op
        Sort.reverse(x, 3, 0);
        assertEquals(5, x.get(0));
        assertEquals(2, x.get(1));
        assertEquals(3, x.get(2));
        assertEquals(4, x.get(3));
        assertEquals(1, x.get(4));

        x = arrays().newIntArray(new int[] { 1, 2, 3, 4, 5, 6 });
        Sort.reverse(x, 0, x.size());
        for (int i = 0; i < 6; i++) {
            assertEquals(6 - i, x.get(i));
        }
    }

    public void testEmpty() {
        sort(new int[0], new double[0], 0);
    }

    public void testOne() {
        int[] order = new int[1];
        sort(order, new double[] { 1 }, 1);
        assertEquals(0, order[0]);
    }

    public void testIdentical() {
        int[] order = new int[6];
        double[] values = new double[6];

        sort(order, values, values.length);
        checkOrder(order, values);
    }

    public void testRepeated() {
        int n = 50;
        int[] order = new int[n];
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = Math.rint(10 * ((double) i / n)) / 10.0;
        }

        sort(order, values, values.length);
        checkOrder(order, values);
    }

    public void testStableSort() {
        // this needs to be long enough to force coverage of both quicksort and insertion sort
        // (i.e. >64)
        int n = 70;
        int z = 10;
        int[] order = new int[n];
        double[] values = new double[n];
        double[] weights = new double[n];
        double totalWeight = 0;

        // generate evenly distributed values and weights
        for (int i = 0; i < n; i++) {
            int k = ((i + 5) * 37) % n;
            values[i] = Math.floor(k / (double) z);
            weights[i] = (k % z) + 1;
            totalWeight += weights[i];
        }

        // verify: test weights should be evenly distributed
        double[] tmp = new double[n / z];
        for (int i = 0; i < n; i++) {
            tmp[(int) values[i]] += weights[i];
        }
        for (double v : tmp) {
            assertEquals(totalWeight / tmp.length, v, 0);
        }

        // now sort ...
        sort(order, values, n);

        // and verify stability of the ordering
        // values must be in order and they must appear in their original ordering
        double last = -1;
        for (int j : order) {
            double m = values[j] * n + j;
            assertTrue(m > last);
            last = m;
        }
    }

    public void testShort() {
        int[] order = new int[6];
        double[] values = new double[6];

        // all duplicates
        for (int i = 0; i < 6; i++) {
            values[i] = 1;
        }

        sort(order, values, values.length);
        checkOrder(order, values);

        values[0] = 0.8;
        values[1] = 0.3;

        sort(order, values, values.length);
        checkOrder(order, values);

        values[5] = 1.5;
        values[4] = 1.2;

        sort(order, values, values.length);
        checkOrder(order, values);
    }

    public void testLonger() {
        int[] order = new int[20];
        double[] values = new double[20];
        for (int i = 0; i < 20; i++) {
            values[i] = (i * 13) % 20;
        }
        sort(order, values, values.length);
        checkOrder(order, values);
    }

    public void testMultiPivots() {
        // more pivots than low split on first pass
        // multiple pivots, but more low data on second part of recursion
        int[] order = new int[30];
        double[] values = new double[30];
        for (int i = 0; i < 9; i++) {
            values[i] = i + 20 * (i % 2);
        }

        for (int i = 9; i < 20; i++) {
            values[i] = 10;
        }

        for (int i = 20; i < 30; i++) {
            values[i] = i - 20 * (i % 2);
        }
        values[29] = 29;
        values[24] = 25;
        values[26] = 25;

        sort(order, values, values.length);
        checkOrder(order, values);
    }

    public void testRandomized() {
        Random rand = random();

        for (int k = 0; k < 100; k++) {
            int[] order = new int[30];
            double[] values = new double[30];
            for (int i = 0; i < 30; i++) {
                values[i] = rand.nextDouble();
            }

            sort(order, values, values.length);
            checkOrder(order, values);
        }
    }

    private void checkOrder(int[] order, double[] values) {
        double previous = -Double.MAX_VALUE;
        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
        for (int i = 0; i < values.length; i++) {
            counts.put(i, counts.getOrDefault(i, 0) + 1);
            double v = values[order[i]];
            if (v < previous) {
                throw new IllegalArgumentException("Values out of order");
            }
            previous = v;
        }

        assertEquals(order.length, counts.size());
        for (var entry : counts.entrySet()) {
            assertEquals(1, entry.getValue().intValue());
        }
    }

    private void sort(int[] order, double[] values, int n) {
        var wrappedOrder = arrays().newIntArray(order);
        var wrappedValues = arrays().newDoubleArray(values);

        Sort.stableSort(wrappedOrder, wrappedValues, n);
    }
}
