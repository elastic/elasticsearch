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

import org.apache.lucene.search.Multiset;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SortTest {
    @Test
    public void testReverse() {
        int[] x = new int[0];

        // don't crash with no input
        Sort.reverse(x);

        // reverse stuff!
        x = new int[]{1, 2, 3, 4, 5};
        Sort.reverse(x);
        for (int i = 0; i < 5; i++) {
            assertEquals(5 - i, x[i]);
        }

        // reverse some stuff back
        Sort.reverse(x, 1, 3);
        assertEquals(5, x[0]);
        assertEquals(2, x[1]);
        assertEquals(3, x[2]);
        assertEquals(4, x[3]);
        assertEquals(1, x[4]);

        // another no-op
        Sort.reverse(x, 3, 0);
        assertEquals(5, x[0]);
        assertEquals(2, x[1]);
        assertEquals(3, x[2]);
        assertEquals(4, x[3]);
        assertEquals(1, x[4]);

        x = new int[]{1, 2, 3, 4, 5, 6};
        Sort.reverse(x);
        for (int i = 0; i < 6; i++) {
            assertEquals(6 - i, x[i]);
        }
    }

    @Test
    public void testEmpty() {
        Sort.sort(new int[]{}, new double[]{}, null, 0);
    }

    @Test
    public void testOne() {
        int[] order = new int[1];
        Sort.sort(order, new double[]{1}, new double[]{1}, 1);
        assertEquals(0, order[0]);
    }

    @Test
    public void testIdentical() {
        int[] order = new int[6];
        double[] values = new double[6];

        Sort.sort(order, values, null, values.length);
        checkOrder(order, values);
    }

    @Test
    public void testRepeated() {
        int n = 50;
        int[] order = new int[n];
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = Math.rint(10 * ((double) i / n)) / 10.0;
        }

        Sort.sort(order, values, null, values.length);
        checkOrder(order, values);
    }

    @Test
    public void testRepeatedSortByWeight() {
        // this needs to be long enough to force coverage of both quicksort and insertion sort
        // (i.e. >64)
        int n = 125;
        int[] order = new int[n];
        double[] values = new double[n];
        double[] weights = new double[n];
        double totalWeight = 0;

        // generate evenly distributed values and weights
        for (int i = 0; i < n; i++) {
            int k = ((i + 5) * 37) % n;
            values[i] = Math.floor(k / 25.0);
            weights[i] = (k % 25) + 1;
            totalWeight += weights[i];
        }

        // verify: test weights should be evenly distributed
        double[] tmp = new double[5];
        for (int i = 0; i < n; i++) {
            tmp[(int) values[i]] += weights[i];
        }
        for (double v : tmp) {
            assertEquals(totalWeight / tmp.length, v, 0);
        }

        // now sort ...
        Sort.sort(order, values, weights, n);

        // and verify our somewhat unusual ordering of the result
        // within the first two quintiles, value is constant, weights increase within each quintile
        int delta = order.length / 5;
        double sum = checkSubOrder(0.0, order, values, weights, 0, delta, 1);
        assertEquals(totalWeight * 0.2, sum, 0);
        sum = checkSubOrder(sum, order, values, weights, delta, 2 * delta, 1);
        assertEquals(totalWeight * 0.4, sum, 0);

        // in the middle quintile, weights go up and then down after the median
        sum = checkMidOrder(totalWeight / 2, sum, order, values, weights, 2 * delta, 3 * delta);
        assertEquals(totalWeight * 0.6, sum, 0);

        // in the last two quintiles, weights decrease
        sum = checkSubOrder(sum, order, values, weights, 3 * delta, 4 * delta, -1);
        assertEquals(totalWeight * 0.8, sum, 0);
        sum = checkSubOrder(sum, order, values, weights, 4 * delta, 5 * delta, -1);
        assertEquals(totalWeight, sum, 0);
    }

    @Test
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
        double[] tmp = new double[n/z];
        for (int i = 0; i < n; i++) {
            tmp[(int) values[i]] += weights[i];
        }
        for (double v : tmp) {
            assertEquals(totalWeight / tmp.length, v, 0);
        }

        // now sort ...
        Sort.stableSort(order, values, n);

        // and verify stability of the ordering
        // values must be in order and they must appear in their original ordering
        double last = -1;
        for (int j : order) {
            double m = values[j] * n + j;
            assertTrue(m > last);
            last = m;
        }
    }

    private double checkMidOrder(double medianWeight, double sofar, int[] order, double[] values, double[] weights, int start, int end) {
        double value = values[order[start]];
        double last = 0;
        assertTrue(sofar < medianWeight);
        for (int i = start; i < end; i++) {
            assertEquals(value, values[order[i]], 0);
            double w = weights[order[i]];
            assertTrue(w > 0);
            if (sofar > medianWeight) {
                w = 2 * medianWeight - w;
            }
            assertTrue(w >= last);
            sofar += weights[order[i]];
        }
        assertTrue(sofar > medianWeight);
        return sofar;
    }

    private double checkSubOrder(double sofar, int[] order, double[] values, double[] weights, int start, int end, int ordering) {
        double lastWeight = weights[order[start]] * ordering;
        double value = values[order[start]];
        for (int i = start; i < end; i++) {
            assertEquals(value, values[order[i]], 0);
            double newOrderedWeight = weights[order[i]] * ordering;
            assertTrue(newOrderedWeight >= lastWeight);
            lastWeight = newOrderedWeight;
            sofar += weights[order[i]];
        }
        return sofar;
    }

    @Test
    public void testShort() {
        int[] order = new int[6];
        double[] values = new double[6];

        // all duplicates
        for (int i = 0; i < 6; i++) {
            values[i] = 1;
        }

        Sort.sort(order, values, null, values.length);
        checkOrder(order, values);

        values[0] = 0.8;
        values[1] = 0.3;

        Sort.sort(order, values, null, values.length);
        checkOrder(order, values);

        values[5] = 1.5;
        values[4] = 1.2;

        Sort.sort(order, values, null, values.length);
        checkOrder(order, values);
    }

    @Test
    public void testLonger() {
        int[] order = new int[20];
        double[] values = new double[20];
        for (int i = 0; i < 20; i++) {
            values[i] = (i * 13) % 20;
        }
        Sort.sort(order, values, null, values.length);
        checkOrder(order, values);
    }

    @Test
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

        Sort.sort(order, values, null, values.length);
        checkOrder(order, values);
    }

    @Test
    public void testMultiPivotsInPlace() {
        // more pivots than low split on first pass
        // multiple pivots, but more low data on second part of recursion
        double[] keys = new double[30];
        for (int i = 0; i < 9; i++) {
            keys[i] = i + 20 * (i % 2);
        }

        for (int i = 9; i < 20; i++) {
            keys[i] = 10;
        }

        for (int i = 20; i < 30; i++) {
            keys[i] = i - 20 * (i % 2);
        }
        keys[29] = 29;
        keys[24] = 25;
        keys[26] = 25;

        double[] v = valuesFromKeys(keys, 0);

        Sort.sort(keys, v);
        checkOrder(keys, 0, keys.length, v);
    }

    @Test
    public void testRandomized() {
        Random rand = new Random();

        for (int k = 0; k < 100; k++) {
            int[] order = new int[30];
            double[] values = new double[30];
            for (int i = 0; i < 30; i++) {
                values[i] = rand.nextDouble();
            }

            Sort.sort(order, values, null, values.length);
            checkOrder(order, values);
        }
    }

    @Test
    public void testRandomizedShortSort() {
        Random rand = new Random();

        for (int k = 0; k < 100; k++) {
            double[] keys = new double[30];
            for (int i = 0; i < 10; i++) {
                keys[i] = i;
            }
            for (int i = 10; i < 20; i++) {
                keys[i] = rand.nextDouble();
            }
            for (int i = 20; i < 30; i++) {
                keys[i] = i;
            }
            double[] v0 = valuesFromKeys(keys, 0);
            double[] v1 = valuesFromKeys(keys, 1);

            Sort.sort(keys, 10, 10, v0, v1);
            checkOrder(keys, 10, 10, v0, v1);
            checkValues(keys, 0, keys.length, v0, v1);
            for (int i = 0; i < 10; i++) {
                assertEquals(i, keys[i], 0);
            }
            for (int i = 20; i < 30; i++) {
                assertEquals(i, keys[i], 0);
            }
        }
    }

    /**
     * Generates a vector of values corresponding to a vector of keys.
     *
     * @param keys A vector of keys
     * @param k    Which value vector to generate
     * @return The new vector containing frac(key_i * 3 * 5^k)
     */
    private double[] valuesFromKeys(double[] keys, int k) {
        double[] r = new double[keys.length];
        double scale = 3;
        for (int i = 0; i < k; i++) {
            scale = scale * 5;
        }
        for (int i = 0; i < keys.length; i++) {
            r[i] = fractionalPart(keys[i] * scale);
        }
        return r;
    }

    /**
     * Verifies that keys are in order and that each value corresponds to the keys
     *
     * @param key    Array of keys
     * @param start  The starting offset of keys and values to check
     * @param length The number of keys and values to check
     * @param values Arrays of associated values. Value_{ki} = frac(key_i * 3 * 5^k)
     */
    private void checkOrder(double[] key, int start, int length, double[]... values) {
        assert start + length <= key.length;

        for (int i = start; i < start + length - 1; i++) {
            assertTrue(String.format("bad ordering at %d, %f > %f", i, key[i], key[i + 1]), key[i] <= key[i + 1]);
        }

        checkValues(key, start, length, values);
    }

    private void checkValues(double[] key, int start, int length, double[]... values) {
        double scale = 3;
        for (int k = 0; k < values.length; k++) {
            double[] v = values[k];
            assertEquals(key.length, v.length);
            for (int i = start; i < length; i++) {
                assertEquals(String.format("value %d not correlated, key=%.5f, k=%d, v=%.5f", i, key[i], k, values[k][i]),
                        fractionalPart(key[i] * scale), values[k][i], 0);
            }
            scale = scale * 5;
        }
    }


    private double fractionalPart(double v) {
        return v - Math.floor(v);
    }

    private void checkOrder(int[] order, double[] values) {
        double previous = -Double.MAX_VALUE;
        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
        for (int i = 0; i < values.length; i++) {
            counts.put(i, counts.getOrDefault(i, 0) + 1);
            double v = values[order[i]];
            if (v < previous) {
                throw new IllegalArgumentException("Values out of order at %d");
            }
            previous = v;
        }

        assertEquals(order.length, counts.size());
        for (var entry : counts.entrySet()) {
            assertEquals(1, entry.getValue().intValue());
        }
    }
}
