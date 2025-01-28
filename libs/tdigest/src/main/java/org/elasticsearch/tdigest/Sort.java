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

import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;

import java.util.Random;

/**
 * Static sorting methods
 */
public class Sort {
    private static final Random prng = new Random(); // for choosing pivots during quicksort

    /**
     * Single-key stabilized quick sort on using an index array
     *
     * @param order   Indexes into values
     * @param values  The values to sort.
     * @param n       The number of values to sort
     */
    public static void stableSort(TDigestIntArray order, TDigestDoubleArray values, int n) {
        for (int i = 0; i < n; i++) {
            order.set(i, i);
        }
        stableQuickSort(order, values, 0, n, 64);
        stableInsertionSort(order, values, 0, n, 64);
    }

    /**
     * Stabilized quick sort on an index array. This is a normal quick sort that uses the
     * original index as a secondary key. Since we are really just sorting an index array
     * we can do this nearly for free.
     *
     * @param order  The pre-allocated index array
     * @param values The values to sort
     * @param start  The beginning of the values to sort
     * @param end    The value after the last value to sort
     * @param limit  The minimum size to recurse down to.
     */
    private static void stableQuickSort(TDigestIntArray order, TDigestDoubleArray values, int start, int end, int limit) {
        // the while loop implements tail-recursion to avoid excessive stack calls on nasty cases
        while (end - start > limit) {

            // pivot by a random element
            int pivotIndex = start + prng.nextInt(end - start);
            double pivotValue = values.get(order.get(pivotIndex));
            int pv = order.get(pivotIndex);

            // move pivot to beginning of array
            swap(order, start, pivotIndex);

            // we use a three way partition because many duplicate values is an important case

            int low = start + 1;   // low points to first value not known to be equal to pivotValue
            int high = end;        // high points to first value > pivotValue
            int i = low;           // i scans the array
            while (i < high) {
                // invariant: (values[order[k]],order[k]) == (pivotValue, pv) for k in [0..low)
                // invariant: (values[order[k]],order[k]) < (pivotValue, pv) for k in [low..i)
                // invariant: (values[order[k]],order[k]) > (pivotValue, pv) for k in [high..end)
                // in-loop: i < high
                // in-loop: low < high
                // in-loop: i >= low
                double vi = values.get(order.get(i));
                int pi = order.get(i);
                if (vi == pivotValue && pi == pv) {
                    if (low != i) {
                        swap(order, low, i);
                    } else {
                        i++;
                    }
                    low++;
                } else if (vi > pivotValue || (vi == pivotValue && pi > pv)) {
                    high--;
                    swap(order, i, high);
                } else {
                    // vi < pivotValue || (vi == pivotValue && pi < pv)
                    i++;
                }
            }
            // invariant: (values[order[k]],order[k]) == (pivotValue, pv) for k in [0..low)
            // invariant: (values[order[k]],order[k]) < (pivotValue, pv) for k in [low..i)
            // invariant: (values[order[k]],order[k]) > (pivotValue, pv) for k in [high..end)
            // assert i == high || low == high therefore, we are done with partition

            // at this point, i==high, from [start,low) are == pivot, [low,high) are < and [high,end) are >
            // we have to move the values equal to the pivot into the middle. To do this, we swap pivot
            // values into the top end of the [low,high) range stopping when we run out of destinations
            // or when we run out of values to copy
            int from = start;
            int to = high - 1;
            for (i = 0; from < low && to >= low; i++) {
                swap(order, from++, to--);
            }
            if (from == low) {
                // ran out of things to copy. This means that the last destination is the boundary
                low = to + 1;
            } else {
                // ran out of places to copy to. This means that there are uncopied pivots and the
                // boundary is at the beginning of those
                low = from;
            }

            // checkPartition(order, values, pivotValue, start, low, high, end);

            // now recurse, but arrange it so we handle the longer limit by tail recursion
            // we have to sort the pivot values because they may have different weights
            // we can't do that, however until we know how much weight is in the left and right
            if (low - start < end - high) {
                // left side is smaller
                stableQuickSort(order, values, start, low, limit);

                // this is really a way to do
                // quickSort(order, values, high, end, limit);
                start = high;
            } else {
                stableQuickSort(order, values, high, end, limit);
                // this is really a way to do
                // quickSort(order, values, start, low, limit);
                end = low;
            }
        }
    }

    private static void swap(TDigestIntArray order, int i, int j) {
        int t = order.get(i);
        order.set(i, order.get(j));
        order.set(j, t);
    }

    /**
     * Limited range insertion sort with primary key stabilized by the use of the
     * original position to break ties.  We assume that no element has to move more
     * than limit steps because quick sort has done its thing.
     *
     * @param order   The permutation index
     * @param values  The values we are sorting
     * @param start   Where to start the sort
     * @param n       How many elements to sort
     * @param limit   The largest amount of disorder
     */
    private static void stableInsertionSort(TDigestIntArray order, TDigestDoubleArray values, int start, int n, int limit) {
        for (int i = start + 1; i < n; i++) {
            int t = order.get(i);
            double v = values.get(order.get(i));
            int vi = order.get(i);
            int m = Math.max(i - limit, start);
            // values in [start, i) are ordered
            // scan backwards to find where to stick t
            for (int j = i; j >= m; j--) {
                if (j == 0 || values.get(order.get(j - 1)) < v || (values.get(order.get(j - 1)) == v && (order.get(j - 1) <= vi))) {
                    if (j < i) {
                        order.set(j + 1, order, j, i - j);
                        order.set(j, t);
                    }
                    break;
                }
            }
        }
    }

    /**
     * Reverses part of an array.
     *
     * @param order  The array containing the data to reverse.
     * @param offset Where to start reversing.
     * @param length How many elements to reverse
     */
    public static void reverse(TDigestIntArray order, int offset, int length) {
        for (int i = 0; i < length / 2; i++) {
            int t = order.get(offset + i);
            order.set(offset + i, order.get(offset + length - i - 1));
            order.set(offset + length - i - 1, t);
        }
    }

    /**
     * Reverses part of an array.
     *
     * @param order  The array containing the data to reverse.
     * @param offset Where to start reversing.
     * @param length How many elements to reverse
     */
    public static void reverse(TDigestDoubleArray order, int offset, int length) {
        for (int i = 0; i < length / 2; i++) {
            double t = order.get(offset + i);
            order.set(offset + i, order.get(offset + length - i - 1));
            order.set(offset + length - i - 1, t);
        }
    }
}
