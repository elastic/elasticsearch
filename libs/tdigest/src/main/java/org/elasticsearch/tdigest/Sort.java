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

import java.util.Arrays;
import java.util.Locale;
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
    @SuppressWarnings("WeakerAccess")
    public static void stableSort(int[] order, double[] values, int n) {
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        stableQuickSort(order, values, 0, n, 64);
        stableInsertionSort(order, values, 0, n, 64);
    }

    /**
     * Two-key quick sort on (values, weights) using an index array
     *
     * @param order   Indexes into values
     * @param values  The values to sort.
     * @param weights The secondary sort key
     * @param n       The number of values to sort
     * @return true if the values were already sorted
     */
    @SuppressWarnings("WeakerAccess")
    public static boolean sort(int[] order, double[] values, double[] weights, int n) {
        if (weights == null) {
            weights = Arrays.copyOf(values, values.length);
        }
        boolean r = sort(order, values, weights, 0, n);
        // now adjust all runs with equal value so that bigger weights are nearer
        // the median
        double medianWeight = 0;
        for (int i = 0; i < n; i++) {
            medianWeight += weights[i];
        }
        medianWeight = medianWeight / 2;
        int i = 0;
        double soFar = 0;
        double nextGroup = 0;
        while (i < n) {
            int j = i;
            while (j < n && values[order[j]] == values[order[i]]) {
                double w = weights[order[j]];
                nextGroup += w;
                j++;
            }
            if (j > i + 1) {
                if (soFar >= medianWeight) {
                    // entire group is in last half, reverse the order
                    reverse(order, i, j - i);
                } else if (nextGroup > medianWeight) {
                    // group straddles the median, but not necessarily evenly
                    // most elements are probably unit weight if there are many
                    double[] scratch = new double[j - i];

                    double netAfter = nextGroup + soFar - 2 * medianWeight;
                    // heuristically adjust weights to roughly balance around median
                    double max = weights[order[j - 1]];
                    for (int k = j - i - 1; k >= 0; k--) {
                        double weight = weights[order[i + k]];
                        if (netAfter < 0) {
                            // sort in normal order
                            scratch[k] = weight;
                            netAfter += weight;
                        } else {
                            // sort reversed, but after normal items
                            scratch[k] = 2 * max + 1 - weight;
                            netAfter -= weight;
                        }
                    }
                    // sort these balanced weights
                    int[] sub = new int[j - i];
                    sort(sub, scratch, scratch, 0, j - i);
                    int[] tmp = Arrays.copyOfRange(order, i, j);
                    for (int k = 0; k < j - i; k++) {
                        order[i + k] = tmp[sub[k]];
                    }
                }
            }
            soFar = nextGroup;
            i = j;
        }
        return r;
    }

    /**
     * Two-key quick sort on (values, weights) using an index array
     *
     * @param order   Indexes into values
     * @param values  The values to sort
     * @param weights The weights that define the secondary ordering
     * @param start   The first element to sort
     * @param n       The number of values to sort
     * @return True if the values were in order without sorting
     */
    @SuppressWarnings("WeakerAccess")
    private static boolean sort(int[] order, double[] values, double[] weights, int start, int n) {
        boolean inOrder = true;
        for (int i = start; i < start + n; i++) {
            if (inOrder && i < start + n - 1) {
                inOrder = values[i] < values[i + 1] || (values[i] == values[i + 1] && weights[i] <= weights[i + 1]);
            }
            order[i] = i;
        }
        if (inOrder) {
            return true;
        }
        quickSort(order, values, weights, start, start + n, 64);
        insertionSort(order, values, weights, start, start + n, 64);
        return false;
    }

    /**
     * Standard two-key quick sort on (values, weights) except that sorting is done on an index array
     * rather than the values themselves
     *
     * @param order   The pre-allocated index array
     * @param values  The values to sort
     * @param weights The weights (secondary key)
     * @param start   The beginning of the values to sort
     * @param end     The value after the last value to sort
     * @param limit   The minimum size to recurse down to.
     */
    private static void quickSort(int[] order, double[] values, double[] weights, int start, int end, int limit) {
        // the while loop implements tail-recursion to avoid excessive stack calls on nasty cases
        while (end - start > limit) {

            // pivot by a random element
            int pivotIndex = start + prng.nextInt(end - start);
            double pivotValue = values[order[pivotIndex]];
            double pivotWeight = weights[order[pivotIndex]];

            // move pivot to beginning of array
            swap(order, start, pivotIndex);

            // we use a three way partition because many duplicate values is an important case

            int low = start + 1;   // low points to first value not known to be equal to pivotValue
            int high = end;        // high points to first value > pivotValue
            int i = low;           // i scans the array
            while (i < high) {
                // invariant: (values,weights)[order[k]] == (pivotValue, pivotWeight) for k in [0..low)
                // invariant: (values,weights)[order[k]] < (pivotValue, pivotWeight) for k in [low..i)
                // invariant: (values,weights)[order[k]] > (pivotValue, pivotWeight) for k in [high..end)
                // in-loop: i < high
                // in-loop: low < high
                // in-loop: i >= low
                double vi = values[order[i]];
                double wi = weights[order[i]];
                if (vi == pivotValue && wi == pivotWeight) {
                    if (low != i) {
                        swap(order, low, i);
                    } else {
                        i++;
                    }
                    low++;
                } else if (vi > pivotValue || (vi == pivotValue && wi > pivotWeight)) {
                    high--;
                    swap(order, i, high);
                } else {
                    // vi < pivotValue || (vi == pivotValue && wi < pivotWeight)
                    i++;
                }
            }
            // invariant: (values,weights)[order[k]] == (pivotValue, pivotWeight) for k in [0..low)
            // invariant: (values,weights)[order[k]] < (pivotValue, pivotWeight) for k in [low..i)
            // invariant: (values,weights)[order[k]] > (pivotValue, pivotWeight) for k in [high..end)
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
                quickSort(order, values, weights, start, low, limit);

                // this is really a way to do
                // quickSort(order, values, high, end, limit);
                start = high;
            } else {
                quickSort(order, values, weights, high, end, limit);
                // this is really a way to do
                // quickSort(order, values, start, low, limit);
                end = low;
            }
        }
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
    private static void stableQuickSort(int[] order, double[] values, int start, int end, int limit) {
        // the while loop implements tail-recursion to avoid excessive stack calls on nasty cases
        while (end - start > limit) {

            // pivot by a random element
            int pivotIndex = start + prng.nextInt(end - start);
            double pivotValue = values[order[pivotIndex]];
            int pv = order[pivotIndex];

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
                double vi = values[order[i]];
                int pi = order[i];
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

    /**
     * Quick sort in place of several paired arrays.  On return,
     * keys[...] is in order and the values[] arrays will be
     * reordered as well in the same way.
     *
     * @param key    Values to sort on
     * @param values The auxiliary values to sort.
     */
    @SuppressWarnings("WeakerAccess")
    public static void sort(double[] key, double[]... values) {
        sort(key, 0, key.length, values);
    }

    /**
     * Quick sort using an index array.  On return,
     * values[order[i]] is in order as i goes start..n
     *  @param key    Values to sort on
     * @param start  The first element to sort
     * @param n      The number of values to sort
     * @param values The auxiliary values to sort.
     */
    @SuppressWarnings("WeakerAccess")
    public static void sort(double[] key, int start, int n, double[]... values) {
        quickSort(key, values, start, start + n, 8);
        insertionSort(key, values, start, start + n, 8);
    }

    /**
     * Standard quick sort except that sorting rearranges parallel arrays
     *
     * @param key    Values to sort on
     * @param values The auxiliary values to sort.
     * @param start  The beginning of the values to sort
     * @param end    The value after the last value to sort
     * @param limit  The minimum size to recurse down to.
     */
    private static void quickSort(double[] key, double[][] values, int start, int end, int limit) {
        // the while loop implements tail-recursion to avoid excessive stack calls on nasty cases
        while (end - start > limit) {

            // median of three values for the pivot
            int a = start;
            int b = (start + end) / 2;
            int c = end - 1;

            int pivotIndex;
            double pivotValue;
            double va = key[a];
            double vb = key[b];
            double vc = key[c];

            if (va > vb) {
                if (vc > va) {
                    // vc > va > vb
                    pivotIndex = a;
                    pivotValue = va;
                } else {
                    // va > vb, va >= vc
                    if (vc < vb) {
                        // va > vb > vc
                        pivotIndex = b;
                        pivotValue = vb;
                    } else {
                        // va >= vc >= vb
                        pivotIndex = c;
                        pivotValue = vc;
                    }
                }
            } else {
                // vb >= va
                if (vc > vb) {
                    // vc > vb >= va
                    pivotIndex = b;
                    pivotValue = vb;
                } else {
                    // vb >= va, vb >= vc
                    if (vc < va) {
                        // vb >= va > vc
                        pivotIndex = a;
                        pivotValue = va;
                    } else {
                        // vb >= vc >= va
                        pivotIndex = c;
                        pivotValue = vc;
                    }
                }
            }

            // move pivot to beginning of array
            swap(start, pivotIndex, key, values);

            // we use a three way partition because many duplicate values is an important case

            int low = start + 1;   // low points to first value not known to be equal to pivotValue
            int high = end;        // high points to first value > pivotValue
            int i = low;           // i scans the array
            while (i < high) {
                // invariant: values[order[k]] == pivotValue for k in [0..low)
                // invariant: values[order[k]] < pivotValue for k in [low..i)
                // invariant: values[order[k]] > pivotValue for k in [high..end)
                // in-loop: i < high
                // in-loop: low < high
                // in-loop: i >= low
                double vi = key[i];
                if (vi == pivotValue) {
                    if (low != i) {
                        swap(low, i, key, values);
                    } else {
                        i++;
                    }
                    low++;
                } else if (vi > pivotValue) {
                    high--;
                    swap(i, high, key, values);
                } else {
                    // vi < pivotValue
                    i++;
                }
            }
            // invariant: values[order[k]] == pivotValue for k in [0..low)
            // invariant: values[order[k]] < pivotValue for k in [low..i)
            // invariant: values[order[k]] > pivotValue for k in [high..end)
            // assert i == high || low == high therefore, we are done with partition

            // at this point, i==high, from [start,low) are == pivot, [low,high) are < and [high,end) are >
            // we have to move the values equal to the pivot into the middle. To do this, we swap pivot
            // values into the top end of the [low,high) range stopping when we run out of destinations
            // or when we run out of values to copy
            int from = start;
            int to = high - 1;
            for (i = 0; from < low && to >= low; i++) {
                swap(from++, to--, key, values);
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
            if (low - start < end - high) {
                quickSort(key, values, start, low, limit);

                // this is really a way to do
                // quickSort(order, values, high, end, limit);
                start = high;
            } else {
                quickSort(key, values, high, end, limit);
                // this is really a way to do
                // quickSort(order, values, start, low, limit);
                end = low;
            }
        }
    }

    /**
     * Limited range insertion sort.  We assume that no element has to move more than limit steps
     * because quick sort has done its thing. This version works on parallel arrays of keys and values.
     *
     * @param key    The array of keys
     * @param values The values we are sorting
     * @param start  The starting point of the sort
     * @param end    The ending point of the sort
     * @param limit  The largest amount of disorder
     */
    @SuppressWarnings("SameParameterValue")
    private static void insertionSort(double[] key, double[][] values, int start, int end, int limit) {
        // loop invariant: all values start ... i-1 are ordered
        for (int i = start + 1; i < end; i++) {
            double v = key[i];
            int m = Math.max(i - limit, start);
            for (int j = i; j >= m; j--) {
                if (j == m || key[j - 1] <= v) {
                    if (j < i) {
                        System.arraycopy(key, j, key, j + 1, i - j);
                        key[j] = v;
                        for (double[] value : values) {
                            double tmp = value[i];
                            System.arraycopy(value, j, value, j + 1, i - j);
                            value[j] = tmp;
                        }
                    }
                    break;
                }
            }
        }
    }

    private static void swap(int[] order, int i, int j) {
        int t = order[i];
        order[i] = order[j];
        order[j] = t;
    }

    private static void swap(int i, int j, double[] key, double[]... values) {
        double t = key[i];
        key[i] = key[j];
        key[j] = t;

        for (int k = 0; k < values.length; k++) {
            t = values[k][i];
            values[k][i] = values[k][j];
            values[k][j] = t;
        }
    }

    /**
     * Check that a partition step was done correctly.  For debugging and testing.
     *
     * @param order      The array of indexes representing a permutation of the keys.
     * @param values     The keys to sort.
     * @param pivotValue The value that splits the data
     * @param start      The beginning of the data of interest.
     * @param low        Values from start (inclusive) to low (exclusive) are &lt; pivotValue.
     * @param high       Values from low to high are equal to the pivot.
     * @param end        Values from high to end are above the pivot.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static void checkPartition(int[] order, double[] values, double pivotValue, int start, int low, int high, int end) {
        if (order.length != values.length) {
            throw new IllegalArgumentException("Arguments must be same size");
        }

        if ((start >= 0 && low >= start && high >= low && end >= high) == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid indices %d, %d, %d, %d", start, low, high, end));
        }

        for (int i = 0; i < low; i++) {
            double v = values[order[i]];
            if (v >= pivotValue) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Value greater than pivot at %d", i));
            }
        }

        for (int i = low; i < high; i++) {
            if (values[order[i]] != pivotValue) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Non-pivot at %d", i));
            }
        }

        for (int i = high; i < end; i++) {
            double v = values[order[i]];
            if (v <= pivotValue) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Value less than pivot at %d", i));
            }
        }
    }

    /**
     * Limited range insertion sort with primary and secondary key.  We assume that no
     * element has to move more than limit steps because quick sort has done its thing.
     *
     * If weights (the secondary key) is null, then only the primary key is used.
     *
     * This sort is inherently stable.
     *
     * @param order   The permutation index
     * @param values  The values we are sorting
     * @param weights The secondary key for sorting
     * @param start   Where to start the sort
     * @param n       How many elements to sort
     * @param limit   The largest amount of disorder
     */
    @SuppressWarnings("SameParameterValue")
    private static void insertionSort(int[] order, double[] values, double[] weights, int start, int n, int limit) {
        for (int i = start + 1; i < n; i++) {
            int t = order[i];
            double v = values[order[i]];
            double w = weights == null ? 0 : weights[order[i]];
            int m = Math.max(i - limit, start);
            // values in [start, i) are ordered
            // scan backwards to find where to stick t
            for (int j = i; j >= m; j--) {
                if (j == 0 || values[order[j - 1]] < v || (values[order[j - 1]] == v && (weights == null || weights[order[j - 1]] <= w))) {
                    if (j < i) {
                        System.arraycopy(order, j, order, j + 1, i - j);
                        order[j] = t;
                    }
                    break;
                }
            }
        }
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
    @SuppressWarnings("SameParameterValue")
    private static void stableInsertionSort(int[] order, double[] values, int start, int n, int limit) {
        for (int i = start + 1; i < n; i++) {
            int t = order[i];
            double v = values[order[i]];
            int vi = order[i];
            int m = Math.max(i - limit, start);
            // values in [start, i) are ordered
            // scan backwards to find where to stick t
            for (int j = i; j >= m; j--) {
                if (j == 0 || values[order[j - 1]] < v || (values[order[j - 1]] == v && (order[j - 1] <= vi))) {
                    if (j < i) {
                        System.arraycopy(order, j, order, j + 1, i - j);
                        order[j] = t;
                    }
                    break;
                }
            }
        }
    }

    /**
     * Reverses an array in-place.
     *
     * @param order The array to reverse
     */
    @SuppressWarnings("WeakerAccess")
    public static void reverse(int[] order) {
        reverse(order, 0, order.length);
    }

    /**
     * Reverses part of an array. See {@link #reverse(int[])}
     *
     * @param order  The array containing the data to reverse.
     * @param offset Where to start reversing.
     * @param length How many elements to reverse
     */
    @SuppressWarnings("WeakerAccess")
    public static void reverse(int[] order, int offset, int length) {
        for (int i = 0; i < length / 2; i++) {
            int t = order[offset + i];
            order[offset + i] = order[offset + length - i - 1];
            order[offset + length - i - 1] = t;
        }
    }

    /**
     * Reverses part of an array. See {@link #reverse(int[])}
     *
     * @param order  The array containing the data to reverse.
     * @param offset Where to start reversing.
     * @param length How many elements to reverse
     */
    @SuppressWarnings({ "WeakerAccess", "SameParameterValue" })
    public static void reverse(double[] order, int offset, int length) {
        for (int i = 0; i < length / 2; i++) {
            double t = order[offset + i];
            order[offset + i] = order[offset + length - i - 1];
            order[offset + length - i - 1] = t;
        }
    }
}
