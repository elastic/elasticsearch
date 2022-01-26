/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

/**
 * Performs binary search on an arbitrary data structure.
 *
 * To do a search, create a subclass and implement custom {@link #compare(int)} and {@link #distance(int)} methods.
 *
 * {@link BinarySearcher} knows nothing about the value being searched for or the underlying data structure.
 * These things should be determined by the subclass in its overridden methods.
 *
 * Refer to {@link BigArrays.DoubleBinarySearcher} for an example.
 *
 * NOTE: this class is not thread safe
 */
public abstract class BinarySearcher {

    /**
     * @return a negative integer, zero, or a positive integer if the array's value at <code>index</code> is less than,
     * equal to, or greater than the value being searched for.
     */
    protected abstract int compare(int index);

    /**
     * @return the magnitude of the distance between the element at <code>index</code> and the value being searched for.
     * It will usually be <code>Math.abs(array[index] - searchValue)</code>.
     */
    protected abstract double distance(int index);

    /**
     * @return the index who's underlying value is closest to the value being searched for.
     */
    private int getClosestIndex(int index1, int index2) {
        if (distance(index1) < distance(index2)) {
            return index1;
        } else {
            return index2;
        }
    }

    /**
     * Uses a binary search to determine the index of the element within the index range {from, ... , to} that is
     * closest to the search value.
     *
     * Unlike most binary search implementations, the value being searched for is not an argument to search method.
     * Rather, this value should be stored by the subclass along with the underlying array.
     *
     * @return the index of the closest element.
     *
     * Requires: The underlying array should be sorted.
     **/
    public int search(int from, int to) {
        while (from < to) {
            int mid = (from + to) >>> 1;
            int compareResult = compare(mid);

            if (compareResult == 0) {
                // arr[mid] == value
                return mid;
            } else if (compareResult < 0) {
                // arr[mid] < val

                if (mid < to) {
                    // Check if val is between (mid, mid + 1) before setting left = mid + 1
                    // (mid < to) ensures that mid + 1 is not out of bounds
                    int compareValAfterMid = compare(mid + 1);
                    if (compareValAfterMid > 0) {
                        return getClosestIndex(mid, mid + 1);
                    }
                } else if (mid == to) {
                    // val > arr[mid] and there are no more elements above mid, so mid is the closest
                    return mid;
                }

                from = mid + 1;
            } else {
                // arr[mid] > val

                if (mid > from) {
                    // Check if val is between (mid - 1, mid)
                    // (mid > from) ensures that mid - 1 is not out of bounds
                    int compareValBeforeMid = compare(mid - 1);
                    if (compareValBeforeMid < 0) {
                        // val is between indices (mid - 1), mid
                        return getClosestIndex(mid, mid - 1);
                    }
                } else if (mid == 0) {
                    // val < arr[mid] and there are no more candidates below mid, so mid is the closest
                    return mid;
                }

                to = mid - 1;
            }
        }

        return from;
    }

}
