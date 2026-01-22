/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import java.lang.reflect.Array;
import java.util.Arrays;

public class ArrayUtils {

    private ArrayUtils() {}

    /**
     * Return the index of <code>value</code> in <code>array</code>, or {@code -1} if there is no such index.
     * If there are several values that are within <code>tolerance</code> or less of <code>value</code>, this method will return the
     * index of the closest value. In case of several values being as close ot <code>value</code>, there is no guarantee which index
     * will be returned.
     * Results are undefined if the array is not sorted.
     */
    public static int binarySearch(double[] array, double value, double tolerance) {
        if (array.length == 0) {
            return -1;
        }
        return binarySearch(array, 0, array.length, value, tolerance);
    }

    private static int binarySearch(double[] array, int fromIndex, int toIndex, double value, double tolerance) {
        int index = Arrays.binarySearch(array, fromIndex, toIndex, value);
        if (index < 0) {
            final int highIndex = -1 - index; // first index of a value that is > value
            final int lowIndex = highIndex - 1; // last index of a value that is < value

            double lowError = Double.POSITIVE_INFINITY;
            double highError = Double.POSITIVE_INFINITY;
            if (lowIndex >= 0) {
                lowError = value - array[lowIndex];
            }
            if (highIndex < array.length) {
                highError = array[highIndex] - value;
            }

            if (highError < lowError) {
                if (highError < tolerance) {
                    index = highIndex;
                }
            } else if (lowError < tolerance) {
                index = lowIndex;
            } else {
                index = -1;
            }
        }
        return index;
    }

    /**
     * Concatenates 2 arrays
     */
    public static <T> T[] concat(T[] one, T[] other) {
        @SuppressWarnings("unchecked")
        T[] target = (T[]) Array.newInstance(other.getClass().componentType(), one.length + other.length);
        System.arraycopy(one, 0, target, 0, one.length);
        System.arraycopy(other, 0, target, one.length, other.length);
        return target;
    }

    /**
     * Copy the given element and array into a new array of size {@code array.length + 1}.
     * @param added first element in the newly created array
     * @param array array to copy to the end of new returned array copy
     * @return copy that contains added element and array
     * @param <T> type of the array elements
     */
    public static <T> T[] prepend(T added, T[] array) {
        @SuppressWarnings("unchecked")
        T[] updated = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length + 1);
        updated[0] = added;
        System.arraycopy(array, 0, updated, 1, array.length);
        return updated;
    }

    /**
     * Copy the given array and the added element into a new array of size {@code array.length + 1}.
     * @param array array to copy to the beginning of new returned array copy
     * @param added last element in the newly created array
     * @return copy that contains array and added element
     * @param <T> type of the array elements
     */
    public static <T> T[] append(T[] array, T added) {
        T[] updated = Arrays.copyOf(array, array.length + 1);
        updated[array.length] = added;
        return updated;
    }

    /**
     * Reverse the {@code length} values on the array starting from {@code offset}.
     */
    public static void reverseSubArray(double[] array, int offset, int length) {
        int start = offset;
        int end = offset + length;
        while (end > start) {
            final double scratch = array[start];
            array[start] = array[end - 1];
            array[end - 1] = scratch;
            start++;
            end--;
        }
    }

    /**
     * Reverse the {@code length} values on the array starting from {@code offset}.
     */
    public static void reverseSubArray(long[] array, int offset, int length) {
        int start = offset;
        int end = offset + length;
        while (end > start) {
            final long scratch = array[start];
            array[start] = array[end - 1];
            array[end - 1] = scratch;
            start++;
            end--;
        }
    }

    /**
     * Reverse the {@code length} values on the array starting from {@code offset}.
     */
    public static void reverseArray(byte[] array, int offset, int length) {
        int start = offset;
        int end = offset + length;
        while (start < end) {
            final byte temp = array[start];
            array[start] = array[end - 1];
            array[end - 1] = temp;
            start++;
            end--;
        }
    }

}
