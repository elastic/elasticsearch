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

package org.elasticsearch.common.util;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 *
 */
public class ArrayUtils {

    private ArrayUtils() {}

    /**
     * Return the index of <code>value</code> in <code>array</code>, or <tt>-1</tt> if there is no such index.
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
    public static String[] concat(String[] one, String[] other) {
        return concat(one, other, String.class);
    }

    /**
     * Concatenates 2 arrays
     */
    public static <T> T[] concat(T[] one, T[] other, Class<T> clazz) {
        T[] target = (T[]) Array.newInstance(clazz, one.length + other.length);
        System.arraycopy(one, 0, target, 0, one.length);
        System.arraycopy(other, 0, target, one.length, other.length);
        return target;
    }

}
