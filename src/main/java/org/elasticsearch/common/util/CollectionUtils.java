/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.apache.lucene.util.IntroSorter;

/** Collections-related utility methods. */
public enum CollectionUtils {
    ;

    private static int compare(long i, long j) {
        return i < j ? -1 : (i == j ? 0 : 1);
    }

    public static void sort(LongArrayList list) {
        sort(list.buffer, list.size());
    }

    public static void sort(final long[] array, int len) {
        new IntroSorter() {

            long pivot;

            @Override
            protected void swap(int i, int j) {
                final long tmp = array[i];
                array[i] = array[j];
                array[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                return CollectionUtils.compare(array[i], array[j]);
            }

            @Override
            protected void setPivot(int i) {
                pivot = array[i];
            }

            @Override
            protected int comparePivot(int j) {
                return CollectionUtils.compare(pivot, array[j]);
            }

        }.sort(0, len);
    }

    public static void sortAndDedup(LongArrayList list) {
        list.elementsCount = sortAndDedup(list.buffer, list.elementsCount);
    }

    /** Sort and deduplicate values in-place, then return the unique element count. */
    public static int sortAndDedup(long[] array, int len) {
        if (len <= 1) {
            return len;
        }
        sort(array, len);
        int uniqueCount = 1;
        for (int i = 1; i < len; ++i) {
            if (array[i] != array[i - 1]) {
                array[uniqueCount++] = array[i];
            }
        }
        return uniqueCount;
    }

    public static void sort(FloatArrayList list) {
        sort(list.buffer, list.size());
    }

    public static void sort(final float[] array, int len) {
        new IntroSorter() {

            float pivot;

            @Override
            protected void swap(int i, int j) {
                final float tmp = array[i];
                array[i] = array[j];
                array[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                return Float.compare(array[i], array[j]);
            }

            @Override
            protected void setPivot(int i) {
                pivot = array[i];
            }

            @Override
            protected int comparePivot(int j) {
                return Float.compare(pivot, array[j]);
            }

        }.sort(0, len);
    }

    public static void sortAndDedup(FloatArrayList list) {
        list.elementsCount = sortAndDedup(list.buffer, list.elementsCount);
    }

    /** Sort and deduplicate values in-place, then return the unique element count. */
    public static int sortAndDedup(float[] array, int len) {
        if (len <= 1) {
            return len;
        }
        sort(array, len);
        int uniqueCount = 1;
        for (int i = 1; i < len; ++i) {
            if (Float.compare(array[i], array[i - 1]) != 0) {
                array[uniqueCount++] = array[i];
            }
        }
        return uniqueCount;
    }

    public static void sort(DoubleArrayList list) {
        sort(list.buffer, list.size());
    }

    public static void sort(final double[] array, int len) {
        new IntroSorter() {

            double pivot;

            @Override
            protected void swap(int i, int j) {
                final double tmp = array[i];
                array[i] = array[j];
                array[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                return Double.compare(array[i], array[j]);
            }

            @Override
            protected void setPivot(int i) {
                pivot = array[i];
            }

            @Override
            protected int comparePivot(int j) {
                return Double.compare(pivot, array[j]);
            }

        }.sort(0, len);
    }

    public static void sortAndDedup(DoubleArrayList list) {
        list.elementsCount = sortAndDedup(list.buffer, list.elementsCount);
    }

    /** Sort and deduplicate values in-place, then return the unique element count. */
    public static int sortAndDedup(double[] array, int len) {
        if (len <= 1) {
            return len;
        }
        sort(array, len);
        int uniqueCount = 1;
        for (int i = 1; i < len; ++i) {
            if (Double.compare(array[i], array[i - 1]) != 0) {
                array[uniqueCount++] = array[i];
            }
        }
        return uniqueCount;
    }

}
