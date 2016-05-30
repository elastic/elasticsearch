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

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.IntroSorter;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;

/** Collections-related utility methods. */
public class CollectionUtils {
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
                return Long.compare(array[i], array[j]);
            }

            @Override
            protected void setPivot(int i) {
                pivot = array[i];
            }

            @Override
            protected int comparePivot(int j) {
                return Long.compare(pivot, array[j]);
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

    /**
     * Checks if the given array contains any elements.
     *
     * @param array The array to check
     *
     * @return false if the array contains an element, true if not or the array is null.
     */
    public static boolean isEmpty(Object[] array) {
        return array == null || array.length == 0;
    }

    /**
     * Return a rotated view of the given list with the given distance.
     */
    public static <T> List<T> rotate(final List<T> list, int distance) {
        if (list.isEmpty()) {
            return list;
        }

        int d = distance % list.size();
        if (d < 0) {
            d += list.size();
        }

        if (d == 0) {
            return list;
        }

        return new RotatedList<>(list, d);
    }

    public static void sortAndDedup(final ObjectArrayList<byte[]> array) {
        int len = array.size();
        if (len > 1) {
            sort(array);
            int uniqueCount = 1;
            for (int i = 1; i < len; ++i) {
                if (!Arrays.equals(array.get(i), array.get(i - 1))) {
                    array.set(uniqueCount++, array.get(i));
                }
            }
            array.elementsCount = uniqueCount;
        }
    }

    public static void sort(final ObjectArrayList<byte[]> array) {
        new IntroSorter() {

            byte[] pivot;

            @Override
            protected void swap(int i, int j) {
                final byte[] tmp = array.get(i);
                array.set(i, array.get(j));
                array.set(j, tmp);
            }

            @Override
            protected int compare(int i, int j) {
                return compare(array.get(i), array.get(j));
            }

            @Override
            protected void setPivot(int i) {
                pivot = array.get(i);
            }

            @Override
            protected int comparePivot(int j) {
                return compare(pivot, array.get(j));
            }

            private int compare(byte[] left, byte[] right) {
                for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
                    int a = left[i] & 0xFF;
                    int b = right[j] & 0xFF;
                    if (a != b) {
                        return a - b;
                    }
                }
                return left.length - right.length;
            }

        }.sort(0, array.size());
    }

    public static int[] toArray(Collection<Integer> ints) {
        Objects.requireNonNull(ints);
        return ints.stream().mapToInt(s -> s).toArray();
    }

    private static class RotatedList<T> extends AbstractList<T> implements RandomAccess {

        private final List<T> in;
        private final int distance;

        public RotatedList(List<T> list, int distance) {
            if (distance < 0 || distance >= list.size()) {
                throw new IllegalArgumentException();
            }
            if (!(list instanceof RandomAccess)) {
                throw new IllegalArgumentException();
            }
            this.in = list;
            this.distance = distance;
        }

        @Override
        public T get(int index) {
            int idx = distance + index;
            if (idx < 0 || idx >= in.size()) {
                idx -= in.size();
            }
            return in.get(idx);
        }

        @Override
        public int size() {
            return in.size();
        }

    };
    public static void sort(final BytesRefArray bytes, final int[] indices) {
        sort(new BytesRefBuilder(), new BytesRefBuilder(), bytes, indices);
    }

    private static void sort(final BytesRefBuilder scratch, final BytesRefBuilder scratch1, final BytesRefArray bytes, final int[] indices) {

        final int numValues = bytes.size();
        assert indices.length >= numValues;
        if (numValues > 1) {
            new InPlaceMergeSorter() {
                final Comparator<BytesRef> comparator = Comparator.naturalOrder();
                @Override
                protected int compare(int i, int j) {
                    return comparator.compare(bytes.get(scratch, indices[i]), bytes.get(scratch1, indices[j]));
                }

                @Override
                protected void swap(int i, int j) {
                    int value_i = indices[i];
                    indices[i] = indices[j];
                    indices[j] = value_i;
                }
            }.sort(0, numValues);
        }

    }

    public static int sortAndDedup(final BytesRefArray bytes, final int[] indices) {
        final BytesRefBuilder scratch = new BytesRefBuilder();
        final BytesRefBuilder scratch1 = new BytesRefBuilder();
        final int numValues = bytes.size();
        assert indices.length >= numValues;
        if (numValues <= 1) {
            return numValues;
        }
        sort(scratch, scratch1, bytes, indices);
        int uniqueCount = 1;
        BytesRefBuilder previous = scratch;
        BytesRefBuilder current = scratch1;
        bytes.get(previous, indices[0]);
        for (int i = 1; i < numValues; ++i) {
            bytes.get(current, indices[i]);
            if (!previous.get().equals(current.get())) {
                indices[uniqueCount++] = indices[i];
            }
            BytesRefBuilder tmp = previous;
            previous = current;
            current = tmp;
        }
        return uniqueCount;

    }

    public static <E> ArrayList<E> iterableAsArrayList(Iterable<? extends E> elements) {
        if (elements == null) {
            throw new NullPointerException("elements");
        }
        if (elements instanceof Collection) {
            return new ArrayList<>((Collection)elements);
        } else {
            ArrayList<E> list = new ArrayList<>();
            for (E element : elements) {
                list.add(element);
            }
            return list;
        }
    }

    public static <E> ArrayList<E> arrayAsArrayList(E... elements) {
        if (elements == null) {
            throw new NullPointerException("elements");
        }
        return new ArrayList<>(Arrays.asList(elements));
    }

    public static <E> ArrayList<E> asArrayList(E first, E... other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        ArrayList<E> list = new ArrayList<>(1 + other.length);
        list.add(first);
        list.addAll(Arrays.asList(other));
        return list;
    }

    public static<E> ArrayList<E> asArrayList(E first, E second, E... other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        ArrayList<E> list = new ArrayList<>(1 + 1 + other.length);
        list.add(first);
        list.add(second);
        list.addAll(Arrays.asList(other));
        return list;
    }

    public static <E> ArrayList<E> newSingletonArrayList(E element) {
        return new ArrayList<>(Collections.singletonList(element));
    }

    public static <E> LinkedList<E> newLinkedList(Iterable<E> elements) {
        if (elements == null) {
            throw new NullPointerException("elements");
        }
        LinkedList<E> linkedList = new LinkedList<>();
        for (E element : elements) {
            linkedList.add(element);
        }
        return linkedList;
    }

    public static <E> List<List<E>> eagerPartition(List<E> list, int size) {
        if (list == null) {
            throw new NullPointerException("list");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("size <= 0");
        }
        List<List<E>> result = new ArrayList<>((int) Math.ceil(list.size() / size));

        List<E> accumulator = new ArrayList<>(size);
        int count = 0;
        for (E element : list) {
            if (count == size) {
                result.add(accumulator);
                accumulator = new ArrayList<>(size);
                count = 0;
            }
            accumulator.add(element);
            count++;
        }
        if (count > 0) {
            result.add(accumulator);
        }

        return result;
    }
}
