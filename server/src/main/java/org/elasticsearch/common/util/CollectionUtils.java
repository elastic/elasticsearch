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

import com.carrotsearch.hppc.ObjectArrayList;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;

import java.nio.file.Path;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;

/** Collections-related utility methods. */
public class CollectionUtils {

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

    /**
     * Deeply inspects a Map, Iterable, or Object array looking for references back to itself.
     * @throws IllegalArgumentException if a self-reference is found
     * @param value The object to evaluate looking for self references
     * @param messageHint A string to be included in the exception message if the call fails, to provide
     *                    more context to the handler of the exception
     */
    public static void ensureNoSelfReferences(Object value, String messageHint) {
        Iterable<?> it = convert(value);
        if (it != null) {
            ensureNoSelfReferences(it, value, Collections.newSetFromMap(new IdentityHashMap<>()), messageHint);
        }
    }

    @SuppressWarnings("unchecked")
    private static Iterable<?> convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map) {
            Map<?,?> map = (Map<?,?>) value;
            return () -> Iterators.concat(map.keySet().iterator(), map.values().iterator());
        } else if ((value instanceof Iterable) && (value instanceof Path == false)) {
            return (Iterable<?>) value;
        } else if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        } else {
            return null;
        }
    }

    private static void ensureNoSelfReferences(final Iterable<?> value, Object originalReference, final Set<Object> ancestors,
                                               String messageHint) {
        if (value != null) {
            if (ancestors.add(originalReference) == false) {
                String suffix = Strings.isNullOrEmpty(messageHint) ? "" : String.format(Locale.ROOT, " (%s)", messageHint);
                throw new IllegalArgumentException("Iterable object is self-referencing itself" + suffix);
            }
            for (Object o : value) {
                ensureNoSelfReferences(convert(o), o, ancestors, messageHint);
            }
            ancestors.remove(originalReference);
        }
    }

    private static class RotatedList<T> extends AbstractList<T> implements RandomAccess {

        private final List<T> in;
        private final int distance;

        RotatedList(List<T> list, int distance) {
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
    }

    public static void sort(final BytesRefArray bytes, final int[] indices) {
        sort(new BytesRefBuilder(), new BytesRefBuilder(), bytes, indices);
    }

    private static void sort(final BytesRefBuilder scratch, final BytesRefBuilder scratch1,
                             final BytesRefArray bytes, final int[] indices) {

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

    @SuppressWarnings("unchecked")
    public static <E> ArrayList<E> iterableAsArrayList(Iterable<? extends E> elements) {
        if (elements == null) {
            throw new NullPointerException("elements");
        }
        if (elements instanceof Collection) {
            return new ArrayList<>((Collection) elements);
        } else {
            ArrayList<E> list = new ArrayList<>();
            for (E element : elements) {
                list.add(element);
            }
            return list;
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E> ArrayList<E> arrayAsArrayList(E... elements) {
        if (elements == null) {
            throw new NullPointerException("elements");
        }
        return new ArrayList<>(Arrays.asList(elements));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E> ArrayList<E> asArrayList(E first, E... other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        ArrayList<E> list = new ArrayList<>(1 + other.length);
        list.add(first);
        list.addAll(Arrays.asList(other));
        return list;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
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
