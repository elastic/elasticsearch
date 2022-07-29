/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

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
import java.util.ListIterator;
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
     * Eliminate duplicates from a sorted list in-place.
     *
     * @param list A sorted list, which will be modified in place.
     * @param cmp A comparator the list is already sorted by.
     */
    public static <T> void uniquify(List<T> list, Comparator<T> cmp) {
        if (list.size() <= 1) {
            return;
        }

        ListIterator<T> uniqueItr = list.listIterator();
        ListIterator<T> existingItr = list.listIterator();
        T uniqueValue = uniqueItr.next(); // get first element to compare with
        existingItr.next(); // advance the existing iterator to the second element, where we will begin comparing
        do {
            T existingValue = existingItr.next();
            if (cmp.compare(existingValue, uniqueValue) != 0 && (uniqueValue = uniqueItr.next()) != existingValue) {
                uniqueItr.set(existingValue);
            }
        } while (existingItr.hasNext());

        // Lop off the rest of the list. Note with LinkedList this requires advancing back to this index,
        // but Java provides no way to efficiently remove from the end of a non random-access list.
        list.subList(uniqueItr.nextIndex(), list.size()).clear();
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
        if (value instanceof Map<?, ?> map) {
            return () -> Iterators.concat(map.keySet().iterator(), map.values().iterator());
        } else if ((value instanceof Iterable) && (value instanceof Path == false)) {
            return (Iterable<?>) value;
        } else if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        } else {
            return null;
        }
    }

    private static void ensureNoSelfReferences(
        final Iterable<?> value,
        Object originalReference,
        final Set<Object> ancestors,
        String messageHint
    ) {
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
            if ((list instanceof RandomAccess) == false) {
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

    /**
     * Creates a copy of the given collection with the given element appended.
     *
     * @param collection collection to copy
     * @param element    element to append
     */
    @SuppressWarnings("unchecked")
    public static <E> List<E> appendToCopy(Collection<E> collection, E element) {
        final int size = collection.size() + 1;
        final E[] array = collection.toArray((E[]) new Object[size]);
        array[size - 1] = element;
        return Collections.unmodifiableList(Arrays.asList(array));
    }

    /**
     * Same as {@link #appendToCopy(Collection, Object)} but faster by assuming that all elements in the collection and the given element
     * are non-null.
     * @param collection collection to copy
     * @param element    element to append
     * @return           list with appended element
     */
    @SuppressWarnings("unchecked")
    public static <E> List<E> appendToCopyNoNullElements(Collection<E> collection, E element) {
        final int existingSize = collection.size();
        if (existingSize == 0) {
            return List.of(element);
        }
        final int size = existingSize + 1;
        final E[] array = collection.toArray((E[]) new Object[size]);
        array[size - 1] = element;
        return List.of(array);
    }

    /**
     * Same as {@link #appendToCopyNoNullElements(Collection, Object)} but for multiple elements to append.
     */
    @SuppressWarnings("unchecked")
    public static <E> List<E> appendToCopyNoNullElements(Collection<E> collection, E... elements) {
        final int existingSize = collection.size();
        if (existingSize == 0) {
            return List.of(elements);
        }
        final int addedSize = elements.length;
        final int size = existingSize + addedSize;
        final E[] array = collection.toArray((E[]) new Object[size]);
        System.arraycopy(elements, 0, array, size - 1, addedSize);
        return List.of(array);
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

    public static <E> List<E> concatLists(List<E> listA, List<E> listB) {
        List<E> concatList = new ArrayList<>(listA.size() + listB.size());
        concatList.addAll(listA);
        concatList.addAll(listB);
        return concatList;
    }

    public static <E> List<E> wrapUnmodifiableOrEmptySingleton(List<E> list) {
        return list.isEmpty() ? List.of() : Collections.unmodifiableList(list);
    }

    public static <E> List<E> limitSize(List<E> list, int size) {
        return list.size() <= size ? list : list.subList(0, size);
    }
}
