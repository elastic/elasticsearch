/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.Strings;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
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

        ListIterator<T> resultItr = list.listIterator();
        ListIterator<T> currentItr = list.listIterator(1); // start at second element to compare
        T lastValue = resultItr.next(); // result always includes first element, so advance it and grab first
        do {
            T currentValue = currentItr.next(); // each iter we check if the next element is different from the last we put in the result
            if (cmp.compare(lastValue, currentValue) != 0) {
                lastValue = currentValue;
                resultItr.next(); // advance result so the current position is where we want to set
                if (resultItr.previousIndex() != currentItr.previousIndex()) {
                    // optimization: only need to set if different
                    resultItr.set(currentValue);
                }
            }
        } while (currentItr.hasNext());

        // Lop off the rest of the list. Note with LinkedList this requires advancing back to this index,
        // but Java provides no way to efficiently remove from the end of a non random-access list.
        list.subList(resultItr.nextIndex(), list.size()).clear();
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
     * Returns the sum of the integer values in the given map.
     *
     * @param map the map whose values to sum; may be null or empty
     * @return the sum of the values, or 0 if the map is null or empty
     */
    public static int sumIntValues(Map<?, Integer> map) {
        if (map == null || map.isEmpty()) {
            return 0;
        }
        return map.values().stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * Deeply inspects a Map, Iterable, or Object array looking for references back to itself.
     * @throws IllegalArgumentException if a self-reference is found
     * @param value The object to evaluate looking for self references
     * @param messageHint A string to be included in the exception message if the call fails, to provide
     *                    more context to the handler of the exception
     */
    public static void ensureNoSelfReferences(final Object value, final String messageHint) {
        ensureNoSelfReferences(value, null, messageHint);
    }

    private static void ensureNoSelfReferences(final Object value, Set<Object> ancestors, final String messageHint) {
        // these instanceof checks are a bit on the ugly side, but it's important for performance that we have
        // a separate dispatch point for Maps versus for Iterables. a polymorphic version of this code would
        // be prettier, but it would also likely be quite a bit slower. this is a hot path for ingest pipelines,
        // and performance here is important.
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            // noop
        } else {
            // defer allocating an ancestors set until necessary
            ancestors = ancestors != null ? ancestors : Collections.newSetFromMap(new IdentityHashMap<>());

            if (value instanceof Map<?, ?> m && m.isEmpty() == false) {
                ensureNoSelfReferences(m, ancestors, messageHint);
            } else if ((value instanceof Iterable<?> i) && (value instanceof Path == false)) {
                ensureNoSelfReferences(i, i, ancestors, messageHint);
            } else if (value instanceof Object[]) {
                // note: the iterable and reference arguments are different
                ensureNoSelfReferences(Arrays.asList((Object[]) value), value, ancestors, messageHint);
            }
        }
    }

    private static void ensureNoSelfReferences(final Map<?, ?> reference, final Set<Object> ancestors, final String messageHint) {
        addToAncestorsOrThrow(reference, ancestors, messageHint);
        for (Map.Entry<?, ?> e : reference.entrySet()) {
            ensureNoSelfReferences(e.getKey(), ancestors, messageHint);
            ensureNoSelfReferences(e.getValue(), ancestors, messageHint);
        }
        ancestors.remove(reference);
    }

    private static void ensureNoSelfReferences(
        final Iterable<?> iterable,
        final Object reference,
        final Set<Object> ancestors,
        final String messageHint
    ) {
        addToAncestorsOrThrow(reference, ancestors, messageHint);
        for (Object o : iterable) {
            ensureNoSelfReferences(o, ancestors, messageHint);
        }
        ancestors.remove(reference);
    }

    private static void addToAncestorsOrThrow(Object reference, Set<Object> ancestors, String messageHint) {
        if (ancestors.add(reference) == false) {
            StringBuilder sb = new StringBuilder("Iterable object is self-referencing itself");
            if (Strings.hasLength(messageHint)) {
                sb.append(" (").append(messageHint).append(")");
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }

    /**
     * Options that control the behavior of {@link #deepCopy(Object)}.
     */
    public enum DeepCopyOption {
        /**
         * Wrap copied {@link Map}, {@link List}, and {@link Set} instances in their
         * {@link Collections#unmodifiableMap unmodifiable} counterparts.
         */
        UNMODIFIABLE(1),
        /**
         * Use insertion-order-preserving collections ({@link LinkedHashMap}, {@link java.util.LinkedHashSet}) when
         * copying {@link Map} and {@link Set} instances.
         */
        ORDERED(2);

        private final int mask;

        DeepCopyOption(int mask) {
            this.mask = mask;
        }
    }

    /**
     * Returns a deep copy of {@code value}. Handles {@link Map}, {@link List}, {@link Set},
     * {@code byte[]}, {@code double[]}, {@code double[][]}, {@link Date}, and all immutable scalar
     * types produced by JSON parsing ({@link String}, {@link Boolean}, {@link Integer}, {@link Long},
     * {@link Float}, {@link Double}, {@link BigInteger}, {@link BigDecimal}, {@link Byte},
     * {@link Short}, {@link Character}, {@link ZonedDateTime}). For any other type, asserts at
     * development time (when assertions are enabled) and returns the value by reference at runtime,
     * so callers on data paths are not broken by an unexpected type.
     */
    public static <T> T deepCopy(T value) {
        return deepCopyInternal(value, 0);
    }

    /** @see #deepCopy(Object) */
    public static <T> T deepCopy(T value, DeepCopyOption a) {
        return deepCopyInternal(value, a.mask);
    }

    /** @see #deepCopy(Object) */
    public static <T> T deepCopy(T value, DeepCopyOption a, DeepCopyOption b) {
        return deepCopyInternal(value, a.mask | b.mask);
    }

    // note: in the future, three option or varargs methods can be added -- with only two possible options at present
    // it's simple enough to just iterate the 0, 1, and 2 options. :shrug:

    @SuppressWarnings("unchecked")
    private static <T> T deepCopyInternal(T value, int options) {
        final boolean unmodifiable = (options & DeepCopyOption.UNMODIFIABLE.mask) != 0;
        final boolean ordered = (options & DeepCopyOption.ORDERED.mask) != 0;
        if (value == null
            || value instanceof String
            || value instanceof Boolean
            || value instanceof Integer
            || value instanceof Long
            || value instanceof Double) {
            // hot path: the most common leaf value types in documents are immutable and need no copy
            return value;
        } else if (value instanceof Map<?, ?> mapValue) {
            Map<Object, Object> copy = ordered ? LinkedHashMap.newLinkedHashMap(mapValue.size()) : HashMap.newHashMap(mapValue.size());
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                copy.put(entry.getKey(), deepCopyInternal(entry.getValue(), options));
            }
            return (T) (unmodifiable ? Collections.unmodifiableMap(copy) : copy);
        } else if (value instanceof List<?> listValue) {
            List<Object> copy = new ArrayList<>(listValue.size());
            for (Object item : listValue) {
                copy.add(deepCopyInternal(item, options));
            }
            return (T) (unmodifiable ? Collections.unmodifiableList(copy) : copy);
        } else if (value instanceof Set<?> setValue) {
            Set<Object> copy = ordered ? LinkedHashSet.newLinkedHashSet(setValue.size()) : HashSet.newHashSet(setValue.size());
            for (Object item : setValue) {
                copy.add(deepCopyInternal(item, options));
            }
            return (T) (unmodifiable ? Collections.unmodifiableSet(copy) : copy);
        } else if (value instanceof byte[] bytes) {
            if (unmodifiable) {
                throw new IllegalArgumentException("cannot make array type [" + value.getClass() + "] unmodifiable");
            }
            return (T) Arrays.copyOf(bytes, bytes.length);
        } else if (value instanceof double[][] doubles) {
            if (unmodifiable) {
                throw new IllegalArgumentException("cannot make array type [" + value.getClass() + "] unmodifiable");
            }
            double[][] result = new double[doubles.length][];
            for (int i = 0; i < doubles.length; i++) {
                result[i] = Arrays.copyOf(doubles[i], doubles[i].length);
            }
            return (T) result;
        } else if (value instanceof double[] doubles) {
            if (unmodifiable) {
                throw new IllegalArgumentException("cannot make array type [" + value.getClass() + "] unmodifiable");
            }
            return (T) Arrays.copyOf(doubles, doubles.length);
        } else if (value instanceof Float
            || value instanceof Byte
            || value instanceof Short
            || value instanceof Character
            || value instanceof BigInteger
            || value instanceof BigDecimal
            || value instanceof ZonedDateTime) {
                // n.b. java.util.concurrent.atomic types (AtomicInteger etc.), and some other Number subclasses are mutable,
                // so we enumerate the immutable Number subclasses explicitly rather than using instanceof Number
                return value;
            } else if (value instanceof Date date) {
                return (T) date.clone();
            } else {
                // If this list of expected value types ends up not being exhaustive, we want to know
                // at development time, but it is better to pass the value through at runtime rather
                // than blow up on users.
                assert false : "unexpected value type [" + value.getClass() + "]";
                return value;
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
            return new ArrayList<>((Collection<E>) elements);
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
        System.arraycopy(elements, 0, array, size - addedSize, addedSize);
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

    public static <E> List<E> concatLists(List<E> listA, Collection<E> listB) {
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
