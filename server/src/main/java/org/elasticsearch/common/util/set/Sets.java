/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.set;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

public final class Sets {
    private Sets() {}

    public static <T> HashSet<T> newHashSet(Iterator<T> iterator) {
        Objects.requireNonNull(iterator);
        HashSet<T> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }
        return set;
    }

    public static <T> HashSet<T> newHashSet(Iterable<T> iterable) {
        Objects.requireNonNull(iterable);
        return iterable instanceof Collection ? new HashSet<>((Collection<T>) iterable) : newHashSet(iterable.iterator());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> HashSet<T> newHashSet(T... elements) {
        Objects.requireNonNull(elements);
        return new HashSet<>(Arrays.asList(elements));
    }

    public static <E> Set<E> newHashSetWithExpectedSize(int expectedSize) {
        return new HashSet<>(capacity(expectedSize));
    }

    public static <E> LinkedHashSet<E> newLinkedHashSetWithExpectedSize(int expectedSize) {
        return new LinkedHashSet<>(capacity(expectedSize));
    }

    static int capacity(int expectedSize) {
        assert expectedSize >= 0;
        return expectedSize < 2 ? expectedSize + 1 : (int) (expectedSize / 0.75 + 1.0);
    }

    public static <T> Set<T> newConcurrentHashSet() {
        return Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public static <T> boolean haveEmptyIntersection(Set<T> left, Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        return left.stream().noneMatch(right::contains);
    }

    public static <T> boolean haveNonEmptyIntersection(Set<T> left, Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        return left.stream().anyMatch(right::contains);
    }

    /**
     * The relative complement, or difference, of the specified left and right set. Namely, the resulting set contains all the elements that
     * are in the left set but not in the right set. Neither input is mutated by this operation, an entirely new set is returned.
     *
     * @param left  the left set
     * @param right the right set
     * @param <T>   the type of the elements of the sets
     * @return the relative complement of the left set with respect to the right set
     */
    public static <T> Set<T> difference(Set<T> left, Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        return left.stream().filter(k -> right.contains(k) == false).collect(Collectors.toSet());
    }

    /**
     * The relative complement, or difference, of the specified left and right set, returned as a sorted set. Namely, the resulting set
     * contains all the elements that are in the left set but not in the right set, and the set is sorted using the natural ordering of
     * element type. Neither input is mutated by this operation, an entirely new set is returned.
     *
     * @param left  the left set
     * @param right the right set
     * @param <T>   the type of the elements of the sets
     * @return the sorted relative complement of the left set with respect to the right set
     */
    public static <T> SortedSet<T> sortedDifference(final Set<T> left, final Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        return left.stream().filter(k -> right.contains(k) == false).collect(toSortedSet());
    }

    /**
     * The relative complement, or difference, of the specified left and right set, returned as a sorted set. Namely, the resulting set
     * contains all the elements that are in the left set but not in the right set, and the set is sorted using the natural ordering of
     * element type. Neither input is mutated by this operation, an entirely new set is returned. The resulting set is unmodifiable.
     *
     * @param left  the left set
     * @param right the right set
     * @param <T>   the type of the elements of the sets
     * @return the unmodifiable sorted relative complement of the left set with respect to the right set
     */
    public static <T> SortedSet<T> unmodifiableSortedDifference(final Set<T> left, final Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        return left.stream().filter(k -> right.contains(k) == false).collect(toUnmodifiableSortedSet());
    }

    /**
     * Returns a {@link Collector} that accumulates the input elements into a sorted set.
     *
     * @param <T> the type of the input elements
     * @return a sorted set
     */
    public static <T> Collector<T, SortedSet<T>, SortedSet<T>> toSortedSet() {
        return Collector.of(TreeSet::new, SortedSet::add, Sets::addAllMutable);
    }

    /**
     * Returns a {@link Collector} that accumulates the input elements into a sorted set and finishes the resulting set into an
     * unmodifiable set. The resulting read-only view through the unmodifiable set is a sorted set.
     *
     * @param <T> the type of the input elements
     * @return an unmodifiable set where the underlying set is sorted
     */
    public static <T> Collector<T, SortedSet<T>, SortedSet<T>> toUnmodifiableSortedSet() {
        return Collector.of(TreeSet::new, SortedSet::add, Sets::addAllMutable, Collections::unmodifiableSortedSet);
    }

    private static <T, S extends Set<T>> S addAllMutable(S a, S b) {
        a.addAll(b);
        return a;
    }

    public static <T> Set<T> union(Set<T> left, Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        Set<T> union = new HashSet<>(left);
        union.addAll(right);
        return union;
    }

    public static <T> Set<T> intersection(Set<T> set1, Set<T> set2) {
        Objects.requireNonNull(set1);
        Objects.requireNonNull(set2);
        final Set<T> left;
        final Set<T> right;
        if (set1.size() < set2.size()) {
            left = set1;
            right = set2;
        } else {
            left = set2;
            right = set1;
        }
        return left.stream().filter(right::contains).collect(Collectors.toSet());
    }

    /**
     * Creates a copy of the given set and adds extra element.
     *
     * @param set     set to copy
     * @param element element to add
     */
    public static <E> Set<E> addToCopy(Set<E> set, E element) {
        return Stream.concat(set.stream(), Stream.of(element)).collect(toUnmodifiableSet());
    }

    /**
     * Creates a copy of the given set and adds extra elements.
     *
     * @param set      set to copy
     * @param elements elements to add
     */
    @SuppressWarnings("unchecked")
    public static <E> Set<E> addToCopy(Set<E> set, E... elements) {
        return Stream.concat(set.stream(), Stream.of(elements)).collect(toUnmodifiableSet());
    }
}
