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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collector;

public final class Sets {
    private Sets() {}

    public static <T> HashSet<T> newHashSet(Iterator<T> iterator) {
        HashSet<T> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }
        return set;
    }

    public static <T> HashSet<T> newHashSet(Iterable<T> iterable) {
        return iterable instanceof Collection ? new HashSet<>((Collection<T>) iterable) : newHashSet(iterable.iterator());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> HashSet<T> newHashSet(T... elements) {
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

    public static <T> boolean haveEmptyIntersection(Set<T> left, Set<T> right) {
        for (T t : left) {
            if (right.contains(t)) {
                return false;
            }
        }
        return true;
    }

    public static <T> boolean haveNonEmptyIntersection(Set<T> left, Set<T> right) {
        return haveEmptyIntersection(left, right) == false;
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
        Set<T> set = new HashSet<>();
        for (T k : left) {
            if (right.contains(k) == false) {
                set.add(k);
            }
        }
        return set;
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
        final SortedSet<T> set = new TreeSet<>();
        for (T k : left) {
            if (right.contains(k) == false) {
                set.add(k);
            }
        }
        return set;
    }

    /**
     * Returns a {@link Collector} that accumulates the input elements into a sorted set and finishes the resulting set into an
     * unmodifiable set. The resulting read-only view through the unmodifiable set is a sorted set.
     *
     * @param <T> the type of the input elements
     * @return an unmodifiable set where the underlying set is sorted
     */
    public static <T> Collector<T, SortedSet<T>, SortedSet<T>> toUnmodifiableSortedSet() {
        return Collector.of(TreeSet::new, SortedSet::add, (a, b) -> {
            a.addAll(b);
            return a;
        }, Collections::unmodifiableSortedSet);
    }

    public static <T> Set<T> union(Set<T> left, Set<T> right) {
        Set<T> union = new HashSet<>(left);
        union.addAll(right);
        return union;
    }

    /**
     * The intersection of two sets. Namely, the resulting set contains all the elements that are in both sets.
     * Neither input is mutated by this operation, an entirely new set is returned.
     *
     * @param set1 the first set
     * @param set2 the second set
     * @return the unmodifiable intersection of the two sets
     */
    public static <T> Set<T> intersection(Set<T> set1, Set<T> set2) {
        final Set<T> left;
        final Set<T> right;
        if (set1.size() < set2.size()) {
            left = set1;
            right = set2;
        } else {
            left = set2;
            right = set1;
        }

        final Set<T> empty = Set.of();
        Set<T> result = empty;
        for (T t : left) {
            if (right.contains(t)) {
                if (result == empty) {
                    // delay allocation of a non-empty result set
                    result = new HashSet<>();
                }
                result.add(t);
            }
        }

        // the empty set is already unmodifiable
        return result == empty ? result : Collections.unmodifiableSet(result);
    }

    /**
     * Creates a copy of the given set and adds extra elements.
     *
     * @param set      set to copy
     * @param elements elements to add
     */
    @SuppressWarnings("unchecked")
    public static <E> Set<E> addToCopy(Set<E> set, E... elements) {
        final var res = new HashSet<>(set);
        Collections.addAll(res, elements);
        return Set.copyOf(res);
    }
}
