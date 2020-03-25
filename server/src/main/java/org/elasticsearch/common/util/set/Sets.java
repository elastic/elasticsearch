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

package org.elasticsearch.common.util.set;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public final class Sets {
    private Sets() {
    }

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
        HashSet<T> set = new HashSet<>(elements.length);
        Collections.addAll(set, elements);
        return set;
    }

    public static <T> Set<T> newConcurrentHashSet() {
        return Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public static <T> boolean haveEmptyIntersection(Set<T> left, Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        return left.stream().noneMatch(right::contains);
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
        return left.stream().filter(k -> !right.contains(k)).collect(Collectors.toSet());
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
        return new SortedSetCollector<>();
    }

    /**
     * Returns a {@link Collector} that accumulates the input elements into a sorted set and finishes the resulting set into an
     * unmodifiable set. The resulting read-only view through the unmodifiable set is a sorted set.
     *
     * @param <T> the type of the input elements
     * @return an unmodifiable set where the underlying set is sorted
     */
    public static <T> Collector<T, SortedSet<T>, SortedSet<T>> toUnmodifiableSortedSet() {
        return new UnmodifiableSortedSetCollector<>();
    }

    abstract static class AbstractSortedSetCollector<T> implements Collector<T, SortedSet<T>, SortedSet<T>> {

        @Override
        public Supplier<SortedSet<T>> supplier() {
            return TreeSet::new;
        }

        @Override
        public BiConsumer<SortedSet<T>, T> accumulator() {
            return SortedSet::add;
        }

        @Override
        public BinaryOperator<SortedSet<T>> combiner() {
            return (s, t) -> {
                s.addAll(t);
                return s;
            };
        }

        public abstract Function<SortedSet<T>, SortedSet<T>> finisher();

        public abstract Set<Characteristics> characteristics();

    }

    private static class SortedSetCollector<T> extends AbstractSortedSetCollector<T> {

        @Override
        public Function<SortedSet<T>, SortedSet<T>> finisher() {
            return Function.identity();
        }

        static final Set<Characteristics> CHARACTERISTICS =
            Collections.unmodifiableSet(EnumSet.of(Characteristics.IDENTITY_FINISH));

        @Override
        public Set<Characteristics> characteristics() {
            return CHARACTERISTICS;
        }

    }

    private static class UnmodifiableSortedSetCollector<T> extends AbstractSortedSetCollector<T> {

        @Override
        public Function<SortedSet<T>, SortedSet<T>> finisher() {
            return Collections::unmodifiableSortedSet;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }

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
}
