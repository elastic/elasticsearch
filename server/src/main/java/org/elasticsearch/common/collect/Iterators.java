/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

public class Iterators {

    /**
     * Returns a single element iterator over the supplied value.
     */
    public static <T> Iterator<T> single(T element) {
        return new SingleIterator<>(element);
    }

    private static final class SingleIterator<T> implements Iterator<T> {
        private T value;

        SingleIterator(T element) {
            value = Objects.requireNonNull(element);
        }

        @Override
        public boolean hasNext() {
            return value != null;
        }

        @Override
        public T next() {
            final T res = value;
            value = null;
            return res;
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Iterator<T> concat(Iterator<? extends T>... iterators) {
        if (iterators == null) {
            throw new NullPointerException("iterators");
        }

        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i].hasNext()) {
                // explicit generic type argument needed for type inference
                return new ConcatenatedIterator<>(iterators, i);
            }
        }

        return Collections.emptyIterator();
    }

    private static class ConcatenatedIterator<T> implements Iterator<T> {
        private final Iterator<? extends T>[] iterators;
        private int index;

        ConcatenatedIterator(Iterator<? extends T>[] iterators, int startIndex) {
            for (int i = startIndex; i < iterators.length; i++) {
                if (iterators[i] == null) {
                    throw new NullPointerException("iterators[" + i + "]");
                }
            }
            this.iterators = iterators;
            this.index = startIndex;
        }

        @Override
        public boolean hasNext() {
            return index < iterators.length;
        }

        @Override
        public T next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            final T value = iterators[index].next();
            while (index < iterators.length && iterators[index].hasNext() == false) {
                index++;
            }
            return value;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            while (index < iterators.length) {
                iterators[index++].forEachRemaining(action);
            }
        }
    }

    public static <T> Iterator<T> forArray(T[] array) {
        return Arrays.asList(array).iterator();
    }

    public static <T> Iterator<T> forRange(int lowerBoundInclusive, int upperBoundExclusive, IntFunction<? extends T> fn) {
        assert lowerBoundInclusive <= upperBoundExclusive : lowerBoundInclusive + " vs " + upperBoundExclusive;
        if (upperBoundExclusive <= lowerBoundInclusive) {
            return Collections.emptyIterator();
        } else {
            return new IntRangeIterator<>(lowerBoundInclusive, upperBoundExclusive, Objects.requireNonNull(fn));
        }
    }

    private static final class IntRangeIterator<T> implements Iterator<T> {
        private final IntFunction<? extends T> fn;
        private final int upperBoundExclusive;
        private int index;

        IntRangeIterator(int lowerBoundInclusive, int upperBoundExclusive, IntFunction<? extends T> fn) {
            this.fn = fn;
            this.index = lowerBoundInclusive;
            this.upperBoundExclusive = upperBoundExclusive;
        }

        @Override
        public boolean hasNext() {
            return index < upperBoundExclusive;
        }

        @Override
        public T next() {
            if (index >= upperBoundExclusive) {
                throw new NoSuchElementException();
            }
            return fn.apply(index++);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T, U> Iterator<U> map(Iterator<? extends T> input, Function<T, ? extends U> fn) {
        if (input.hasNext()) {
            if (input instanceof MapIterator mapIterator) {
                return new MapIterator<>(mapIterator.input, mapIterator.fn.andThen(fn));
            }
            return new MapIterator<>(input, fn);
        } else {
            return Collections.emptyIterator();
        }
    }

    private static final class MapIterator<T, U> implements Iterator<U> {
        private final Iterator<? extends T> input;
        private final Function<T, ? extends U> fn;

        MapIterator(Iterator<? extends T> input, Function<T, ? extends U> fn) {
            this.input = input;
            this.fn = fn;
        }

        @Override
        public boolean hasNext() {
            return input.hasNext();
        }

        @Override
        public U next() {
            return fn.apply(input.next());
        }

        @Override
        public void forEachRemaining(Consumer<? super U> action) {
            input.forEachRemaining(t -> action.accept(fn.apply(t)));
        }
    }

    /**
     * @param input An iterator over <i>non-null</i> values.
     * @param predicate The predicate with which to filter the input.
     * @return an iterator which returns the values from {@code input} which match {@code predicate}.
     */
    public static <T> Iterator<T> filter(Iterator<? extends T> input, Predicate<T> predicate) {
        while (input.hasNext()) {
            final var value = input.next();
            assert value != null;
            if (predicate.test(value)) {
                return new FilterIterator<>(value, input, predicate);
            }
        }
        return Collections.emptyIterator();
    }

    private static final class FilterIterator<T> implements Iterator<T> {
        private final Iterator<? extends T> input;
        private final Predicate<T> predicate;
        private T next;

        FilterIterator(T value, Iterator<? extends T> input, Predicate<T> predicate) {
            this.next = value;
            this.input = input;
            this.predicate = predicate;
            assert next != null;
            assert predicate.test(next);
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public T next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            final var value = next;
            while (input.hasNext()) {
                final var laterValue = input.next();
                assert laterValue != null;
                if (predicate.test(laterValue)) {
                    next = laterValue;
                    return value;
                }
            }
            next = null;
            return value;
        }
    }

    /**
     * Returns an iterator that yields at most the first {@code n} elements of the provided {@code input} iterator.
     */
    public static <T> Iterator<T> limit(Iterator<? extends T> input, int n) {
        assert n >= 0 : "negative limit";
        if (n > 0 && input.hasNext()) {
            return new LimitIterator<>(input, n);
        } else {
            return Collections.emptyIterator();
        }
    }

    private static final class LimitIterator<T> implements Iterator<T> {
        private final Iterator<? extends T> input;
        private final int limit;
        private int current;

        LimitIterator(Iterator<? extends T> input, int limit) {
            this.input = input;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            return current < limit && input.hasNext();
        }

        @Override
        public T next() {
            if (current >= limit) {
                throw new NoSuchElementException();
            }
            ++current;
            return input.next();
        }
    }

    /**
     * Returns a list containing the elements of the provided {@code iterator}.
     */
    public static <T> List<T> toList(Iterator<T> iterator) {
        if (iterator.hasNext()) {
            var list = new ArrayList<T>();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            return Collections.unmodifiableList(list);
        }
        return Collections.emptyList();
    }

    public static <T, U> Iterator<U> flatMap(Iterator<? extends T> input, Function<T, Iterator<? extends U>> fn) {
        while (input.hasNext()) {
            final var value = fn.apply(input.next());
            if (value.hasNext()) {
                return new FlatMapIterator<>(input, fn, value);
            }
        }

        return Collections.emptyIterator();
    }

    private static final class FlatMapIterator<T, U> implements Iterator<U> {

        private final Iterator<? extends T> input;
        private final Function<T, Iterator<? extends U>> fn;

        @Nullable // if finished, otherwise currentOutput.hasNext() is true
        private Iterator<? extends U> currentOutput;

        FlatMapIterator(Iterator<? extends T> input, Function<T, Iterator<? extends U>> fn, Iterator<? extends U> firstOutput) {
            this.input = input;
            this.fn = fn;
            this.currentOutput = firstOutput;
        }

        @Override
        public boolean hasNext() {
            return currentOutput != null;
        }

        @Override
        public U next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            // noinspection ConstantConditions this is for documentation purposes
            assert currentOutput != null && currentOutput.hasNext();
            final U value = currentOutput.next();
            while (currentOutput != null && currentOutput.hasNext() == false) {
                if (input.hasNext()) {
                    currentOutput = fn.apply(input.next());
                } else {
                    currentOutput = null;
                }
            }
            return value;
        }
    }

    /**
     * Returns an iterator over the same items as the provided {@code input} except that it stops yielding items (i.e. starts returning
     * {@code false} from {@link Iterator#hasNext()} on failure.
     */
    public static <T> Iterator<T> failFast(Iterator<T> input, BooleanSupplier isFailingSupplier) {
        if (isFailingSupplier.getAsBoolean()) {
            return Collections.emptyIterator();
        } else {
            return new FailFastIterator<>(input, isFailingSupplier);
        }
    }

    private static class FailFastIterator<T> implements Iterator<T> {
        private final Iterator<T> delegate;
        private final BooleanSupplier isFailingSupplier;

        FailFastIterator(Iterator<T> delegate, BooleanSupplier isFailingSupplier) {
            this.delegate = delegate;
            this.isFailingSupplier = isFailingSupplier;
        }

        @Override
        public boolean hasNext() {
            return isFailingSupplier.getAsBoolean() == false && delegate.hasNext();
        }

        @Override
        public T next() {
            return delegate.next();
        }
    }

    /**
     * Enumerates the elements of an iterator together with their index, using a function to combine the pair together into the final items
     * produced by the iterator.
     * <p>
     * An example of its usage to enumerate a list of names together with their positional index in the list:
     * </p>
     * <pre><code>
     * Iterator&lt;String&gt; nameIterator = ...;
     * Iterator&lt;Tuple&lt;Integer, String&gt;&gt; enumeratedNames = Iterators.enumerate(nameIterator, Tuple::new);
     * enumeratedNames.forEachRemaining(tuple -> System.out.println("Index: " + t.v1() + ", Name: " + t.v2()));
     * </code></pre>
     *
     * @param input The iterator to wrap
     * @param fn A function that takes the index for an entry and the entry itself, returning an item that combines them together
     * @return An iterator that combines elements together with their indices in the underlying collection
     * @param <T> The object type contained in the original iterator
     * @param <U> The object type that results from combining the original entry with its index in the iterator
     */
    public static <T, U> Iterator<U> enumerate(Iterator<? extends T> input, BiFunction<Integer, T, ? extends U> fn) {
        return new EnumeratingIterator<>(Objects.requireNonNull(input), Objects.requireNonNull(fn));
    }

    private static class EnumeratingIterator<T, U> implements Iterator<U> {
        private final Iterator<? extends T> input;
        private final BiFunction<Integer, T, ? extends U> fn;

        private int idx = 0;

        EnumeratingIterator(Iterator<? extends T> input, BiFunction<Integer, T, ? extends U> fn) {
            this.input = input;
            this.fn = fn;
        }

        @Override
        public boolean hasNext() {
            return input.hasNext();
        }

        @Override
        public U next() {
            return fn.apply(idx++, input.next());
        }

        @Override
        public void forEachRemaining(Consumer<? super U> action) {
            input.forEachRemaining(t -> action.accept(fn.apply(idx++, t)));
        }
    }

    /**
     * Adapts a {@link Supplier} object into an iterator. The resulting iterator will return values from the delegate Supplier until the
     * delegate returns a <code>null</code> value. Once the delegate returns <code>null</code>, the iterator will claim to be empty.
     * <p>
     * An example of its usage to iterate over a queue while draining it at the same time:
     * </p>
     * <pre><code>
     *     LinkedList&lt;String&gt; names = ...;
     *     assert names.size() != 0;
     *
     *     Iterator&lt;String&gt; nameIterator = Iterator.fromSupplier(names::pollFirst);
     *     nameIterator.forEachRemaining(System.out::println)
     *     assert names.size() == 0;
     * </code></pre>
     *
     * @param input A {@link Supplier} that returns null when no more elements should be returned from the iterator
     * @return An iterator that returns elements by calling the supplier until a null value is returned
     * @param <T> The object type returned from the supplier function
     */
    public static <T> Iterator<T> fromSupplier(Supplier<? extends T> input) {
        return new SupplierIterator<>(Objects.requireNonNull(input));
    }

    private static final class SupplierIterator<T> implements Iterator<T> {
        private final Supplier<? extends T> fn;
        private T head;

        SupplierIterator(Supplier<? extends T> fn) {
            this.fn = fn;
            this.head = fn.get();
        }

        @Override
        public boolean hasNext() {
            return head != null;
        }

        @Override
        public T next() {
            if (head == null) {
                throw new NoSuchElementException();
            }
            T next = head;
            head = fn.get();
            return next;
        }
    }

    /**
     * Cycles infinitely over the elements in the {@code source} parameter
     */
    public static <T> Iterator<T> cycling(final Iterable<T> source) {
        return new CyclingIterator<>(Objects.requireNonNull(source));
    }

    private static class CyclingIterator<T> implements Iterator<T> {
        private final Iterable<T> source;
        private Iterator<T> iterator;

        CyclingIterator(Iterable<T> source) {
            this.source = source;
            this.iterator = source.iterator();
            if (iterator.hasNext() == false) {
                throw new IllegalArgumentException("Cannot cycle over empty iterable");
            }
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public T next() {
            if (iterator.hasNext()) {
                return iterator.next();
            }
            iterator = source.iterator();
            if (iterator.hasNext() == false) {
                throw new IllegalArgumentException("Cannot cycle over empty iterable");
            }
            return iterator.next();
        }
    }

    public static <T> boolean equals(Iterator<? extends T> iterator1, Iterator<? extends T> iterator2, BiPredicate<T, T> itemComparer) {
        if (iterator1 == null) {
            return iterator2 == null;
        }
        if (iterator2 == null) {
            return false;
        }

        while (iterator1.hasNext()) {
            if (iterator2.hasNext() == false) {
                return false;
            }

            if (itemComparer.test(iterator1.next(), iterator2.next()) == false) {
                return false;
            }
        }

        return iterator2.hasNext() == false;
    }

    public static <T> int hashCode(Iterator<? extends T> iterator, ToIntFunction<T> itemHashcode) {
        if (iterator == null) {
            return 0;
        }
        int result = 1;
        while (iterator.hasNext()) {
            result = 31 * result + itemHashcode.applyAsInt(iterator.next());
        }
        return result;
    }
}
