/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.core.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

public class Iterators {

    /**
     * Returns a single element iterator over the supplied value.
     */
    public static <T> Iterator<T> single(T element) {
        return new Iterator<>() {

            private T value = Objects.requireNonNull(element);

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
        };
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
                return new ConcatenatedIterator<T>(iterators, i);
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

    public static <T, U> Iterator<U> zip(Iterator<? extends T> input, BiFunction<Integer, T, ? extends U> fn) {
        return new ZipIterator<>(Objects.requireNonNull(input), Objects.requireNonNull(fn));
    }

    private static class ZipIterator<T, U> implements Iterator<U> {
        private final Iterator<? extends T> input;
        private final BiFunction<Integer, T, ? extends U> fn;

        private int idx = 0;

        public ZipIterator(Iterator<? extends T> input, BiFunction<Integer, T, ? extends U> fn) {
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

    public static <T> Iterator<T> fromSupplier(Supplier<? extends T> input) {
        return new SupplierIterator<>(Objects.requireNonNull(input));
    }

    private static final class SupplierIterator<T> implements Iterator<T> {
        private final Supplier<? extends T> fn;
        private T head;

        public SupplierIterator(Supplier<? extends T> fn) {
            this.fn = fn;
            this.head = fn.get();
        }

        @Override
        public boolean hasNext() {
            return head != null;
        }

        @Override
        public T next() {
            T next = head;
            head = fn.get();
            return next;
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
