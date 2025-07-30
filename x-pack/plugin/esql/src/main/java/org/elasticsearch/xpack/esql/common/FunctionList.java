/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A special {@link List} wrapper which can mapped lazily, i.e., it can apply a function to future elements (as opposed to
 * {@code stream.map()}, which only maps the current list elements). While the entire point of this class is that it assumes the underlying
 * list can add new elements, it does actually assume that this is the *only* source of change to the list, since it caches the values it
 * maps.
 */
public abstract class FunctionList<T> extends AbstractList<T> {
    public abstract <S> FunctionList<S> map(Function<T, S> mapper);

    @SuppressWarnings("unchecked")
    private static final FunctionList<?> EMPTY = new FromImmutableList<>(List.of());

    @SuppressWarnings("unchecked")
    public static <T> FunctionList<T> empty() {
        return (FunctionList<T>) EMPTY;
    }

    /**
     * Note that while the resulting mapped list is synchronized, the underlying list is not necessarily so, so if it is expected to be
     * modified, it should be synchronized as well.
     */
    public static <T> FunctionList<T> fromMutableList(List<T> list) {
        return new FromMutableList<>(list);
    }

    private static class FromMutableList<T> extends FunctionList<T> {
        private final List<T> list;

        FromMutableList(List<T> list) {
            this.list = list;
        }

        @Override
        public T get(int index) {
            return list.get(index);
        }

        @Override
        public int size() {
            return list.size();
        }

        @Override
        public <S> FunctionList<S> map(Function<T, S> mapper) {
            return new FromListMapped<>(this, mapper);
        }
    }

    private static class FromListMapped<T, S> extends FunctionList<S> {
        private final List<T> source;
        private final List<S> mapped = new ArrayList<>();
        private final Function<T, S> mapper;

        private FromListMapped(List<T> source, Function<T, S> mapper) {
            this.source = source;
            this.mapper = mapper;
        }

        @Override
        public S get(int index) {
            if (source.size() <= index) {
                throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + source.size());
            }
            synchronized (this) {
                if (mapped.size() > index && mapped.get(index) != null) {
                    return mapped.get(index);
                }

                if (mapped.size() > index) {
                    var result = mapped.get(index);
                    return result != null ? result : getNewValue(index);
                }

                for (int i = mapped.size(); i <= index; i++) {
                    mapped.add((S) null);
                }
                return getNewValue(index);
            }
        }

        private synchronized S getNewValue(int index) {
            var value = mapper.apply(source.get(index));
            if (value == null) {
                throw new NullPointerException("Mapper returned null for index: " + index);
            }
            mapped.set(index, value);
            return value;
        }

        @Override
        public int size() {
            return source.size();
        }

        @Override
        public <U> FunctionList<U> map(Function<S, U> mapper) {
            return new FromListMapped<>(this, mapper);
        }

        @Override
        public String toString() {
            // Since the super's toString() method calls get(index) for each index, we want to avoid the computation of all the elements.
            return "LazyListMapped{" + "list=" + source + '}';
        }
    }

    /** If the list is known to be immutable, we can forgo all the ceremony of applying the function lazily. */
    public static <T> FunctionList<T> fromImmutableList(List<T> list) {
        return new FromImmutableList<>(list);
    }

    private static class FromImmutableList<T> extends FunctionList<T> {
        private final List<T> list;

        FromImmutableList(List<T> list) {
            this.list = list;
        }

        @Override
        public T get(int index) {
            return list.get(index);
        }

        @Override
        public int size() {
            return list.size();
        }

        public <S> FunctionList<S> map(Function<T, S> mapper) {
            return fromImmutableList(list.stream().map(mapper).toList());
        }
    }
}
