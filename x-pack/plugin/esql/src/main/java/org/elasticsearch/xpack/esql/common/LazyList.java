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
import java.util.function.IntFunction;
import java.util.function.IntSupplier;

// FIXME(gal, NOCOMMIT) Document
public abstract class LazyList<T> extends AbstractList<T> {
    public <S> LazyList<S> map(Function<T, S> mapper) {
        return new FromFunction<>(i -> mapper.apply(get(i)), this::size);
    }

    @SuppressWarnings("unchecked")
    private static final LazyList<?> EMPTY = new FromList<Object>(List.of());

    @SuppressWarnings("unchecked")
    public static <T> LazyList<T> empty() {
        return (LazyList<T>) EMPTY;
    }

    public static <T> LazyList<T> fromList(List<T> list) {
        return new FromList<>(list);
    }

    // FIXME(gal, NOCOMMIT) Should we add a cache?
    private static class FromFunction<T> extends LazyList<T> {
        private final IntFunction<T> getElement;
        private final IntSupplier getSize;

        FromFunction(IntFunction<T> getElement, IntSupplier getSize) {
            this.getElement = getElement;
            this.getSize = getSize;
        }

        @Override
        public T get(int index) {
            return getElement.apply(index);
        }

        @Override
        public int size() {
            return getSize.getAsInt();
        }

        @Override
        public <S> LazyList<S> map(Function<T, S> mapper) {
            return new FromFunction<>(i -> mapper.apply(get(i)), getSize);
        }

        @Override
        public String toString() {
            return "LazyList from function";
        }
    }

    private static class FromList<T> extends LazyList<T> {
        private final List<T> list;

        FromList(List<T> list) {
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
        public <S> LazyList<S> map(Function<T, S> mapper) {
            return new FromFunction<>(i -> mapper.apply(get(i)), this::size);
        }

        @Override
        public String toString() {
            return "LazyList{" + "list=" + list + '}';
        }
    }

    private static class FromListMapped<T, S> extends LazyList<S> {
        private final List<T> source;
        private final List<S> mapped = new ArrayList<>();
        private final Function<T, S> mapper;

        private FromListMapped(List<T> source, Function<T, S> mapper) {
            this.source = source;
            this.mapper = mapper;
        }

        @Override
        public S get(int index) {
            if (mapped.get(index) != null) {
                mapped.get(index);
            }
            if (source.size() <= index) {
                throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + source.size());
            }
            if (mapped.size() <= index) {
                for (int i = mapped.size(); i <= index; i++) {
                    mapped.add(null);
                }
            }
            S value = mapper.apply(source.get(index));
            mapped.set(index, value);
            return value;
        }

        @Override
        public int size() {
            return source.size();
        }

        @Override
        public <W> LazyList<W> map(Function<S, W> mapper) {
            return new FromListMapped<>(this, mapper);
        }

        @Override
        public String toString() {
            return "LazyListMapped{" + "list=" + source + '}';
        }
    }
}
