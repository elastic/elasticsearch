/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * An implementation backed by an array. We use an array, instead of the much more natural ArrayList, to make explicit the assumption that
 * the total number of shards is known in advance (even though, due to failure handling and such, we might not allocate all the elements).
 *
 * This class is both mutable and unsynchronized, and is thus not thread-safe. However, if the only modification to the underlying array is
 * adding immutable elements via the {@link #add} method (which probably <i>should</i> be synchronized, but that's up to the calling code),
 * it <i>should</i> be fine.
 */
public final class IndexedByShardIdFromArray<T> extends IndexedByShardId<T> {
    private final T[] array;
    private int currentIndex;

    public IndexedByShardIdFromArray(T[] array) {
        this.array = array;
        this.currentIndex = 0;
    }

    public void add(T t) {
        array[currentIndex++] = t;
    }

    @Override
    public T get(int shardId) {
        var result = array[shardId];
        if (result == null) {
            throw new IndexOutOfBoundsException("shardId " + shardId + " out of bounds [0, " + currentIndex + ")");
        }
        return result;
    }

    @Override
    public Collection<? extends T> collection() {
        return Arrays.asList(array).subList(0, currentIndex);
    }

    @Override
    public <S> IndexedByShardId<S> map(Function<T, S> mapper) {
        return new Mapped<>(this, array.length, 0, mapper);
    }

    public IndexedByShardId<T> subRange(int fromIndex, int toIndex) {
        return new SubRanged<>(array, fromIndex, toIndex);
    }

    public int length() {
        return currentIndex;
    }

    private static class SubRanged<T> extends IndexedByShardId<T> {
        private final T[] array;
        private final int from;
        private final int to;

        SubRanged(T[] array, int from, int to) {
            this.array = array;
            this.from = from;
            this.to = to;
        }

        @Override
        public T get(int shardId) {
            if (shardId < from || shardId >= to) {
                throw new IndexOutOfBoundsException("shardId " + shardId + " out of bounds [" + from + ", " + to + ")");
            }
            return array[shardId];
        }

        @Override
        public Collection<? extends T> collection() {
            return Arrays.asList(array).subList(from, to);
        }

        @Override
        public <S> IndexedByShardId<S> map(Function<T, S> mapper) {
            return new Mapped<>(this, to - from, from, mapper);
        }
    }

    private static class Mapped<T, S> extends IndexedByShardId<S> {
        private final IndexedByShardId<T> original;
        private final S[] cache;
        private final int offset;
        private final Function<T, S> mapper;

        @SuppressWarnings("unchecked")
        Mapped(IndexedByShardId<T> original, int size, int offset, Function<T, S> mapper) {
            this.original = original;
            this.mapper = mapper;
            this.cache = (S[]) new Object[size];
            this.offset = offset;
        }

        @Override
        public S get(int shardId) {
            var fixedShardId = shardId - offset;
            if (cache[fixedShardId] == null) {
                synchronized (this) {
                    if (cache[fixedShardId] == null) {
                        cache[fixedShardId] = mapper.apply(original.get(shardId));
                    }
                }
            }
            return cache[fixedShardId];
        }

        @Override
        public Collection<? extends S> collection() {
            return IntStream.range(offset, original.collection().size() + offset).mapToObj(this::get).toList();
        }

        @Override
        public <U> IndexedByShardId<U> map(Function<S, U> anotherMapper) {
            return new Mapped<>(this, cache.length, offset, anotherMapper);
        }
    }
}
