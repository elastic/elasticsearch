/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * An implementation backed by an array. We use an array, instead of the much more natural ArrayList, to make explicit the assumption that
 * the total number of shards is known in advance (even though, due to failure handling and such, we might not allocate all the elements).
 *<br>
 * This class is mutable and "mostly" {@code synchronized}; the only unsynchronized part is the {@link #get} method, which uses
 * {@link VarHandle} to ensure safe publication of the elements once added so it <i>should</i> be fine 🐶🔥.
 */
class ComputeSearchContextByShardId implements IndexedByShardId<ComputeSearchContext>, Releasable {
    private static final VarHandle HANDLE = MethodHandles.arrayElementVarHandle(Object[].class);
    private final ComputeSearchContext[] array;
    private int nextAddIndex = 0;

    ComputeSearchContextByShardId(int size) {
        this.array = new ComputeSearchContext[size];
    }

    public synchronized void add(ComputeSearchContext cse) {
        HANDLE.setRelease(array, nextAddIndex++, cse);
    }

    @Override
    public ComputeSearchContext get(int shardId) {
        // Since the main instance is shared between the node-reduce and data drivers, it's critial to use getAcquire here to ensure that
        // any thread reading the element after it's been added sees a fully constructed object.
        var result = HANDLE.getAcquire(array, shardId);
        if (result == null) {
            throw new IndexOutOfBoundsException("shardId " + shardId + " out of bounds [0, " + nextAddIndex + ")");
        }
        return (ComputeSearchContext) result;
    }

    public synchronized boolean isEmpty() {
        return nextAddIndex == 0;
    }

    @Override
    public synchronized Collection<ComputeSearchContext> collection() {
        return Arrays.asList(array).subList(0, nextAddIndex);
    }

    @Override
    public <S> IndexedByShardId<S> map(Function<ComputeSearchContext, S> mapper) {
        return new Mapped<>(this, array.length, 0, mapper);
    }

    /**
     * Returns a view of this instance containing only the elements from {@code fromIndex} (inclusive) to {@code toIndex} (exclusive).
     * This has consequences:
     * <ol>
     * <li>{@link #get} fails if the index is out of range.</li>
     * <li>{@link #collection} only returns the elements in the specified range.</li>
     * <li>{@link #map} creates a cache in the specified range size.</li>
     * </ol>
     */
    public synchronized IndexedByShardId<ComputeSearchContext> subRange(int fromIndex, int toIndex) {
        if (fromIndex < 0 || toIndex > nextAddIndex || fromIndex > toIndex) {
            throw new IndexOutOfBoundsException("Invalid subrange: [" + fromIndex + ", " + toIndex + ") in [0, " + nextAddIndex + ")");
        }
        return new SubRanged<>(array, fromIndex, toIndex);
    }

    public synchronized int length() {
        return nextAddIndex;
    }

    @Override
    public synchronized void close() {
        Releasables.close(collection());
    }

    // This class doesn't need synchronization since it's created from a synchronized context and it doesn't read past its initial bounds.
    private static class SubRanged<T> implements IndexedByShardId<T> {
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

    // This class doesn't need to be synchronized since it delegates to the underlying IndexedByShardId, which is assumed to be thread-safe.
    private static class Mapped<T, S> implements IndexedByShardId<S> {
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
