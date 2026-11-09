/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * References indexed by shard id over a map: for an operator that meets documents from several sources, each with
 * references of its own over the same request-wide shard ids, and keeps the ones it pinned by id. The map is the
 * caller's and may grow after this view is taken.
 */
public final class IndexedByShardIdFromMap<T> implements IndexedByShardId<T> {
    private final Map<Integer, T> byShard;

    public IndexedByShardIdFromMap(Map<Integer, T> byShard) {
        this.byShard = byShard;
    }

    @Override
    public T get(int shardId) {
        T value = byShard.get(shardId);
        if (value == null) {
            throw new IllegalArgumentException("no reference for shard [" + shardId + "]");
        }
        return value;
    }

    @Override
    public Iterable<? extends T> iterable() {
        return byShard.values();
    }

    @Override
    public int size() {
        return byShard.size();
    }

    /**
     * Maps lazily, as the contract asks: a view mapped before the map grew still sees the entries added after.
     */
    @Override
    public <S> IndexedByShardId<S> map(Function<T, S> mapper) {
        IndexedByShardIdFromMap<T> source = this;
        return new IndexedByShardId<>() {
            @Override
            public S get(int shardId) {
                return mapper.apply(source.get(shardId));
            }

            @Override
            public Iterable<? extends S> iterable() {
                return () -> {
                    Iterator<? extends T> values = source.iterable().iterator();
                    return new Iterator<S>() {
                        @Override
                        public boolean hasNext() {
                            return values.hasNext();
                        }

                        @Override
                        public S next() {
                            return mapper.apply(values.next());
                        }
                    };
                };
            }

            @Override
            public int size() {
                return source.size();
            }

            @Override
            public <U> IndexedByShardId<U> map(Function<S, U> next) {
                return source.map(mapper.andThen(next));
            }
        };
    }
}
