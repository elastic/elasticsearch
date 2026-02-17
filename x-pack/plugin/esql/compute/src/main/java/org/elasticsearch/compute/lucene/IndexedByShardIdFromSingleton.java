/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import java.util.List;

/** A simple implementation when there's only a single value being used. */
public class IndexedByShardIdFromSingleton<T> implements IndexedByShardId<T> {
    private final T value;
    private final int shardId;

    public IndexedByShardIdFromSingleton(T value) {
        this(value, 0);
    }

    public IndexedByShardIdFromSingleton(T value, int shardId) {
        this.value = value;
        this.shardId = shardId;
    }

    @Override
    public T get(int shardId) {
        if (shardId != this.shardId) {
            throw new IndexOutOfBoundsException("shardId should be 0 for a singleton, got: " + shardId);
        }
        return value;
    }

    @Override
    public Iterable<? extends T> iterable() {
        return List.of(value);
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public <S> IndexedByShardId<S> map(java.util.function.Function<T, S> mapper) {
        return new IndexedByShardIdFromSingleton<>(mapper.apply(value), shardId);
    }
}
