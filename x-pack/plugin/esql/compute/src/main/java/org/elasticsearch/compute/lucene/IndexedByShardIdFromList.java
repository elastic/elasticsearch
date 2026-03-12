/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import java.util.List;

/** An {@link IndexedByShardId} used by only in tests, but placed here so it's visible for all test modules. */
public class IndexedByShardIdFromList<T> implements IndexedByShardId<T> {
    private final List<T> list;

    public IndexedByShardIdFromList(List<T> list) {
        this.list = list;
    }

    @Override
    public T get(int shardId) {
        return list.get(shardId);
    }

    @Override
    public Iterable<? extends T> iterable() {
        return list;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public <S> IndexedByShardId<S> map(java.util.function.Function<T, S> mapper) {
        return new IndexedByShardIdFromList<>(list.stream().map(mapper).toList());
    }
}
