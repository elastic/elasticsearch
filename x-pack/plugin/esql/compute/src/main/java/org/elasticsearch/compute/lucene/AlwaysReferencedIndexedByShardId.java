/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.core.RefCounted;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * An implementation which always returns {@link RefCounted#ALWAYS_REFERENCED} for any shard ID. Used by tests, but defined here so it could
 * also be used by the benchmarks.
 */
public class AlwaysReferencedIndexedByShardId implements IndexedByShardId<RefCounted> {
    public static final AlwaysReferencedIndexedByShardId INSTANCE = new AlwaysReferencedIndexedByShardId();

    private AlwaysReferencedIndexedByShardId() {}

    @Override
    public RefCounted get(int shardId) {
        return RefCounted.ALWAYS_REFERENCED;
    }

    @Override
    public Collection<? extends RefCounted> collection() {
        return List.of(RefCounted.ALWAYS_REFERENCED);
    }

    @Override
    public <S> IndexedByShardId<S> map(Function<RefCounted, S> mapper) {
        throw new UnsupportedOperationException();
    }
}
