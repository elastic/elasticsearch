/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import java.util.Collection;
import java.util.function.Function;

public class EmptyIndexedByShardId {
    @SuppressWarnings("unchecked")
    public static <T> IndexedByShardId<T> instance() {
        return (IndexedByShardId<T>) EMPTY;
    }

    private static IndexedByShardId<?> EMPTY = new IndexedByShardId<>() {
        @Override
        public Object get(int shardId) {
            throw new IndexOutOfBoundsException("no shards");
        }

        @Override
        public Collection<?> collection() {
            return java.util.List.of();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <S> IndexedByShardId<S> map(Function<Object, S> mapper) {
            return (IndexedByShardId<S>) this;
        }
    };
}
