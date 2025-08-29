/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import java.util.Collection;
import java.util.function.Function;

/**
 * Ceci n'est pas une List (though it may be backed by one).
 * Using this interface instead of a list makes it explicit that the values are not necessarily continuous by index, especially after the
 * reduce-side top n operation has been run.
 */
public abstract class IndexedByShardId<T> {
    public abstract T get(int shardId);

    /**
     * This is not necessarily a list of all values visible via get(int), but rather, a list of the relevant values.
     * This is useful when you need to perform an operation over all relevant values, e.g., closing them.
     */
    public abstract Collection<? extends T> collection();

    public boolean isEmpty() {
        return collection().isEmpty();
    }

    /**
     * The elements are mapped lazily, i.e., the function would also apply to future elements (as opposed to
     * {@code collection().stream().map}, which only maps the current elements).
     */
    public abstract <S> IndexedByShardId<S> map(Function<T, S> mapper);

    @SuppressWarnings("unchecked")
    public static <T> IndexedByShardId<T> empty() {
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
