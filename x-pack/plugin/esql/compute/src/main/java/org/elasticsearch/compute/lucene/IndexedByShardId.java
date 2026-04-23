/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.compute.lucene.query.LuceneQueryEvaluator;

import java.util.function.Function;

/**
 * Ceci n'est pas une List (though it may be backed by one).
 * Using this interface instead of a list makes it explicit that the values are not necessarily continuous by index, especially after the
 * reduce-side top n operation has been run, or it can be sliced up into groups, e.g., 0..10, 10..20, etc., as done in
 * DataNodeComputeHandler and with the help of the ComputeSearchContextByShardId subclass, which is the main production implementation.
 *
 * When you see this class, it will usually be parameterized by {@link ShardContext}, its super classes, or one of its variants,
 * e.g., {@link LuceneQueryEvaluator.ShardConfig}.
 * These shard IDs are sliced up by DataNodeComputeHandler, and depend on the MAX_CONCURRENT_SHARDS_PER_NODE setting.
 */
public interface IndexedByShardId<T> {
    T get(int shardId);

    /**
     * This is not necessarily an iterable of all values visible via get(int), but rather, an iterable of the relevant values.
     * This is useful when you need to perform an operation over all relevant values, e.g., closing them.
     */
    Iterable<? extends T> iterable();

    /** The number of elements returned by {@link IndexedByShardId#iterable()}. */
    int size();

    default boolean isEmpty() {
        return iterable().iterator().hasNext() == false;
    }

    /**
     * The elements are mapped lazily, i.e., the function would also apply to future elements (as opposed to
     * {@code collection().stream().map}, which only maps the current elements).
     */
    <S> IndexedByShardId<S> map(Function<T, S> mapper);
}
