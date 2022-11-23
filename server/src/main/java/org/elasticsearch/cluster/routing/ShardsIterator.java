/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.util.Countable;

import java.util.List;

/**
 * Allows to iterate over unrelated shards.
 */
public interface ShardsIterator extends Iterable<ShardRouting>, Countable {

    /**
     * Resets the iterator to its initial state.
     */
    void reset();

    /**
     * The number of shard routing instances.
     *
     * @return number of shard routing instances in this iterator
     */
    int size();

    /**
     * The number of active shard routing instances
     *
     * @return number of active shard routing instances
     */
    int sizeActive();

    /**
     * Returns the next shard, or {@code null} if none available.
     */
    ShardRouting nextOrNull();

    /**
     * Return the number of shards remaining in this {@link ShardsIterator}
     *
     * @return number of shard remaining
     */
    int remaining();

    @Override
    int hashCode();

    @Override
    boolean equals(Object other);

    /**
     * Returns the {@link ShardRouting}s that this shards iterator holds.
     */
    List<ShardRouting> getShardRoutings();
}
