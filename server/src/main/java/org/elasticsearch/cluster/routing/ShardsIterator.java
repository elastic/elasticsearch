/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.routing;

import java.util.List;

/**
 * Allows to iterate over unrelated shards.
 */
public interface ShardsIterator extends Iterable<ShardRouting> {

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

    @Override
    int hashCode();

    @Override
    boolean equals(Object other);

    /**
     * Returns the {@link ShardRouting}s that this shards iterator holds.
     */
    List<ShardRouting> getShardRoutings();
}
