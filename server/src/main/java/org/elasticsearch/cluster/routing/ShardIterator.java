/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.index.shard.ShardId;

/**
 * Allows to iterate over a set of shard instances (routing) within a shard id group.
 */
public interface ShardIterator extends ShardsIterator, Comparable<ShardIterator> {

    /**
     * The shard id this group relates to.
     */
    ShardId shardId();

    /**
     * Resets the iterator.
     */
    @Override
    void reset();
}
