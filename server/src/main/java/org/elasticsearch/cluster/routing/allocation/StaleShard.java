/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.index.shard.ShardId;

/**
 * A class that represents a stale shard copy.
 */
public class StaleShard {
    private final ShardId shardId;
    private final String allocationId;

    public StaleShard(ShardId shardId, String allocationId) {
        this.shardId = shardId;
        this.allocationId = allocationId;
    }

    @Override
    public String toString() {
        return "stale shard, shard " + shardId + ", alloc. id [" + allocationId + "]";
    }

    /**
     * The shard id of the stale shard.
     */
    public ShardId getShardId() {
        return shardId;
    }

    /**
     * The allocation id of the stale shard.
     */
    public String getAllocationId() {
        return allocationId;
    }
}
