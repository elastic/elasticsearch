/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.core.RefCounted;

import java.util.List;

/** Manages reference counting for {@link ShardContext}. */
public interface ShardRefCounted {
    RefCounted get(int shardId);

    static ShardRefCounted fromList(List<? extends RefCounted> refCounters) {
        return shardId -> refCounters.get(shardId);
    }

    static ShardRefCounted fromShardContext(ShardContext shardContext) {
        return single(shardContext.index(), shardContext);
    }

    static ShardRefCounted single(int index, RefCounted refCounted) {
        return shardId -> {
            if (shardId != index) {
                throw new IllegalArgumentException("Invalid shardId: " + shardId + ", expected: " + index);
            }
            return refCounted;
        };
    }

    ShardRefCounted ALWAYS_REFERENCED = shardId -> RefCounted.ALWAYS_REFERENCED;
}
