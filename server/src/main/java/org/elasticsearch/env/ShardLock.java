/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.env;

import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A shard lock guarantees exclusive access to a shards data
 * directory. Internal processes should acquire a lock on a shard
 * before executing any write operations on the shards data directory.
 *
 * @see NodeEnvironment
 */
public abstract class ShardLock implements Closeable {

    private final ShardId shardId;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ShardLock(ShardId id) {
        this.shardId = id;
    }

    /**
     * Returns the locks shards Id.
     */
    public final ShardId getShardId() {
        return shardId;
    }

    @Override
    public final void close() {
        if (this.closed.compareAndSet(false, true)) {
            closeInternal();
        }
    }

    protected abstract void closeInternal();

    /**
     * Update the details of the holder of this lock. These details are displayed alongside a {@link ShardLockObtainFailedException}. Must
     * only be called by the holder of this lock.
     */
    public void setDetails(String details) {}

    @Override
    public String toString() {
        return "ShardLock{" + "shardId=" + shardId + '}';
    }

}
