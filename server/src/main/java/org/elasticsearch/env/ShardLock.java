/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.env;

import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides exclusive access to a shard's data directory through a locking mechanism.
 * <p>
 * A shard lock guarantees that only one process can access a shard's data directory
 * at a time, preventing concurrent modifications that could corrupt the shard data.
 * Internal processes must acquire a lock on a shard before executing any write
 * operations on the shard's data directory.
 * <p>
 * This lock is {@link Closeable} and should be used with try-with-resources to
 * ensure proper release.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * ShardId shardId = new ShardId("myindex", "_na_", 0);
 * try (ShardLock lock = nodeEnvironment.shardLock(shardId, "operation description")) {
 *     // perform write operations on shard data directory
 *     modifyShardData(shardId);
 * } // lock automatically released
 * }</pre>
 *
 * @see NodeEnvironment
 */
public abstract class ShardLock implements Closeable {

    private final ShardId shardId;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Constructs a shard lock for the specified shard ID.
     *
     * @param id the shard identifier for this lock
     */
    public ShardLock(ShardId id) {
        this.shardId = id;
    }

    /**
     * Returns the shard ID protected by this lock.
     *
     * @return the shard identifier
     */
    public final ShardId getShardId() {
        return shardId;
    }

    /**
     * Releases the shard lock.
     * <p>
     * This method is idempotent; calling it multiple times has no additional effect
     * beyond the first call. The actual lock release logic is delegated to
     * {@link #closeInternal()}.
     */
    @Override
    public final void close() {
        if (this.closed.compareAndSet(false, true)) {
            closeInternal();
        }
    }

    /**
     * Internal method to release the lock.
     * <p>
     * Subclasses must implement this method to provide the actual lock release logic.
     */
    protected abstract void closeInternal();

    /**
     * Updates the details of the current holder of this lock.
     * <p>
     * These details are displayed in {@link ShardLockObtainFailedException} when another
     * process attempts to acquire the lock. This helps diagnose which operation is
     * holding the lock.
     * <p>
     * This method must only be called by the current holder of this lock.
     *
     * @param details a description of the operation holding the lock
     */
    public void setDetails(String details) {}

    @Override
    public String toString() {
        return "ShardLock{shardId=" + shardId + '}';
    }

}
