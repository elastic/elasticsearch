/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class RecoveryTarget extends AbstractRefCounted {

    protected final Logger logger;
    private static final AtomicLong idGenerator = new AtomicLong();


    private final ShardId shardId;
    private final long recoveryId;
    private final IndexShard indexShard;
    private final DiscoveryNode sourceNode;
    private final PeerRecoveryTargetService.RecoveryListener listener;
    private final AtomicBoolean finished = new AtomicBoolean();

    private final CancellableThreads cancellableThreads;

    // last time this status was accessed
    private volatile long lastAccessTime = System.nanoTime();

    // latch that can be used to blockingly wait for RecoveryTarget to be closed
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard local shard where we want to recover to
     * @param sourceNode source node of the recovery where we recover from
     * @param listener   called when recovery is completed/failed
     */
    public RecoveryTarget(final IndexShard indexShard,
                          final DiscoveryNode sourceNode,
                          final PeerRecoveryTargetService.RecoveryListener listener) {
        super("recovery_target");
        this.cancellableThreads = new CancellableThreads();
        this.recoveryId = idGenerator.incrementAndGet();
        this.listener = listener;
        this.logger = Loggers.getLogger(getClass(), indexShard.indexSettings().getSettings(),
            indexShard.shardId());
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.shardId = indexShard.shardId();
        indexShard.recoveryStats().incCurrentAsTarget();
    }

    public long recoveryId() {
        return recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public RecoveryState state() {
        return indexShard.recoveryState();
    }

    public CancellableThreads cancellableThreads() {
        return cancellableThreads;
    }

    protected PeerRecoveryTargetService.RecoveryListener listener() {
        return null;
    }

    /** return the last time this RecoveryStatus was used (based on System.nanoTime() */
    public long lastAccessTime() {
        return lastAccessTime;
    }

    /** sets the lasAccessTime flag to now */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    protected void ensureRefCount() {
        if (refCount() <= 0) {
            throw new ElasticsearchException("RecoveryTarget is used but it's refcount is 0. " +
                "Probably a mismatch between incRef/decRef calls");
        }
    }

    /**
     * Returns a fresh recovery target to retry recovery from the same source node onto the same shard and using the same listener.
     *
     * @return a copy of this recovery target
     */
    public abstract RecoveryTarget retryCopy();

    protected void doClose() {}

    @Override
    final protected void closeInternal() {
        try {
            doClose();
        } finally {
            indexShard.recoveryStats().decCurrentAsTarget();
            closedLatch.countDown();
        }
    }

    @Override
    public String toString() {
        return shardId + " [" + recoveryId + "][" + getClass().getName() + "]";
    }

    public void notifyListener(RecoveryFailedException e, boolean sendShardFailure) {
        listener.onRecoveryFailure(state(), e, sendShardFailure);
    }

    /** mark the current recovery as done */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            assert assertOnDone();
            try {
                // this might still throw an exception ie. if the shard is CLOSED due to some other event.
                // it's safer to decrement the reference in a try finally here.
                indexShard.postRecovery("peer recovery done");
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onRecoveryDone(state());
        }
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     * <p>
     * if {@link #cancellableThreads()} was used, the threads will be interrupted.
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
                cancellableThreads.cancel(reason);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }

    protected boolean assertOnDone() { return true;}

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(RecoveryFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                notifyListener(e, sendShardFailure);
            } finally {
                try {
                    cancellableThreads.cancel("failed recovery [" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                    // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    /**
     * Closes the current recovery target and waits up to a certain timeout for resources to be freed.
     * Returns true if resetting the recovery was successful, false if the recovery target is already cancelled / failed or marked as done.
     */
    final boolean resetRecovery(CancellableThreads newTargetCancellableThreads) throws IOException {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("reset of recovery with shard {} and id [{}]", shardId, recoveryId);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now.
                decRef();
            }
            try {
                newTargetCancellableThreads.execute(closedLatch::await);
            } catch (CancellableThreads.ExecutionCancelledException e) {
                logger.trace("new recovery target cancelled for shard {} while waiting on old recovery target with id [{}] to close",
                    shardId, recoveryId);
                return false;
            }
            onResetRecovery();
            return true;
        }
        return false;
    }

    protected void onResetRecovery() throws IOException {

    }

    public abstract String startRecoveryActionName();

    public abstract StartRecoveryRequest createStartRecoveryRequest(Logger logger,
                                                                    DiscoveryNode localNode) throws IOException;

    public RecoveryResponse createRecoveryResponse() {
        return new RecoveryResponse();
    }
}
