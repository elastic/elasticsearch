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

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * This class holds a collection of all on going recoveries on the current node (i.e., the node is the target node
 * of those recoveries). The class is used to guarantee concurrent semantics such that once a recoveries was done/cancelled/failed
 * no other thread will be able to find it. Last, the {@link RecoveryRef} inner class verifies that recovery temporary files
 * and store will only be cleared once on going usage is finished.
 */
public class RecoveriesCollection {

    /** This is the single source of truth for ongoing recoveries. If it's not here, it was canceled or done */
    private final ConcurrentMap<Long, RecoveryTarget> onGoingRecoveries = ConcurrentCollections.newConcurrentMap();

    final private ESLogger logger;
    final private ThreadPool threadPool;

    public RecoveriesCollection(ESLogger logger, ThreadPool threadPool) {
        this.logger = logger;
        this.threadPool = threadPool;
    }

    /**
     * Starts are new recovery for the given shard, source node and state
     *
     * @return the id of the new recovery.
     */
    public long startRecovery(IndexShard indexShard, DiscoveryNode sourceNode,
                              RecoveryTargetService.RecoveryListener listener, TimeValue activityTimeout) {
        RecoveryTarget status = new RecoveryTarget(indexShard, sourceNode, listener);
        RecoveryTarget existingStatus = onGoingRecoveries.putIfAbsent(status.recoveryId(), status);
        assert existingStatus == null : "found two RecoveryStatus instances with the same id";
        logger.trace("{} started recovery from {}, id [{}]", indexShard.shardId(), sourceNode, status.recoveryId());
        threadPool.schedule(activityTimeout, ThreadPool.Names.GENERIC,
                new RecoveryMonitor(status.recoveryId(), status.lastAccessTime(), activityTimeout));
        return status.recoveryId();
    }

    /**
     * gets the {@link RecoveryTarget } for a given id. The RecoveryStatus returned has it's ref count already incremented
     * to make sure it's safe to use. However, you must call {@link RecoveryTarget#decRef()} when you are done with it, typically
     * by using this method in a try-with-resources clause.
     * <p>
     * Returns null if recovery is not found
     */
    public RecoveryRef getRecovery(long id) {
        RecoveryTarget status = onGoingRecoveries.get(id);
        if (status != null && status.tryIncRef()) {
            return new RecoveryRef(status);
        }
        return null;
    }

    /** Similar to {@link #getRecovery(long)} but throws an exception if no recovery is found */
    public RecoveryRef getRecoverySafe(long id, ShardId shardId) {
        RecoveryRef recoveryRef = getRecovery(id);
        if (recoveryRef == null) {
            throw new IndexShardClosedException(shardId);
        }
        assert recoveryRef.status().shardId().equals(shardId);
        return recoveryRef;
    }

    /** cancel the recovery with the given id (if found) and remove it from the recovery collection */
    public boolean cancelRecovery(long id, String reason) {
        RecoveryTarget removed = onGoingRecoveries.remove(id);
        boolean cancelled = false;
        if (removed != null) {
            logger.trace("{} canceled recovery from {}, id [{}] (reason [{}])",
                    removed.shardId(), removed.sourceNode(), removed.recoveryId(), reason);
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * fail the recovery with the given id (if found) and remove it from the recovery collection
     *
     * @param id               id of the recovery to fail
     * @param e                exception with reason for the failure
     * @param sendShardFailure true a shard failed message should be sent to the master
     */
    public void failRecovery(long id, RecoveryFailedException e, boolean sendShardFailure) {
        RecoveryTarget removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} failing recovery from {}, id [{}]. Send shard failure: [{}]", removed.shardId(), removed.sourceNode(), removed.recoveryId(), sendShardFailure);
            removed.fail(e, sendShardFailure);
        }
    }

    /** mark the recovery with the given id as done (if found) */
    public void markRecoveryAsDone(long id) {
        RecoveryTarget removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} marking recovery from {} as done, id [{}]", removed.shardId(), removed.sourceNode(), removed.recoveryId());
            removed.markAsDone();
        }
    }

    /** the number of ongoing recoveries */
    public int size() {
        return onGoingRecoveries.size();
    }

    /**
     * cancel all ongoing recoveries for the given shard
     *
     * @param reason       reason for cancellation
     * @param shardId      shardId for which to cancel recoveries
     * @return true if a recovery was cancelled
     */
    public boolean cancelRecoveriesForShard(ShardId shardId, String reason) {
        boolean cancelled = false;
        for (RecoveryTarget status : onGoingRecoveries.values()) {
            if (status.shardId().equals(shardId)) {
                cancelled |= cancelRecovery(status.recoveryId(), reason);
            }
        }
        return cancelled;
    }


    /**
     * a reference to {@link RecoveryTarget}, which implements {@link AutoCloseable}. closing the reference
     * causes {@link RecoveryTarget#decRef()} to be called. This makes sure that the underlying resources
     * will not be freed until {@link RecoveryRef#close()} is called.
     */
    public static class RecoveryRef implements AutoCloseable {

        private final RecoveryTarget status;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        /**
         * Important: {@link RecoveryTarget#tryIncRef()} should
         * be *successfully* called on status before
         */
        public RecoveryRef(RecoveryTarget status) {
            this.status = status;
            this.status.setLastAccessTime();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                status.decRef();
            }
        }

        public RecoveryTarget status() {
            return status;
        }
    }

    private class RecoveryMonitor extends AbstractRunnable {
        private final long recoveryId;
        private final TimeValue checkInterval;

        private long lastSeenAccessTime;

        private RecoveryMonitor(long recoveryId, long lastSeenAccessTime, TimeValue checkInterval) {
            this.recoveryId = recoveryId;
            this.checkInterval = checkInterval;
            this.lastSeenAccessTime = lastSeenAccessTime;
        }

        @Override
        public void onFailure(Throwable t) {
            logger.error("unexpected error while monitoring recovery [{}]", t, recoveryId);
        }

        @Override
        protected void doRun() throws Exception {
            RecoveryTarget status = onGoingRecoveries.get(recoveryId);
            if (status == null) {
                logger.trace("[monitor] no status found for [{}], shutting down", recoveryId);
                return;
            }
            long accessTime = status.lastAccessTime();
            if (accessTime == lastSeenAccessTime) {
                String message = "no activity after [" + checkInterval + "]";
                failRecovery(recoveryId,
                        new RecoveryFailedException(status.state(), message, new ElasticsearchTimeoutException(message)),
                        true // to be safe, we don't know what go stuck
                );
                return;
            }
            lastSeenAccessTime = accessTime;
            logger.trace("[monitor] rescheduling check for [{}]. last access time is [{}]", recoveryId, lastSeenAccessTime);
            threadPool.schedule(checkInterval, ThreadPool.Names.GENERIC, this);
        }
    }

}

