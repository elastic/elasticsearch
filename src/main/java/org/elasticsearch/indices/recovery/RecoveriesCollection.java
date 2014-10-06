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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class holds a collection of all on going recoveries on the current node (i.e., the node is the target node
 * of those recoveries). The class is used to guarantee concurrent semantics such that once a recoveries was done/cancelled/failed
 * no other thread will be able to find it. Last, the {@link StatusRef} inner class verifies that recovery temporary files
 * and store will only be cleared once on going usage is finished.
 */
public class RecoveriesCollection {

    /** This is the single source of truth for ongoing recoveries. If it's not here, it was canceled or done */
    private final ConcurrentMap<Long, RecoveryStatus> onGoingRecoveries = ConcurrentCollections.newConcurrentMap();

    final private ESLogger logger;

    public RecoveriesCollection(ESLogger logger) {
        this.logger = logger;
    }

    /**
     * Starts are new recovery for the given shard, source node and state
     *
     * @return the id of the new recovery.
     */
    public long startRecovery(InternalIndexShard indexShard, DiscoveryNode sourceNode, RecoveryState state, RecoveryTarget.RecoveryListener listener) {
        RecoveryStatus status = new RecoveryStatus(indexShard, sourceNode, state, listener);
        RecoveryStatus existingStatus = onGoingRecoveries.putIfAbsent(status.recoveryId(), status);
        assert existingStatus == null : "found two RecoveryStatus instances with the same id";
        logger.trace("{} started recovery from {}, id [{}]", indexShard.shardId(), sourceNode, status.recoveryId());
        return status.recoveryId();
    }

    /**
     * gets the {@link RecoveryStatus } for a given id. The RecoveryStatus returned has it's ref count already incremented
     * to make sure it's safe to use. However, you must call {@link RecoveryStatus#decRef()} when you are done with it, typically
     * by using this method in a try-with-resources clause.
     * <p/>
     * Returns null if recovery is not found
     */
    public StatusRef getStatus(long id) {
        RecoveryStatus status = onGoingRecoveries.get(id);
        if (status != null && status.tryIncRef()) {
            return new StatusRef(status);
        }
        return null;
    }

    /** Similar to {@link #getStatus(long)} but throws an exception if no recovery is found */
    public StatusRef getStatusSafe(long id, ShardId shardId) {
        StatusRef statusRef = getStatus(id);
        if (statusRef == null) {
            throw new IndexShardClosedException(shardId);
        }
        assert statusRef.status().shardId().equals(shardId);
        return statusRef;
    }

    /** cancel the recovery with the given id (if found) and remove it from the recovery collection */
    public void cancelRecovery(long id, String reason) {
        RecoveryStatus removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} canceled recovery from {}, id [{}] (reason [{}])",
                    removed.shardId(), removed.sourceNode(), removed.recoveryId(), reason);
            removed.cancel(reason);
        }
    }

    /**
     * fail the recovery with the given id (if found) and remove it from the recovery collection
     *
     * @param id id of the recovery to fail
     * @param e  exception with reason for the failure
     * @param sendShardFailure true a shard failed message should be sent to the master
     */
    public void failRecovery(long id, RecoveryFailedException e, boolean sendShardFailure) {
        RecoveryStatus removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} failing recovery from {}, id [{}]. Send shard failure: [{}]", removed.shardId(), removed.sourceNode(), removed.recoveryId(), sendShardFailure);
            removed.fail(e, sendShardFailure);
        }
    }

    /** mark the recovery with the given id as done (if found) */
    public void markRecoveryAsDone(long id) {
        RecoveryStatus removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} marking recovery from {} as done, id [{}]", removed.shardId(), removed.sourceNode(), removed.recoveryId());
            removed.markAsDone();
        }
    }

    /**
     * Try to find an ongoing recovery for a given shard. returns null if not found.
     */
    @Nullable
    public StatusRef findRecoveryByShard(IndexShard indexShard) {
        for (RecoveryStatus recoveryStatus : onGoingRecoveries.values()) {
            if (recoveryStatus.indexShard() == indexShard) {
                if (recoveryStatus.tryIncRef()) {
                    return new StatusRef(recoveryStatus);
                } else {
                    return null;
                }
            }
        }
        return null;
    }


    /** cancel all ongoing recoveries for the given shard. typically because the shards is closed */
    public void cancelRecoveriesForShard(ShardId shardId, String reason) {
        for (RecoveryStatus status : onGoingRecoveries.values()) {
            if (status.shardId().equals(shardId)) {
                cancelRecovery(status.recoveryId(), reason);
            }
        }
    }

    /**
     * a reference to {@link RecoveryStatus}, which implements {@link AutoCloseable}. closing the reference
     * causes {@link RecoveryStatus#decRef()} to be called. This makes sure that the underlying resources
     * will not be freed until {@link RecoveriesCollection.StatusRef#close()} is called.
     */
    public static class StatusRef implements AutoCloseable {

        private final RecoveryStatus status;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        /**
         * Important: {@link org.elasticsearch.indices.recovery.RecoveryStatus#tryIncRef()} should
         * be *successfully* called on status before
         */
        public StatusRef(RecoveryStatus status) {
            this.status = status;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                status.decRef();
            }
        }

        public RecoveryStatus status() {
            return status;
        }
    }
}

