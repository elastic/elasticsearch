package org.elasticsearch.indices.recovery;/*
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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;

import java.util.Map;

public class RecoveriesCollection {

    private final Map<Long, RecoveryStatus> onGoingRecoveries = ConcurrentCollections.newConcurrentMap();

    final private ESLogger logger;

    public RecoveriesCollection(ESLogger logger) {
        this.logger = logger;
    }

    public long startRecovery(InternalIndexShard indexShard, DiscoveryNode sourceNode, RecoveryState state, RecoveryTarget.RecoveryListener listener) {
        RecoveryStatus status = new RecoveryStatus(indexShard, sourceNode, state, listener);
        onGoingRecoveries.put(status.recoveryId(), status);
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
    public RecoveryStatus getStatus(long id) {
        RecoveryStatus status = onGoingRecoveries.get(id);
        if (status != null && status.tryIncRef()) {
            return status;
        }
        return null;
    }

    /** Similar to {@link #getStatus(long)} but throws an exception if no recovery is found */
    public RecoveryStatus getStatusSafe(long id, ShardId shardId) {
        RecoveryStatus status = getStatus(id);
        if (status == null) {
            throw new IndexShardClosedException(shardId);
        }
        // to be safe
        try {
            status.validateRecoveryStatus();
        } catch (Exception e) {
            status.decRef();
            throw e;
        }
        assert status.shardId().equals(shardId);
        return status;
    }

    public void cancelRecovery(long id, String reason) {
        RecoveryStatus removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} canceled recovery from {}, id [{}] (reason [{}])",
                    removed.shardId(), removed.sourceNode(), removed.recoveryId(), reason);
            removed.cancel(reason);
        }
    }

    public void failRecovery(long id, RecoveryFailedException e, boolean sendShardFailure) {
        RecoveryStatus removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} failing recovery from {}, id [{}]. Send shard failure: [{}]", removed.shardId(), removed.sourceNode(), removed.recoveryId(), sendShardFailure);
            removed.fail(e, sendShardFailure);
        }
    }


    public void markRecoveryAsDone(long id) {
        RecoveryStatus removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} marking recovery from {} as done, id [{}]", removed.shardId(), removed.sourceNode(), removed.recoveryId());
            removed.markAsDone();
        }
    }

    @Nullable
    public RecoveryStatus findRecoveryByShard(IndexShard indexShard) {
        for (RecoveryStatus recoveryStatus : onGoingRecoveries.values()) {
            if (recoveryStatus.indexShard() == indexShard) {
                if (recoveryStatus.tryIncRef()) {
                    return recoveryStatus;
                } else {
                    return null;
                }
            }
        }
        return null;
    }


    public void cancelRecoveriesForShard(ShardId shardId, String reason) {
        for (RecoveryStatus status : onGoingRecoveries.values()) {
            if (status.shardId().equals(shardId)) {
                cancelRecovery(status.recoveryId(), reason);
            }

        }
    }

}

