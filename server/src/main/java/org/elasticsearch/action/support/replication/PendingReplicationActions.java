/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class PendingReplicationActions implements Consumer<ReplicationGroup>, Releasable {

    private final Map<String, Set<RetryableAction<?>>> onGoingReplicationActions = ConcurrentCollections.newConcurrentMap();
    private final ShardId shardId;
    private final ThreadPool threadPool;
    private volatile long replicationGroupVersion = -1;

    public PendingReplicationActions(ShardId shardId, ThreadPool threadPool) {
        this.shardId = shardId;
        this.threadPool = threadPool;
    }

    public void addPendingAction(String allocationId, RetryableAction<?> replicationAction) {
        Set<RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(allocationId);
        if (ongoingActionsOnNode != null) {
            ongoingActionsOnNode.add(replicationAction);
            if (onGoingReplicationActions.containsKey(allocationId) == false) {
                replicationAction.cancel(
                    new IndexShardClosedException(
                        shardId,
                        "Replica unavailable - replica could have left ReplicationGroup or IndexShard might have closed"
                    )
                );
            }
        } else {
            replicationAction.cancel(
                new IndexShardClosedException(
                    shardId,
                    "Replica unavailable - replica could have left ReplicationGroup or IndexShard might have closed"
                )
            );
        }
    }

    public void removeReplicationAction(String allocationId, RetryableAction<?> action) {
        Set<RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(allocationId);
        if (ongoingActionsOnNode != null) {
            ongoingActionsOnNode.remove(action);
        }
    }

    @Override
    public void accept(ReplicationGroup replicationGroup) {
        if (isNewerVersion(replicationGroup)) {
            synchronized (this) {
                if (isNewerVersion(replicationGroup)) {
                    acceptNewTrackedAllocationIds(replicationGroup.getTrackedAllocationIds());
                    replicationGroupVersion = replicationGroup.getVersion();
                }
            }
        }
    }

    private boolean isNewerVersion(ReplicationGroup replicationGroup) {
        // Relative comparison to mitigate long overflow
        return replicationGroup.getVersion() - replicationGroupVersion > 0;
    }

    // Visible for testing
    synchronized void acceptNewTrackedAllocationIds(Set<String> trackedAllocationIds) {
        for (String targetAllocationId : trackedAllocationIds) {
            onGoingReplicationActions.computeIfAbsent(targetAllocationId, k -> ConcurrentCollections.newConcurrentSet());
        }
        ArrayList<RetryableAction<?>> toCancel = new ArrayList<>();
        for (String allocationId : onGoingReplicationActions.keySet()) {
            if (trackedAllocationIds.contains(allocationId) == false) {
                toCancel.addAll(onGoingReplicationActions.remove(allocationId));
            }
        }

        cancelActions(toCancel, "Replica left ReplicationGroup");
    }

    @Override
    public synchronized void close() {
        final List<RetryableAction<?>> toCancel = onGoingReplicationActions.values().stream().flatMap(Collection::stream).toList();
        onGoingReplicationActions.clear();

        cancelActions(toCancel, "Primary closed.");
    }

    private void cancelActions(List<RetryableAction<?>> toCancel, String message) {
        if (toCancel.isEmpty() == false) {
            threadPool.executor(ThreadPool.Names.GENERIC)
                .execute(() -> toCancel.forEach(action -> action.cancel(new IndexShardClosedException(shardId, message))));
        }
    }
}
