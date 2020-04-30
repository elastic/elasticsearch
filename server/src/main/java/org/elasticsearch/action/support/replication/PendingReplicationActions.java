/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
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
                replicationAction.cancel(new IndexShardClosedException(shardId,
                    "Replica unavailable - replica could have left ReplicationGroup or IndexShard might have closed"));
            }
        } else {
            replicationAction.cancel(new IndexShardClosedException(shardId,
                "Replica unavailable - replica could have left ReplicationGroup or IndexShard might have closed"));
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
            onGoingReplicationActions.putIfAbsent(targetAllocationId, ConcurrentCollections.newConcurrentSet());
        }
        ArrayList<Set<RetryableAction<?>>> toCancel = new ArrayList<>();
        for (String allocationId : onGoingReplicationActions.keySet()) {
            if (trackedAllocationIds.contains(allocationId) == false) {
                toCancel.add(onGoingReplicationActions.remove(allocationId));
            }
        }

        cancelActions(toCancel, "Replica left ReplicationGroup");
    }

    @Override
    public synchronized void close() {
        ArrayList<Set<RetryableAction<?>>> toCancel = new ArrayList<>(onGoingReplicationActions.values());
        onGoingReplicationActions.clear();

        cancelActions(toCancel, "Primary closed.");
    }

    private void cancelActions(ArrayList<Set<RetryableAction<?>>> toCancel, String message) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> toCancel.stream()
            .flatMap(Collection::stream)
            .forEach(action -> action.cancel(new IndexShardClosedException(shardId, message))));
    }
}
