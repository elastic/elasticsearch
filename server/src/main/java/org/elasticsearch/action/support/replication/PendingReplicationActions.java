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

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PendingReplicationActions implements Consumer<ReplicationGroup> {

    private final Map<String, Map<Object, RetryableAction<?>>> onGoingReplicationActions = ConcurrentCollections.newConcurrentMap();
    private final ShardId shardId;
    private final ThreadPool threadPool;

    public PendingReplicationActions(ShardId shardId, ThreadPool threadPool) {
        this.shardId = shardId;
        this.threadPool = threadPool;
    }

    public void addPendingAction(String nodeId, Object actionKey, RetryableAction<?> replicationAction) {
        Map<Object, RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(nodeId);
        if (ongoingActionsOnNode != null) {
            ongoingActionsOnNode.put(actionKey, replicationAction);
            if (onGoingReplicationActions.containsKey(nodeId) == false) {
                replicationAction.cancel(new UnavailableShardsException(shardId, "Replica left ReplicationGroup"));
            }
        } else {
            replicationAction.cancel(new UnavailableShardsException(shardId, "Replica left ReplicationGroup"));
        }
    }

    public void removeReplicationAction(String nodeId, Object actionKey) {
        Map<Object, RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(nodeId);
        if (ongoingActionsOnNode != null) {
            ongoingActionsOnNode.remove(actionKey);
        }
    }

    @Override
    public synchronized void accept(ReplicationGroup replicationGroup) {
        Set<String> newReplicaNodeIds = replicationGroup.getReplicationTargets().stream()
            .map(ShardRouting::currentNodeId)
            .collect(Collectors.toSet());

        for (String replicaNodeId : newReplicaNodeIds) {
            onGoingReplicationActions.putIfAbsent(replicaNodeId, ConcurrentCollections.newConcurrentMap());
        }
        ArrayList<Map<Object, RetryableAction<?>>> toCancel = new ArrayList<>();
        for (String existingReplicaNodeId : onGoingReplicationActions.keySet()) {
            if (newReplicaNodeIds.contains(existingReplicaNodeId) == false) {
                toCancel.add(onGoingReplicationActions.remove(existingReplicaNodeId));
            }
        }

        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> toCancel.stream()
            .flatMap(m -> m.values().stream())
            .forEach(action -> action.cancel(new UnavailableShardsException(shardId, "Replica left ReplicationGroup"))));
    }
}
