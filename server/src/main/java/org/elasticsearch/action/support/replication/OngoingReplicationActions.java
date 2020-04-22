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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Map;

public class OngoingReplicationActions {

    private final Map<String, Map<Object, RetryableAction<?>>> onGoingReplicationActions = ConcurrentCollections.newConcurrentMap();

    public void addPendingReplicationAction(String nodeId, Object actionKey, RetryableAction<?> replicationAction) {
        Map<Object, RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(nodeId);
        if (ongoingActionsOnNode != null) {
            ongoingActionsOnNode.put(actionKey, replicationAction);
            if (onGoingReplicationActions.containsKey(nodeId) == false) {
                replicationAction.cancel(new ElasticsearchException("TODO"));
            }
        }
    }

    public void removePendingReplicationAction(String nodeId, Object actionKey) {
        Map<Object, RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(nodeId);
        if (ongoingActionsOnNode != null) {
            ongoingActionsOnNode.remove(actionKey);
        }
    }

    public synchronized void nodeJoinedReplicationGroup(String nodeId) {
        onGoingReplicationActions.put(nodeId, ConcurrentCollections.newConcurrentMap());
    }

    public synchronized void nodeLeftReplicationGroup(String nodeId) {
        Map<Object, RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.remove(nodeId);
        for (RetryableAction<?> replicationAction : ongoingActionsOnNode.values()) {
            replicationAction.cancel(new ElasticsearchException("TODO"));
        }
    }
}
