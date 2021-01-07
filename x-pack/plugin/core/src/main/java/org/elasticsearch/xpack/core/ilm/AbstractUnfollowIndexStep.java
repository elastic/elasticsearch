/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;

abstract class AbstractUnfollowIndexStep extends AsyncActionStep {

    AbstractUnfollowIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public final void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState,
                                    ClusterStateObserver observer, Listener listener) {
        String followerIndex = indexMetadata.getIndex().getName();
        Map<String, String> customIndexMetadata = indexMetadata.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            listener.onResponse(true);
            return;
        }

        PersistentTasksCustomMetadata persistentTasksMetadata = currentClusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (persistentTasksMetadata == null) {
            listener.onResponse(true);
            return;
        }

        List<PersistentTask<?>> shardFollowTasks = persistentTasksMetadata.tasks().stream()
            .filter(persistentTask -> ShardFollowTask.NAME.equals(persistentTask.getTaskName()))
            .filter(persistentTask -> {
                ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
                return shardFollowTask.getFollowShardId().getIndexName().equals(followerIndex);
            })
            .collect(Collectors.toList());

        if (shardFollowTasks.isEmpty()) {
            listener.onResponse(true);
            return;
        }

        innerPerformAction(followerIndex, listener);
    }

    abstract void innerPerformAction(String followerIndex, Listener listener);
}
