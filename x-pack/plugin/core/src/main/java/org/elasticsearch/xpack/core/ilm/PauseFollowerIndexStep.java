/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.List;
import java.util.stream.Collectors;

final class PauseFollowerIndexStep extends AbstractUnfollowIndexStep {

    static final String NAME = "pause-follower-index";

    PauseFollowerIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void innerPerformAction(String followerIndex, ClusterState currentClusterState, Listener listener) {
        PersistentTasksCustomMetadata persistentTasksMetadata = currentClusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (persistentTasksMetadata == null) {
            listener.onResponse(true);
            return;
        }

        List<PersistentTasksCustomMetadata.PersistentTask<?>> shardFollowTasks = persistentTasksMetadata.tasks().stream()
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

        PauseFollowAction.Request request = new PauseFollowAction.Request(followerIndex);
        getClient().execute(PauseFollowAction.INSTANCE, request, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() : "pause follow response is not acknowledged";
                listener.onResponse(true);
            },
            listener::onFailure
        ));
    }
}
