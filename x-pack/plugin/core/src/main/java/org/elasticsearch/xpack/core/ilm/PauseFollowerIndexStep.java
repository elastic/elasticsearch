/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.List;

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
    void innerPerformAction(String followerIndex, ClusterState currentClusterState, ActionListener<Void> listener) {
        PersistentTasksCustomMetadata persistentTasksMetadata = currentClusterState.metadata()
            .getProject()
            .custom(PersistentTasksCustomMetadata.TYPE);
        if (persistentTasksMetadata == null) {
            listener.onResponse(null);
            return;
        }

        List<PersistentTasksCustomMetadata.PersistentTask<?>> shardFollowTasks = persistentTasksMetadata.tasks()
            .stream()
            .filter(persistentTask -> ShardFollowTask.NAME.equals(persistentTask.getTaskName()))
            .filter(persistentTask -> {
                ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
                return shardFollowTask.getFollowShardId().getIndexName().equals(followerIndex);
            })
            .toList();

        if (shardFollowTasks.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        PauseFollowAction.Request request = new PauseFollowAction.Request(TimeValue.MAX_VALUE, followerIndex);
        getClient().execute(PauseFollowAction.INSTANCE, request, listener.delegateFailureAndWrap((l, r) -> {
            if (r.isAcknowledged() == false) {
                throw new ElasticsearchException("pause follow request failed to be acknowledged");
            }
            l.onResponse(null);
        }));
    }
}
