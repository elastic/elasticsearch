/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.List;

public class TransportPauseFollowAction extends AcknowledgedTransportMasterNodeAction<PauseFollowAction.Request> {

    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportPauseFollowAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final PersistentTasksService persistentTasksService
    ) {
        super(
            PauseFollowAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PauseFollowAction.Request::new,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PauseFollowAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final IndexMetadata followerIMD = state.metadata().index(request.getFollowIndex());
        if (followerIMD == null) {
            listener.onFailure(new IndexNotFoundException(request.getFollowIndex()));
            return;
        }
        if (followerIMD.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY) == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.getFollowIndex() + "] is not a follower index"));
            return;
        }
        PersistentTasksCustomMetadata persistentTasksMetadata = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (persistentTasksMetadata == null) {
            listener.onFailure(new IllegalArgumentException("no shard follow tasks found"));
            return;
        }

        List<String> shardFollowTaskIds = persistentTasksMetadata.tasks()
            .stream()
            .filter(persistentTask -> ShardFollowTask.NAME.equals(persistentTask.getTaskName()))
            .filter(persistentTask -> {
                ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
                return shardFollowTask.getFollowShardId().getIndexName().equals(request.getFollowIndex());
            })
            .map(PersistentTasksCustomMetadata.PersistentTask::getId)
            .toList();

        if (shardFollowTaskIds.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("no shard follow tasks for [" + request.getFollowIndex() + "]"));
            return;
        }

        int i = 0;
        final ResponseHandler responseHandler = new ResponseHandler(shardFollowTaskIds.size(), listener);
        for (String taskId : shardFollowTaskIds) {
            final int taskSlot = i++;
            persistentTasksService.sendRemoveRequest(taskId, request.masterNodeTimeout(), responseHandler.getActionListener(taskSlot));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PauseFollowAction.Request request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowIndex());
    }

}
