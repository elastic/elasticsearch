/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TransportPauseFollowAction extends TransportMasterNodeAction<PauseFollowAction.Request, AcknowledgedResponse> {

    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportPauseFollowAction(
            final TransportService transportService,
            final ActionFilters actionFilters,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final PersistentTasksService persistentTasksService) {
        super(PauseFollowAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PauseFollowAction.Request::new, indexNameExpressionResolver);
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task, PauseFollowAction.Request request,
                                   ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        final IndexMetaData followerIMD = state.metaData().index(request.getFollowIndex());
        if (followerIMD == null) {
            listener.onFailure(new IndexNotFoundException(request.getFollowIndex()));
            return;
        }
        if (followerIMD.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY) == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.getFollowIndex() + "] is not a follower index"));
            return;
        }
        PersistentTasksCustomMetaData persistentTasksMetaData = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (persistentTasksMetaData == null) {
            listener.onFailure(new IllegalArgumentException("no shard follow tasks found"));
            return;
        }

        List<String> shardFollowTaskIds = persistentTasksMetaData.tasks().stream()
            .filter(persistentTask -> ShardFollowTask.NAME.equals(persistentTask.getTaskName()))
            .filter(persistentTask -> {
                ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
                return shardFollowTask.getFollowShardId().getIndexName().equals(request.getFollowIndex());
            })
            .map(PersistentTasksCustomMetaData.PersistentTask::getId)
            .collect(Collectors.toList());

        if (shardFollowTaskIds.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("no shard follow tasks for [" + request.getFollowIndex() + "]"));
            return;
        }

        int i = 0;
        final ResponseHandler responseHandler = new ResponseHandler(shardFollowTaskIds.size(), listener);
        for (String taskId : shardFollowTaskIds) {
            final int taskSlot = i++;
            persistentTasksService.sendRemoveRequest(taskId, responseHandler.getActionListener(taskSlot));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PauseFollowAction.Request request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowIndex());
    }

}
