/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowerInfo;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.Status;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TransportFollowInfoAction extends TransportMasterNodeReadAction<FollowInfoAction.Request, FollowInfoAction.Response> {

    @Inject
    public TransportFollowInfoAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(FollowInfoAction.NAME, transportService, clusterService, threadPool, actionFilters, FollowInfoAction.Request::new,
            indexNameExpressionResolver, FollowInfoAction.Response::new, ThreadPool.Names.SAME);
    }

    @Override
    protected void masterOperation(Task task, FollowInfoAction.Request request,
                                   ClusterState state,
                                   ActionListener<FollowInfoAction.Response> listener) throws Exception {

        List<String> concreteFollowerIndices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.STRICT_EXPAND_OPEN_CLOSED, request.getFollowerIndices()));

        List<FollowerInfo> followerInfos = getFollowInfos(concreteFollowerIndices, state);
        listener.onResponse(new FollowInfoAction.Response(followerInfos));
    }

    @Override
    protected ClusterBlockException checkBlock(FollowInfoAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    static List<FollowerInfo> getFollowInfos(List<String> concreteFollowerIndices, ClusterState state) {
        List<FollowerInfo> followerInfos = new ArrayList<>();
        PersistentTasksCustomMetadata persistentTasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);

        for (String index : concreteFollowerIndices) {
            IndexMetadata indexMetadata = state.metadata().index(index);
            Map<String, String> ccrCustomData = indexMetadata.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
            if (ccrCustomData != null) {
                Optional<ShardFollowTask> result;
                if (persistentTasks != null) {
                    result = persistentTasks.findTasks(ShardFollowTask.NAME, task -> true).stream()
                        .map(task -> (ShardFollowTask) task.getParams())
                        .filter(shardFollowTask -> index.equals(shardFollowTask.getFollowShardId().getIndexName()))
                        .findAny();
                } else {
                    result = Optional.empty();
                }

                String followerIndex = indexMetadata.getIndex().getName();
                String remoteCluster = ccrCustomData.get(Ccr.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY);
                String leaderIndex = ccrCustomData.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY);
                if (result.isPresent()) {
                    ShardFollowTask params = result.get();
                    FollowParameters followParameters = new FollowParameters();
                    followParameters.setMaxOutstandingReadRequests(params.getMaxOutstandingReadRequests());
                    followParameters.setMaxOutstandingWriteRequests(params.getMaxOutstandingWriteRequests());
                    followParameters.setMaxReadRequestOperationCount(params.getMaxReadRequestOperationCount());
                    followParameters.setMaxWriteRequestOperationCount(params.getMaxWriteRequestOperationCount());
                    followParameters.setMaxReadRequestSize(params.getMaxReadRequestSize());
                    followParameters.setMaxWriteRequestSize(params.getMaxWriteRequestSize());
                    followParameters.setMaxWriteBufferCount(params.getMaxWriteBufferCount());
                    followParameters.setMaxWriteBufferSize(params.getMaxWriteBufferSize());
                    followParameters.setMaxRetryDelay(params.getMaxRetryDelay());
                    followParameters.setReadPollTimeout(params.getReadPollTimeout());
                    followerInfos.add(new FollowerInfo(followerIndex, remoteCluster, leaderIndex, Status.ACTIVE, followParameters));
                } else {
                    followerInfos.add(new FollowerInfo(followerIndex, remoteCluster, leaderIndex, Status.PAUSED, null));
                }
            }
        }

        return followerInfos;
    }
}
