/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowerInfo;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.Status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TransportFollowInfoAction extends TransportMasterNodeReadAction<FollowInfoAction.Request, FollowInfoAction.Response> {

    @Inject
    public TransportFollowInfoAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, FollowInfoAction.NAME, transportService, clusterService, threadPool, actionFilters,
            FollowInfoAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected FollowInfoAction.Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected FollowInfoAction.Response read(StreamInput in) throws IOException {
        return new FollowInfoAction.Response(in);
    }

    @Override
    protected void masterOperation(FollowInfoAction.Request request,
                                   ClusterState state,
                                   ActionListener<FollowInfoAction.Response> listener) throws Exception {

        List<String> concreteFollowerIndices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.STRICT_EXPAND_OPEN_CLOSED, request.getFollowerIndices()));


        List<FollowerInfo> followerInfos = new ArrayList<>();
        PersistentTasksCustomMetaData persistentTasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);

        for (IndexMetaData indexMetaData : state.metaData()) {
            Map<String, String> ccrCustomData = indexMetaData.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
            if (ccrCustomData != null) {
                Optional<ShardFollowTask> result;
                if (persistentTasks != null) {
                    result = persistentTasks.taskMap().values().stream()
                        .map(persistentTask -> (ShardFollowTask) persistentTask.getParams())
                        .filter(shardFollowTask -> concreteFollowerIndices.isEmpty() ||
                            concreteFollowerIndices.contains(shardFollowTask.getFollowShardId().getIndexName()))
                        .findAny();
                } else {
                    result = Optional.empty();
                }

                String followerIndex = indexMetaData.getIndex().getName();
                String remoteCluster = ccrCustomData.get(Ccr.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY);
                String leaderIndex = ccrCustomData.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY);
                if (result.isPresent()) {
                    ShardFollowTask params = result.get();
                    FollowParameters followParameters = new FollowParameters(
                        params.getMaxReadRequestOperationCount(),
                        params.getMaxReadRequestSize(),
                        params.getMaxOutstandingReadRequests(),
                        params.getMaxWriteRequestOperationCount(),
                        params.getMaxWriteRequestSize(),
                        params.getMaxOutstandingWriteRequests(),
                        params.getMaxWriteBufferCount(),
                        params.getMaxWriteBufferSize(),
                        params.getMaxRetryDelay(),
                        params.getReadPollTimeout()
                    );
                    followerInfos.add(new FollowerInfo(followerIndex, remoteCluster, leaderIndex, Status.ACTIVE, followParameters));
                } else {
                    followerInfos.add(new FollowerInfo(followerIndex, remoteCluster, leaderIndex, Status.PAUSED, null));
                }
            }
        }

        listener.onResponse(new FollowInfoAction.Response(followerInfos));
    }

    @Override
    protected ClusterBlockException checkBlock(FollowInfoAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
