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
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

public class TransportUnfollowAction extends TransportMasterNodeAction<UnfollowAction.Request, AcknowledgedResponse> {

    @Inject
    public TransportUnfollowAction(TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver) {
        super(UnfollowAction.NAME, transportService, clusterService, threadPool, actionFilters,
            UnfollowAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(UnfollowAction.Request request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("unfollow_action", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState current) throws Exception {
                String followerIndex = request.getFollowerIndex();
                return unfollow(followerIndex, current);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new AcknowledgedResponse(true));
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(UnfollowAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    static ClusterState unfollow(String followerIndex, ClusterState current) {
        IndexMetaData followerIMD = current.metaData().index(followerIndex);
        if (followerIMD == null) {
            throw new IndexNotFoundException(followerIndex);
        }

        if (followerIMD.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY) == null) {
            throw new IllegalArgumentException("index [" + followerIndex + "] is not a follower index");
        }

        if (followerIMD.getState() != IndexMetaData.State.CLOSE) {
            throw new IllegalArgumentException("cannot convert the follower index [" + followerIndex +
                "] to a non-follower, because it has not been closed");
        }

        PersistentTasksCustomMetaData persistentTasks = current.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (persistentTasks != null) {
            for (PersistentTasksCustomMetaData.PersistentTask<?> persistentTask : persistentTasks.tasks()) {
                if (persistentTask.getTaskName().equals(ShardFollowTask.NAME)) {
                    ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
                    if (shardFollowTask.getFollowShardId().getIndexName().equals(followerIndex)) {
                        throw new IllegalArgumentException("cannot convert the follower index [" + followerIndex +
                            "] to a non-follower, because it has not been paused");
                    }
                }
            }
        }

        IndexMetaData.Builder newIMD = IndexMetaData.builder(followerIMD);
        // Remove index.xpack.ccr.following_index setting
        Settings.Builder builder = Settings.builder();
        builder.put(followerIMD.getSettings());
        builder.remove(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey());

        newIMD.settings(builder);
        // Remove ccr custom metadata
        newIMD.removeCustom(Ccr.CCR_CUSTOM_METADATA_KEY);

        MetaData newMetaData = MetaData.builder(current.metaData())
            .put(newIMD)
            .build();
        return ClusterState.builder(current)
            .metaData(newMetaData)
            .build();
    }
}
