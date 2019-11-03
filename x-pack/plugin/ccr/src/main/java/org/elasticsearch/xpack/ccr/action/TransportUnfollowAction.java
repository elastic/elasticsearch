/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrRetentionLeases;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class TransportUnfollowAction extends TransportMasterNodeAction<UnfollowAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportUnfollowAction.class);

    private final Client client;

    @Inject
    public TransportUnfollowAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final Client client) {
        super(
                UnfollowAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                UnfollowAction.Request::new,
                indexNameExpressionResolver);
        this.client = Objects.requireNonNull(client);
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
    protected void masterOperation(
        Task task, final UnfollowAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("unfollow_action", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(final ClusterState current) {
                String followerIndex = request.getFollowerIndex();
                return unfollow(followerIndex, current);
            }

            @Override
            public void onFailure(final String source, final Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                final IndexMetaData indexMetaData = oldState.metaData().index(request.getFollowerIndex());
                final Map<String, String> ccrCustomMetaData = indexMetaData.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
                final String remoteClusterName = ccrCustomMetaData.get(Ccr.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY);
                final Client remoteClient = client.getRemoteClusterClient(remoteClusterName);
                final String leaderIndexName = ccrCustomMetaData.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY);
                final String leaderIndexUuid = ccrCustomMetaData.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
                final Index leaderIndex = new Index(leaderIndexName, leaderIndexUuid);
                final String retentionLeaseId = CcrRetentionLeases.retentionLeaseId(
                        oldState.getClusterName().value(),
                        indexMetaData.getIndex(),
                        remoteClusterName,
                        leaderIndex);
                final int numberOfShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexMetaData.getSettings());

                final GroupedActionListener<RetentionLeaseActions.Response> groupListener = new GroupedActionListener<>(
                        new ActionListener<Collection<RetentionLeaseActions.Response>>() {

                            @Override
                            public void onResponse(final Collection<RetentionLeaseActions.Response> responses) {
                                logger.trace(
                                        "[{}] removed retention lease [{}] on all leader primary shards",
                                        indexMetaData.getIndex(),
                                        retentionLeaseId);
                                listener.onResponse(new AcknowledgedResponse(true));
                            }

                            @Override
                            public void onFailure(final Exception e) {
                                logger.warn(new ParameterizedMessage(
                                        "[{}] failure while removing retention lease [{}] on leader primary shards",
                                        indexMetaData.getIndex(),
                                        retentionLeaseId),
                                        e);
                                final ElasticsearchException wrapper = new ElasticsearchException(e);
                                wrapper.addMetadata("es.failed_to_remove_retention_leases", retentionLeaseId);
                                listener.onFailure(wrapper);
                            }

                        },
                        numberOfShards
                );
                for (int i = 0; i < numberOfShards; i++) {
                    final ShardId followerShardId = new ShardId(indexMetaData.getIndex(), i);
                    final ShardId leaderShardId = new ShardId(leaderIndex, i);
                    removeRetentionLeaseForShard(
                            followerShardId,
                            leaderShardId,
                            retentionLeaseId,
                            remoteClient,
                            ActionListener.wrap(
                                    groupListener::onResponse,
                                    e -> handleException(
                                            followerShardId,
                                            retentionLeaseId,
                                            leaderShardId,
                                            groupListener,
                                            e)));
                }
            }

            private void removeRetentionLeaseForShard(
                    final ShardId followerShardId,
                    final ShardId leaderShardId,
                    final String retentionLeaseId,
                    final Client remoteClient,
                    final ActionListener<RetentionLeaseActions.Response> listener) {
                logger.trace("{} removing retention lease [{}] while unfollowing leader index", followerShardId, retentionLeaseId);
                final ThreadContext threadContext = threadPool.getThreadContext();
                try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
                    // we have to execute under the system context so that if security is enabled the removal is authorized
                    threadContext.markAsSystemContext();
                    CcrRetentionLeases.asyncRemoveRetentionLease(leaderShardId, retentionLeaseId, remoteClient, listener);
                }
            }

            private void handleException(
                    final ShardId followerShardId,
                    final String retentionLeaseId,
                    final ShardId leaderShardId,
                    final ActionListener<RetentionLeaseActions.Response> listener,
                    final Exception e) {
                final Throwable cause = ExceptionsHelper.unwrapCause(e);
                assert cause instanceof ElasticsearchSecurityException == false : e;
                if (cause instanceof RetentionLeaseNotFoundException) {
                    // treat as success
                    logger.trace(new ParameterizedMessage(
                            "{} retention lease [{}] not found on {} while unfollowing",
                            followerShardId,
                            retentionLeaseId,
                            leaderShardId),
                            e);
                    listener.onResponse(new RetentionLeaseActions.Response());
                } else {
                    logger.warn(new ParameterizedMessage(
                            "{} failed to remove retention lease [{}] on {} while unfollowing",
                            followerShardId,
                            retentionLeaseId,
                            leaderShardId),
                            e);
                    listener.onFailure(e);
                }
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

        // Remove index.xpack.ccr.following_index setting
        Settings.Builder builder = Settings.builder();
        builder.put(followerIMD.getSettings());
        builder.remove(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey());

        final IndexMetaData.Builder newIndexMetaData = IndexMetaData.builder(followerIMD);
        newIndexMetaData.settings(builder);
        newIndexMetaData.settingsVersion(followerIMD.getSettingsVersion() + 1);
        // Remove ccr custom metadata
        newIndexMetaData.removeCustom(Ccr.CCR_CUSTOM_METADATA_KEY);

        MetaData newMetaData = MetaData.builder(current.metaData())
            .put(newIndexMetaData)
            .build();
        return ClusterState.builder(current)
            .metaData(newMetaData)
            .build();
    }
}
