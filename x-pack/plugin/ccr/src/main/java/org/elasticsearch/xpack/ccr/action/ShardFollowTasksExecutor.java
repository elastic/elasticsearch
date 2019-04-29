/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrRetentionLeases;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ccr.CcrLicenseChecker.wrapClient;
import static org.elasticsearch.xpack.ccr.action.TransportResumeFollowAction.extractLeaderShardHistoryUUIDs;

public class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask> {

    private static final Logger logger = LogManager.getLogger(ShardFollowTasksExecutor.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IndexScopedSettings indexScopedSettings;
    private final TimeValue retentionLeaseRenewInterval;
    private volatile TimeValue waitForMetadataTimeOut;

    public ShardFollowTasksExecutor(Client client,
                                    ThreadPool threadPool,
                                    ClusterService clusterService,
                                    SettingsModule settingsModule) {
        super(ShardFollowTask.NAME, Ccr.CCR_THREAD_POOL_NAME);
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indexScopedSettings = settingsModule.getIndexScopedSettings();
        this.retentionLeaseRenewInterval = CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(settingsModule.getSettings());
        this.waitForMetadataTimeOut = CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT.get(settingsModule.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT,
            newVal -> this.waitForMetadataTimeOut = newVal);
    }

    @Override
    public void validate(ShardFollowTask params, ClusterState clusterState) {
        IndexRoutingTable routingTable = clusterState.getRoutingTable().index(params.getFollowShardId().getIndex());
        if (routingTable.shard(params.getFollowShardId().id()).primaryShard().started() == false) {
            throw new IllegalArgumentException("Not all copies of follow shard are started");
        }
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetaData.PersistentTask<ShardFollowTask> taskInProgress,
                                                 Map<String, String> headers) {
        ShardFollowTask params = taskInProgress.getParams();
        Client followerClient = wrapClient(client, params.getHeaders());
        BiConsumer<TimeValue, Runnable> scheduler = (delay, command) ->
            threadPool.scheduleUnlessShuttingDown(delay, Ccr.CCR_THREAD_POOL_NAME, command);

        final String recordedLeaderShardHistoryUUID = getLeaderShardHistoryUUID(params);
        return new ShardFollowNodeTask(id, type, action, getDescription(taskInProgress), parentTaskId, headers, params,
            scheduler, System::nanoTime) {

            @Override
            protected void innerUpdateMapping(long minRequiredMappingVersion, LongConsumer handler, Consumer<Exception> errorHandler) {
                final Index followerIndex = params.getFollowShardId().getIndex();
                final Index leaderIndex = params.getLeaderShardId().getIndex();
                final Supplier<TimeValue> timeout = () -> isStopped() ? TimeValue.MINUS_ONE : waitForMetadataTimeOut;

                final Client remoteClient;
                try {
                    remoteClient = remoteClient(params);
                } catch (NoSuchRemoteClusterException e) {
                    errorHandler.accept(e);
                    return;
                }

                CcrRequests.getIndexMetadata(remoteClient, leaderIndex, minRequiredMappingVersion, 0L, timeout, ActionListener.wrap(
                    indexMetaData -> {
                        if (indexMetaData.getMappings().isEmpty()) {
                            assert indexMetaData.getMappingVersion() == 1;
                            handler.accept(indexMetaData.getMappingVersion());
                            return;
                        }
                        assert indexMetaData.getMappings().size() == 1 : "expected exactly one mapping, but got [" +
                            indexMetaData.getMappings().size() + "]";
                        MappingMetaData mappingMetaData = indexMetaData.getMappings().iterator().next().value;
                        PutMappingRequest putMappingRequest = CcrRequests.putMappingRequest(followerIndex.getName(), mappingMetaData);
                        followerClient.admin().indices().putMapping(putMappingRequest, ActionListener.wrap(
                            putMappingResponse -> handler.accept(indexMetaData.getMappingVersion()),
                            errorHandler));
                    },
                    errorHandler
                ));
            }

            @Override
            protected void innerUpdateSettings(final LongConsumer finalHandler, final Consumer<Exception> errorHandler) {
                final Index leaderIndex = params.getLeaderShardId().getIndex();
                final Index followIndex = params.getFollowShardId().getIndex();

                ClusterStateRequest clusterStateRequest = CcrRequests.metaDataRequest(leaderIndex.getName());

                CheckedConsumer<ClusterStateResponse, Exception> onResponse = clusterStateResponse -> {
                    final IndexMetaData leaderIMD = clusterStateResponse.getState().metaData().getIndexSafe(leaderIndex);
                    final IndexMetaData followerIMD = clusterService.state().metaData().getIndexSafe(followIndex);

                    final Settings existingSettings = TransportResumeFollowAction.filter(followerIMD.getSettings());
                    final Settings settings = TransportResumeFollowAction.filter(leaderIMD.getSettings());
                    if (existingSettings.equals(settings)) {
                        // If no settings have been changed then just propagate settings version to shard follow node task:
                        finalHandler.accept(leaderIMD.getSettingsVersion());
                    } else {
                        // Figure out which settings have been updated:
                        final Settings updatedSettings = settings.filter(
                            s -> existingSettings.get(s) == null || existingSettings.get(s).equals(settings.get(s)) == false
                        );

                        // Figure out whether the updated settings are all dynamic settings and
                        // if so just update the follower index's settings:
                        if (updatedSettings.keySet().stream().allMatch(indexScopedSettings::isDynamicSetting)) {
                            // If only dynamic settings have been updated then just update these settings in follower index:
                            final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(followIndex.getName());
                            updateSettingsRequest.settings(updatedSettings);
                            followerClient.admin().indices().updateSettings(updateSettingsRequest,
                                ActionListener.wrap(response -> finalHandler.accept(leaderIMD.getSettingsVersion()), errorHandler));
                        } else {
                            // If one or more setting are not dynamic then close follow index, update leader settings and
                            // then open leader index:
                            Runnable handler = () -> finalHandler.accept(leaderIMD.getSettingsVersion());
                            closeIndexUpdateSettingsAndOpenIndex(followIndex.getName(), updatedSettings, handler, errorHandler);
                        }
                    }
                };
                try {
                    remoteClient(params).admin().cluster().state(clusterStateRequest, ActionListener.wrap(onResponse, errorHandler));
                } catch (NoSuchRemoteClusterException e) {
                    errorHandler.accept(e);
                }
            }

            private void closeIndexUpdateSettingsAndOpenIndex(String followIndex,
                                                              Settings updatedSettings,
                                                              Runnable handler,
                                                              Consumer<Exception> onFailure) {
                CloseIndexRequest closeRequest = new CloseIndexRequest(followIndex);
                CheckedConsumer<CloseIndexResponse, Exception> onResponse = response -> {
                    updateSettingsAndOpenIndex(followIndex, updatedSettings, handler, onFailure);
                };
                followerClient.admin().indices().close(closeRequest, ActionListener.wrap(onResponse, onFailure));
            }

            private void updateSettingsAndOpenIndex(String followIndex,
                                                    Settings updatedSettings,
                                                    Runnable handler,
                                                    Consumer<Exception> onFailure) {
                final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(followIndex);
                updateSettingsRequest.settings(updatedSettings);
                CheckedConsumer<AcknowledgedResponse, Exception> onResponse = response -> openIndex(followIndex, handler, onFailure);
                followerClient.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(onResponse, onFailure));
            }

            private void openIndex(String followIndex,
                                   Runnable handler,
                                   Consumer<Exception> onFailure) {
                OpenIndexRequest openIndexRequest = new OpenIndexRequest(followIndex);
                CheckedConsumer<OpenIndexResponse, Exception> onResponse = response -> handler.run();
                followerClient.admin().indices().open(openIndexRequest, ActionListener.wrap(onResponse, onFailure));
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(
                final String followerHistoryUUID,
                final List<Translog.Operation> operations,
                final long maxSeqNoOfUpdatesOrDeletes,
                final Consumer<BulkShardOperationsResponse> handler,
                final Consumer<Exception> errorHandler) {

                final BulkShardOperationsRequest request = new BulkShardOperationsRequest(params.getFollowShardId(),
                    followerHistoryUUID, operations, maxSeqNoOfUpdatesOrDeletes);
                followerClient.execute(BulkShardOperationsAction.INSTANCE, request, ActionListener.wrap(handler::accept, errorHandler));
            }

            @Override
            protected void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {
                ShardChangesAction.Request request =
                    new ShardChangesAction.Request(params.getLeaderShardId(), recordedLeaderShardHistoryUUID);
                request.setFromSeqNo(from);
                request.setMaxOperationCount(maxOperationCount);
                request.setMaxBatchSize(params.getMaxReadRequestSize());
                request.setPollTimeout(params.getReadPollTimeout());
                try {
                    remoteClient(params).execute(ShardChangesAction.INSTANCE, request, ActionListener.wrap(handler::accept, errorHandler));
                } catch (NoSuchRemoteClusterException e) {
                    errorHandler.accept(e);
                }
            }

            @Override
            protected Scheduler.Cancellable scheduleBackgroundRetentionLeaseRenewal(final LongSupplier followerGlobalCheckpoint) {
                final String retentionLeaseId = CcrRetentionLeases.retentionLeaseId(
                        clusterService.getClusterName().value(),
                        params.getFollowShardId().getIndex(),
                        params.getRemoteCluster(),
                        params.getLeaderShardId().getIndex());

                /*
                 * We are going to attempt to renew the retention lease. If this fails it is either because the retention lease does not
                 * exist, or something else happened. If the retention lease does not exist, we will attempt to add the retention lease
                 * again. If that fails, it had better not be because the retention lease already exists. Either way, we will attempt to
                 * renew again on the next scheduled execution.
                 */
                final ActionListener<RetentionLeaseActions.Response> listener = ActionListener.wrap(
                        r -> {},
                        e -> {
                            /*
                             * We have to guard against the possibility that the shard follow node task has been stopped and the retention
                             * lease deliberately removed via the act of unfollowing. Note that the order of operations is important in
                             * TransportUnfollowAction. There, we first stop the shard follow node task, and then remove the retention
                             * leases on the leader. This means that if we end up here with the retention lease not existing because of an
                             * unfollow action, then we know that the unfollow action has already stopped the shard follow node task and
                             * there is no race condition with the unfollow action.
                             */
                            if (isCancelled() || isCompleted()) {
                                return;
                            }
                            final Throwable cause = ExceptionsHelper.unwrapCause(e);
                            logRetentionLeaseFailure(retentionLeaseId, cause);
                            // noinspection StatementWithEmptyBody
                            if (cause instanceof RetentionLeaseNotFoundException) {
                                // note that we do not need to mark as system context here as that is restored from the original renew
                                logger.trace(
                                        "{} background adding retention lease [{}] while following",
                                        params.getFollowShardId(),
                                        retentionLeaseId);
                                CcrRetentionLeases.asyncAddRetentionLease(
                                        params.getLeaderShardId(),
                                        retentionLeaseId,
                                        followerGlobalCheckpoint.getAsLong(),
                                        remoteClient(params),
                                        ActionListener.wrap(
                                                r -> {},
                                                inner -> {
                                                    /*
                                                     * If this fails that the retention lease already exists, something highly unusual is
                                                     * going on. Log it, and renew again after another renew interval has passed.
                                                     */
                                                    final Throwable innerCause = ExceptionsHelper.unwrapCause(inner);
                                                    assert innerCause instanceof RetentionLeaseAlreadyExistsException == false;
                                                    logRetentionLeaseFailure(retentionLeaseId, innerCause);
                                                }));
                            } else {
                                 // if something else happened, we will attempt to renew again after another renew interval has passed
                            }
                        });

                return threadPool.scheduleWithFixedDelay(
                        () -> {
                            final ThreadContext threadContext = threadPool.getThreadContext();
                            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                                // we have to execute under the system context so that if security is enabled the management is authorized
                                threadContext.markAsSystemContext();
                                logger.trace(
                                        "{} background renewing retention lease [{}] while following",
                                        params.getFollowShardId(),
                                        retentionLeaseId);
                                CcrRetentionLeases.asyncRenewRetentionLease(
                                        params.getLeaderShardId(),
                                        retentionLeaseId,
                                        followerGlobalCheckpoint.getAsLong(),
                                        remoteClient(params),
                                        listener);
                            }
                        },
                        retentionLeaseRenewInterval,
                        Ccr.CCR_THREAD_POOL_NAME);
            }

            private void logRetentionLeaseFailure(final String retentionLeaseId, final Throwable cause) {
                assert cause instanceof ElasticsearchSecurityException == false : cause;
                logger.warn(new ParameterizedMessage(
                                "{} background management of retention lease [{}] failed while following",
                                params.getFollowShardId(),
                                retentionLeaseId),
                        cause);
            }

        };
    }

    private String getLeaderShardHistoryUUID(ShardFollowTask params) {
        IndexMetaData followIndexMetaData = clusterService.state().metaData().index(params.getFollowShardId().getIndex());
        Map<String, String> ccrIndexMetadata = followIndexMetaData.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
        String[] recordedLeaderShardHistoryUUIDs = extractLeaderShardHistoryUUIDs(ccrIndexMetadata);
        return recordedLeaderShardHistoryUUIDs[params.getLeaderShardId().id()];
    }

    private Client remoteClient(ShardFollowTask params) {
        return wrapClient(client.getRemoteClusterClient(params.getRemoteCluster()), params.getHeaders());
    }

    interface FollowerStatsInfoHandler {
        void accept(String followerHistoryUUID, long globalCheckpoint, long maxSeqNo);
    }

    @Override
    protected void nodeOperation(final AllocatedPersistentTask task, final ShardFollowTask params, final PersistentTaskState state) {
        Client followerClient = wrapClient(client, params.getHeaders());
        ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
        logger.info("{} Starting to track leader shard {}", params.getFollowShardId(), params.getLeaderShardId());

        FollowerStatsInfoHandler handler = (followerHistoryUUID, followerGCP, maxSeqNo) -> {
            shardFollowNodeTask.start(followerHistoryUUID, followerGCP, maxSeqNo, followerGCP, maxSeqNo);
        };
        Consumer<Exception> errorHandler = e -> {
            if (shardFollowNodeTask.isStopped()) {
                return;
            }

            if (ShardFollowNodeTask.shouldRetry(params.getRemoteCluster(), e)) {
                logger.debug(new ParameterizedMessage("failed to fetch follow shard global {} checkpoint and max sequence number",
                    shardFollowNodeTask), e);
                threadPool.schedule(() -> nodeOperation(task, params, state), params.getMaxRetryDelay(), Ccr.CCR_THREAD_POOL_NAME);
            } else {
                shardFollowNodeTask.setFatalException(e);
            }
        };

        fetchFollowerShardInfo(followerClient, params.getFollowShardId(), handler, errorHandler);
    }

    private void fetchFollowerShardInfo(
            final Client client,
            final ShardId shardId,
            final FollowerStatsInfoHandler handler,
            final Consumer<Exception> errorHandler) {
        client.admin().indices().stats(new IndicesStatsRequest().indices(shardId.getIndexName()), ActionListener.wrap(r -> {
            IndexStats indexStats = r.getIndex(shardId.getIndexName());
            if (indexStats == null) {
                IndexMetaData indexMetaData = clusterService.state().metaData().index(shardId.getIndex());
                if (indexMetaData != null) {
                    errorHandler.accept(new ShardNotFoundException(shardId));
                } else {
                    errorHandler.accept(new IndexNotFoundException(shardId.getIndex()));
                }
                return;
            }

            Optional<ShardStats> filteredShardStats = Arrays.stream(indexStats.getShards())
                    .filter(shardStats -> shardStats.getShardRouting().shardId().equals(shardId))
                    .filter(shardStats -> shardStats.getShardRouting().primary())
                    .findAny();
            if (filteredShardStats.isPresent()) {
                final ShardStats shardStats = filteredShardStats.get();
                final CommitStats commitStats = shardStats.getCommitStats();
                if (commitStats == null) {
                    // If commitStats is null then AlreadyClosedException has been thrown: TransportIndicesStatsAction#shardOperation(...)
                    // AlreadyClosedException will be retried byShardFollowNodeTask.shouldRetry(...)
                    errorHandler.accept(new AlreadyClosedException(shardId + " commit_stats are missing"));
                    return;
                }
                final SeqNoStats seqNoStats = shardStats.getSeqNoStats();
                if (seqNoStats == null) {
                    // If seqNoStats is null then AlreadyClosedException has been thrown at TransportIndicesStatsAction#shardOperation(...)
                    // AlreadyClosedException will be retried byShardFollowNodeTask.shouldRetry(...)
                    errorHandler.accept(new AlreadyClosedException(shardId + " seq_no_stats are missing"));
                    return;
                }

                final String historyUUID = commitStats.getUserData().get(Engine.HISTORY_UUID_KEY);
                final long globalCheckpoint = seqNoStats.getGlobalCheckpoint();
                final long maxSeqNo = seqNoStats.getMaxSeqNo();
                handler.accept(historyUUID, globalCheckpoint, maxSeqNo);
            } else {
                errorHandler.accept(new ShardNotFoundException(shardId));
            }
        }, errorHandler));
    }

}
