/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrRetentionLeases;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.ccr.CcrLicenseChecker.wrapClient;
import static org.elasticsearch.xpack.ccr.CcrLicenseChecker.wrapRemoteClusterClient;
import static org.elasticsearch.xpack.ccr.action.TransportResumeFollowAction.extractLeaderShardHistoryUUIDs;

public final class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask> {

    private static final Logger logger = LogManager.getLogger(ShardFollowTasksExecutor.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final Executor ccrExecutor;
    private final ClusterService clusterService;
    private final IndexScopedSettings indexScopedSettings;
    private final TimeValue retentionLeaseRenewInterval;
    private volatile TimeValue waitForMetadataTimeOut;

    public ShardFollowTasksExecutor(Client client, ThreadPool threadPool, ClusterService clusterService, SettingsModule settingsModule) {
        super(ShardFollowTask.NAME, threadPool.executor(Ccr.CCR_THREAD_POOL_NAME));
        this.client = client;
        this.threadPool = threadPool;
        this.ccrExecutor = getExecutor();
        this.clusterService = clusterService;
        this.indexScopedSettings = settingsModule.getIndexScopedSettings();
        this.retentionLeaseRenewInterval = CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(settingsModule.getSettings());
        this.waitForMetadataTimeOut = CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT.get(settingsModule.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT, newVal -> this.waitForMetadataTimeOut = newVal);
    }

    @Override
    public void validate(ShardFollowTask params, ClusterState clusterState) {
        final IndexRoutingTable routingTable = clusterState.getRoutingTable().index(params.getFollowShardId().getIndex());
        final ShardRouting primaryShard = routingTable.shard(params.getFollowShardId().id()).primaryShard();
        if (primaryShard.active() == false) {
            throw new IllegalArgumentException("The primary shard of a follower index " + primaryShard + " is not active");
        }
    }

    private static final Assignment NO_ASSIGNMENT = new Assignment(null, "no nodes found with data and remote cluster client roles");

    @Override
    public Assignment getAssignment(
        final ShardFollowTask params,
        Collection<DiscoveryNode> candidateNodes,
        final ClusterState clusterState
    ) {
        final DiscoveryNode node = selectLeastLoadedNode(
            clusterState,
            candidateNodes,
            ((Predicate<DiscoveryNode>) DiscoveryNode::canContainData).and(DiscoveryNode::isRemoteClusterClient)
        );
        if (node == null) {
            return NO_ASSIGNMENT;
        } else {
            return new Assignment(node.getId(), "node is the least loaded data node and remote cluster client");
        }
    }

    @Override
    protected AllocatedPersistentTask createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<ShardFollowTask> taskInProgress,
        Map<String, String> headers
    ) {
        ShardFollowTask params = taskInProgress.getParams();
        Client followerClient = wrapClient(client, params.getHeaders(), clusterService.state());
        BiConsumer<TimeValue, Runnable> scheduler = (delay, command) -> threadPool.scheduleUnlessShuttingDown(delay, ccrExecutor, command);

        final String recordedLeaderShardHistoryUUID = getLeaderShardHistoryUUID(params);
        return new ShardFollowNodeTask(
            id,
            type,
            action,
            getDescription(taskInProgress),
            parentTaskId,
            headers,
            params,
            scheduler,
            System::nanoTime
        ) {

            @Override
            protected void innerUpdateMapping(long minRequiredMappingVersion, LongConsumer handler, Consumer<Exception> errorHandler) {
                final Index followerIndex = params.getFollowShardId().getIndex();
                final Index leaderIndex = params.getLeaderShardId().getIndex();
                final Supplier<TimeValue> timeout = () -> isStopped() ? TimeValue.MINUS_ONE : waitForMetadataTimeOut;
                final ActionListener<IndexMetadata> listener = ActionListener.wrap(indexMetadata -> {
                    if (indexMetadata.mapping() == null) {
                        assert indexMetadata.getMappingVersion() == 1;
                        handler.accept(indexMetadata.getMappingVersion());
                        return;
                    }
                    MappingMetadata mappingMetadata = indexMetadata.mapping();
                    PutMappingRequest putMappingRequest = CcrRequests.putMappingRequest(followerIndex.getName(), mappingMetadata);
                    followerClient.admin()
                        .indices()
                        .putMapping(
                            putMappingRequest,
                            ActionListener.wrap(putMappingResponse -> handler.accept(indexMetadata.getMappingVersion()), errorHandler)
                        );
                }, errorHandler);
                try {
                    CcrRequests.getIndexMetadata(remoteClient(params), leaderIndex, minRequiredMappingVersion, 0L, timeout, listener);
                } catch (NoSuchRemoteClusterException e) {
                    errorHandler.accept(e);
                }
            }

            @Override
            protected void innerUpdateSettings(final LongConsumer finalHandler, final Consumer<Exception> errorHandler) {
                final Index leaderIndex = params.getLeaderShardId().getIndex();
                final Index followIndex = params.getFollowShardId().getIndex();

                CheckedConsumer<ClusterStateResponse, Exception> onResponse = clusterStateResponse -> {
                    final IndexMetadata leaderIMD = clusterStateResponse.getState().metadata().getProject().getIndexSafe(leaderIndex);
                    final IndexMetadata followerIMD = clusterService.state().metadata().getProject().getIndexSafe(followIndex);

                    final Settings existingSettings = TransportResumeFollowAction.filter(followerIMD.getSettings());
                    final Settings settings = TransportResumeFollowAction.filter(leaderIMD.getSettings());
                    if (existingSettings.equals(settings)) {
                        // If no settings have been changed then just propagate settings version to shard follow node task:
                        finalHandler.accept(leaderIMD.getSettingsVersion());
                    } else {
                        // Figure out which settings have been updated:
                        final Settings updatedSettings = settings.filter(s -> {
                            final Setting<?> indexSettings = indexScopedSettings.get(s);
                            if (indexSettings == null || indexSettings.isPrivateIndex() || indexSettings.isInternalIndex()) {
                                return false;
                            }
                            return existingSettings.get(s) == null || existingSettings.get(s).equals(settings.get(s)) == false;
                        });
                        if (updatedSettings.isEmpty()) {
                            finalHandler.accept(leaderIMD.getSettingsVersion());
                            return;
                        }
                        // Figure out whether the updated settings are all dynamic settings and
                        // if so just update the follower index's settings:
                        if (updatedSettings.keySet().stream().allMatch(indexScopedSettings::isDynamicSetting)) {
                            // If only dynamic settings have been updated then just update these settings in follower index:
                            final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(followIndex.getName())
                                .masterNodeTimeout(TimeValue.MAX_VALUE)
                                .settings(updatedSettings);
                            followerClient.admin()
                                .indices()
                                .updateSettings(
                                    updateSettingsRequest,
                                    ActionListener.wrap(response -> finalHandler.accept(leaderIMD.getSettingsVersion()), errorHandler)
                                );
                        } else {
                            // If one or more setting are not dynamic then close follow index, update leader settings and
                            // then open leader index:
                            Runnable handler = () -> finalHandler.accept(leaderIMD.getSettingsVersion());
                            closeIndexUpdateSettingsAndOpenIndex(followIndex.getName(), updatedSettings, handler, errorHandler);
                        }
                    }
                };
                try {
                    remoteClient(params).execute(
                        ClusterStateAction.REMOTE_TYPE,
                        CcrRequests.metadataRequest(leaderIndex.getName()),
                        ActionListener.wrap(onResponse, errorHandler)
                    );
                } catch (NoSuchRemoteClusterException e) {
                    errorHandler.accept(e);
                }
            }

            @Override
            protected void innerUpdateAliases(final LongConsumer handler, final Consumer<Exception> errorHandler) {
                /*
                 * The strategy for updating the aliases is fairly simple. We look at the aliases that exist on the leader, and those that
                 * exist on the follower. We partition these aliases into three sets: the aliases that exist on both the leader and the
                 * follower, the aliases that are on the leader only, and the aliases that are on the follower only.
                 *
                 * For the aliases that are on the leader and the follower, we compare the aliases and add an action to overwrite the
                 * follower view of the alias if the aliases are different. If the aliases are the same, we skip the alias. Note that the
                 * meaning of equals here intentionally ignores the write index. There are two reasons for this. First, follower indices
                 * do not receive direct writes so conceptually the write index is not useful. Additionally, there is a larger challenge.
                 * Suppose that we did copy over the write index from the leader to the follower. On the leader, when the write index is
                 * swapped from one index to another, this is done atomically. However, to do this on the follower, we would have to step
                 * outside the shard follow tasks framework and have a separate framework for copying aliases over. This is because if we
                 * try to manage the aliases including the write aliases with the shard follow tasks, we do not have a way to move the write
                 * index atomically (since we have a single-index view here only) and therefore we can end up in situations where we would
                 * try to assign the write index to two indices. Further, trying to do this outside the shard follow tasks framework has
                 * problems too, since it could be that the new aliases arrive on the coordinator before the write index has even been
                 * created on the local cluster. So there are race conditions either way. All of this put together means that we will simply
                 * ignore the write index.
                 *
                 * For aliases that are on the leader but not the follower, we copy those aliases over to the follower.
                 *
                 * For aliases that are on the follower but not the leader, we remove those aliases from the follower.
                 */
                final var leaderIndex = params.getLeaderShardId().getIndex();
                final var followerIndex = params.getFollowShardId().getIndex();

                final CheckedConsumer<ClusterStateResponse, Exception> onResponse = clusterStateResponse -> {
                    final var leaderIndexMetadata = clusterStateResponse.getState().metadata().getProject().getIndexSafe(leaderIndex);
                    final var followerIndexMetadata = clusterService.state().metadata().getProject().getIndexSafe(followerIndex);

                    // partition the aliases into the three sets
                    final var aliasesOnLeaderNotOnFollower = new HashSet<String>();
                    final var aliasesInCommon = new HashSet<String>();
                    final var aliasesOnFollowerNotOnLeader = new HashSet<String>();

                    for (final var aliasName : leaderIndexMetadata.getAliases().keySet()) {
                        if (followerIndexMetadata.getAliases().containsKey(aliasName)) {
                            aliasesInCommon.add(aliasName);
                        } else {
                            aliasesOnLeaderNotOnFollower.add(aliasName);
                        }
                    }

                    for (final var aliasName : followerIndexMetadata.getAliases().keySet()) {
                        if (leaderIndexMetadata.getAliases().containsKey(aliasName)) {
                            assert aliasesInCommon.contains(aliasName) : aliasName;
                        } else {
                            aliasesOnFollowerNotOnLeader.add(aliasName);
                        }
                    }

                    final var aliasActions = new ArrayList<IndicesAliasesRequest.AliasActions>();

                    // add the aliases the follower does not have
                    for (final var aliasName : aliasesOnLeaderNotOnFollower) {
                        final var alias = leaderIndexMetadata.getAliases().get(aliasName);
                        // we intentionally override that the alias is not a write alias as follower indices do not receive direct writes
                        aliasActions.add(
                            IndicesAliasesRequest.AliasActions.add()
                                .index(followerIndex.getName())
                                .alias(alias.alias())
                                .filter(alias.filter() == null ? null : alias.filter().toString())
                                .indexRouting(alias.indexRouting())
                                .searchRouting(alias.searchRouting())
                                .writeIndex(false)
                        );
                    }

                    // update the aliases that are different (ignoring write aliases)
                    for (final var aliasName : aliasesInCommon) {
                        final var leaderAliasMetadata = leaderIndexMetadata.getAliases().get(aliasName);
                        // we intentionally override that the alias is not a write alias as follower indices do not receive direct writes
                        final var leaderAliasMetadataWithoutWriteIndex = new AliasMetadata.Builder(aliasName).filter(
                            leaderAliasMetadata.filter()
                        )
                            .indexRouting(leaderAliasMetadata.indexRouting())
                            .searchRouting(leaderAliasMetadata.searchRouting())
                            .writeIndex(false)
                            .build();
                        final var followerAliasMetadata = followerIndexMetadata.getAliases().get(aliasName);
                        if (leaderAliasMetadataWithoutWriteIndex.equals(followerAliasMetadata)) {
                            // skip this alias, the leader and follower have the same modulo the write index
                            continue;
                        }
                        // we intentionally override that the alias is not a write alias as follower indices do not receive direct writes
                        aliasActions.add(
                            IndicesAliasesRequest.AliasActions.add()
                                .index(followerIndex.getName())
                                .alias(leaderAliasMetadata.alias())
                                .filter(leaderAliasMetadata.filter() == null ? null : leaderAliasMetadata.filter().toString())
                                .indexRouting(leaderAliasMetadata.indexRouting())
                                .searchRouting(leaderAliasMetadata.searchRouting())
                                .writeIndex(false)
                        );
                    }

                    // remove aliases that the leader no longer has
                    for (final var aliasName : aliasesOnFollowerNotOnLeader) {
                        aliasActions.add(IndicesAliasesRequest.AliasActions.remove().index(followerIndex.getName()).alias(aliasName));
                    }

                    if (aliasActions.isEmpty()) {
                        handler.accept(leaderIndexMetadata.getAliasesVersion());
                    } else {
                        final var request = new IndicesAliasesRequest(TimeValue.MAX_VALUE, TimeValue.ZERO);
                        request.origin("ccr");
                        aliasActions.forEach(request::addAliasAction);
                        followerClient.admin()
                            .indices()
                            .aliases(
                                request,
                                ActionListener.wrap(r -> handler.accept(leaderIndexMetadata.getAliasesVersion()), errorHandler)
                            );
                    }
                };

                try {
                    remoteClient(params).execute(
                        ClusterStateAction.REMOTE_TYPE,
                        CcrRequests.metadataRequest(leaderIndex.getName()),
                        ActionListener.wrap(onResponse, errorHandler)
                    );
                } catch (final NoSuchRemoteClusterException e) {
                    errorHandler.accept(e);
                }
            }

            private void closeIndexUpdateSettingsAndOpenIndex(
                String followIndex,
                Settings updatedSettings,
                Runnable handler,
                Consumer<Exception> onFailure
            ) {
                CloseIndexRequest closeRequest = new CloseIndexRequest(followIndex).masterNodeTimeout(TimeValue.MAX_VALUE);
                CheckedConsumer<CloseIndexResponse, Exception> onResponse = response -> {
                    updateSettingsAndOpenIndex(followIndex, updatedSettings, handler, onFailure);
                };
                followerClient.admin().indices().close(closeRequest, ActionListener.wrap(onResponse, onFailure));
            }

            private void updateSettingsAndOpenIndex(
                String followIndex,
                Settings updatedSettings,
                Runnable handler,
                Consumer<Exception> onFailure
            ) {
                final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(followIndex).masterNodeTimeout(
                    TimeValue.MAX_VALUE
                );
                updateSettingsRequest.settings(updatedSettings);
                CheckedConsumer<AcknowledgedResponse, Exception> onResponse = response -> openIndex(followIndex, handler, onFailure);
                followerClient.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(onResponse, onFailure));
            }

            private void openIndex(String followIndex, Runnable handler, Consumer<Exception> onFailure) {
                OpenIndexRequest openIndexRequest = new OpenIndexRequest(followIndex).masterNodeTimeout(TimeValue.MAX_VALUE);
                CheckedConsumer<OpenIndexResponse, Exception> onResponse = response -> handler.run();
                followerClient.admin().indices().open(openIndexRequest, ActionListener.wrap(onResponse, onFailure));
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(
                final String followerHistoryUUID,
                final List<Translog.Operation> operations,
                final long maxSeqNoOfUpdatesOrDeletes,
                final Consumer<BulkShardOperationsResponse> handler,
                final Consumer<Exception> errorHandler
            ) {

                final BulkShardOperationsRequest request = new BulkShardOperationsRequest(
                    params.getFollowShardId(),
                    followerHistoryUUID,
                    operations,
                    maxSeqNoOfUpdatesOrDeletes
                );
                followerClient.execute(BulkShardOperationsAction.INSTANCE, request, ActionListener.wrap(handler::accept, errorHandler));
            }

            @Override
            protected void innerSendShardChangesRequest(
                long from,
                int maxOperationCount,
                Consumer<ShardChangesAction.Response> handler,
                Consumer<Exception> errorHandler
            ) {
                ShardChangesAction.Request request = new ShardChangesAction.Request(
                    params.getLeaderShardId(),
                    recordedLeaderShardHistoryUUID
                );
                request.setFromSeqNo(from);
                request.setMaxOperationCount(maxOperationCount);
                request.setMaxBatchSize(params.getMaxReadRequestSize());
                request.setPollTimeout(params.getReadPollTimeout());
                try {
                    remoteClient(params).execute(
                        ShardChangesAction.REMOTE_TYPE,
                        request,
                        ActionListener.wrap(handler::accept, errorHandler)
                    );
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
                    params.getLeaderShardId().getIndex()
                );

                /*
                 * We are going to attempt to renew the retention lease. If this fails it is either because the retention lease does not
                 * exist, or something else happened. If the retention lease does not exist, we will attempt to add the retention lease
                 * again. If that fails, it had better not be because the retention lease already exists. Either way, we will attempt to
                 * renew again on the next scheduled execution.
                 */
                final ActionListener<ActionResponse.Empty> listener = ActionListener.wrap(r -> {}, e -> {
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
                            retentionLeaseId
                        );
                        try {
                            final ActionListener<ActionResponse.Empty> wrappedListener = ActionListener.wrap(r -> {}, inner -> {
                                /*
                                 * If this fails that the retention lease already exists, something highly unusual is
                                 * going on. Log it, and renew again after another renew interval has passed.
                                 */
                                final Throwable innerCause = ExceptionsHelper.unwrapCause(inner);
                                logRetentionLeaseFailure(retentionLeaseId, innerCause);
                            });
                            CcrRetentionLeases.asyncAddRetentionLease(
                                params.getLeaderShardId(),
                                retentionLeaseId,
                                followerGlobalCheckpoint.getAsLong() + 1,
                                remoteClient(params),
                                wrappedListener
                            );
                        } catch (NoSuchRemoteClusterException rce) {
                            // we will attempt to renew again after another renew interval has passed
                            logRetentionLeaseFailure(retentionLeaseId, rce);
                        }
                    } else {
                        // if something else happened, we will attempt to renew again after another renew interval has passed
                    }
                });

                return threadPool.scheduleWithFixedDelay(() -> {
                    logger.trace(
                        "{} background renewing retention lease [{}] while following",
                        params.getFollowShardId(),
                        retentionLeaseId
                    );
                    CcrRetentionLeases.asyncRenewRetentionLease(
                        params.getLeaderShardId(),
                        retentionLeaseId,
                        followerGlobalCheckpoint.getAsLong() + 1,
                        remoteClient(params),
                        listener
                    );
                }, retentionLeaseRenewInterval, ccrExecutor);
            }

            private void logRetentionLeaseFailure(final String retentionLeaseId, final Throwable cause) {
                assert cause instanceof ElasticsearchSecurityException == false : cause;
                if (cause instanceof RetentionLeaseInvalidRetainingSeqNoException == false) {
                    logger.warn(
                        () -> format(
                            "%s background management of retention lease [%s] failed while following",
                            params.getFollowShardId(),
                            retentionLeaseId
                        ),
                        cause
                    );
                }
            }

        };
    }

    private String getLeaderShardHistoryUUID(ShardFollowTask params) {
        IndexMetadata followIndexMetadata = clusterService.state().metadata().getProject().index(params.getFollowShardId().getIndex());
        Map<String, String> ccrIndexMetadata = followIndexMetadata.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
        String[] recordedLeaderShardHistoryUUIDs = extractLeaderShardHistoryUUIDs(ccrIndexMetadata);
        return recordedLeaderShardHistoryUUIDs[params.getLeaderShardId().id()];
    }

    private RemoteClusterClient remoteClient(ShardFollowTask params) {
        // TODO: do we need minNodeVersion here since it is for remote cluster
        return wrapRemoteClusterClient(
            client.threadPool().getThreadContext(),
            client.getRemoteClusterClient(
                params.getRemoteCluster(),
                // this client is only used for lightweight single-index metadata responses and for the shard-changes actions themselves
                // which are about as easy to parse as shard bulks, and which handle their own forking, so we can handle responses on the
                // transport thread
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
            ),
            params.getHeaders(),
            clusterService.state()
        );
    }

    interface FollowerStatsInfoHandler {
        void accept(String followerHistoryUUID, long globalCheckpoint, long maxSeqNo);
    }

    @Override
    protected void nodeOperation(final AllocatedPersistentTask task, final ShardFollowTask params, final PersistentTaskState state) {
        Client followerClient = wrapClient(client, params.getHeaders(), clusterService.state());
        ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
        logger.info("{} Starting to track leader shard {}", params.getFollowShardId(), params.getLeaderShardId());

        FollowerStatsInfoHandler handler = (followerHistoryUUID, followerGCP, maxSeqNo) -> {
            shardFollowNodeTask.start(followerHistoryUUID, followerGCP, maxSeqNo, followerGCP, maxSeqNo);
        };
        Consumer<Exception> errorHandler = e -> {
            if (shardFollowNodeTask.isStopped()) {
                return;
            }

            if (ShardFollowNodeTask.shouldRetry(e)) {
                logger.debug(
                    () -> format("failed to fetch follow shard global %s checkpoint and max sequence number", shardFollowNodeTask),
                    e
                );
                try {
                    threadPool.schedule(() -> nodeOperation(task, params, state), params.getMaxRetryDelay(), ccrExecutor);
                } catch (EsRejectedExecutionException rex) {
                    rex.addSuppressed(e);
                    shardFollowNodeTask.onFatalFailure(rex);
                }
            } else {
                shardFollowNodeTask.onFatalFailure(e);
            }
        };

        fetchFollowerShardInfo(followerClient, params.getFollowShardId(), handler, errorHandler);
    }

    private void fetchFollowerShardInfo(
        final Client followerClient,
        final ShardId shardId,
        final FollowerStatsInfoHandler handler,
        final Consumer<Exception> errorHandler
    ) {
        followerClient.admin().indices().stats(new IndicesStatsRequest().indices(shardId.getIndexName()), ActionListener.wrap(r -> {
            IndexStats indexStats = r.getIndex(shardId.getIndexName());
            if (indexStats == null) {
                IndexMetadata indexMetadata = clusterService.state().metadata().getProject().index(shardId.getIndex());
                if (indexMetadata != null) {
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
