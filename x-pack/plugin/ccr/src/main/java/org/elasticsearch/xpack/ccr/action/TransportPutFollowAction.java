/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.respository.RemoteClusterRepository;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class TransportPutFollowAction
    extends TransportMasterNodeAction<PutFollowAction.Request, PutFollowAction.Response> {

    private final Client client;
    private final AllocationService allocationService;
    private final RestoreService restoreService;
    private final ActiveShardsObserver activeShardsObserver;
    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportPutFollowAction(
        final ThreadPool threadPool,
        final TransportService transportService,
        final ClusterService clusterService,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Client client,
        final AllocationService allocationService,
        final RestoreService restoreService,
        final CcrLicenseChecker ccrLicenseChecker) {
        super(
            PutFollowAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutFollowAction.Request::new,
            indexNameExpressionResolver);
        this.client = client;
        this.allocationService = allocationService;
        this.restoreService = restoreService;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutFollowAction.Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected PutFollowAction.Response read(StreamInput in) throws IOException {
        return new PutFollowAction.Response(in);
    }

    @Override
    protected void masterOperation(
        final PutFollowAction.Request request,
        final ClusterState state,
        final ActionListener<PutFollowAction.Response> listener) throws Exception {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        String remoteCluster = request.getRemoteCluster();
        // Validates whether the leader cluster has been configured properly:
        client.getRemoteClusterClient(remoteCluster);

        String leaderIndex = request.getLeaderIndex();
        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
            client,
            remoteCluster,
            leaderIndex,
            listener::onFailure,
            (historyUUID, leaderIndexMetaData) -> createFollowerIndex(leaderIndexMetaData, historyUUID, request, listener));
    }

    private void createFollowerIndex(
        final IndexMetaData leaderIndexMetaData,
        final String[] historyUUIDs,
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener) {
        if (leaderIndexMetaData == null) {
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not exist"));
            return;
        }
        if (leaderIndexMetaData.getSettings().getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false) == false) {
            listener.onFailure(
                new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not have soft deletes enabled"));
            return;
        }

        ActionListener<Boolean> handler2 = ActionListener.wrap(
            result -> {
                if (result) {
                    initiateFollowing(request, listener);
                } else {
                    listener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            },
            listener::onFailure);

        ActionListener<RestoreSnapshotResponse> handler = ActionListener.wrap(result -> {
                if (result != null) {
                    initiateFollowing(request, listener);
                } else {
                    listener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            },
            listener::onFailure);

        Settings.Builder settings = Settings.builder()
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);

        String remoteCluster = request.getRemoteCluster();
        RestoreService.RestoreRequest restoreRequest = restoreRequest = new RestoreService.RestoreRequest("_ccr_repository_",
            remoteCluster, request.indices(), request.indicesOptions(), "^(.*)$", request.getFollowRequest().getFollowerIndex(),
            Settings.EMPTY, request.masterNodeTimeout(), false, false, true, settings.build(), new String[0],
            "restore_snapshot[" + remoteCluster + "]");


        restoreService.restoreSnapshot(restoreRequest, new ActionListener<RestoreService.RestoreCompletionResponse>() {
            @Override
            public void onResponse(RestoreService.RestoreCompletionResponse restoreCompletionResponse) {
                final Snapshot snapshot = restoreCompletionResponse.getSnapshot();

                ClusterStateListener clusterStateListener = new ClusterStateListener() {
                    @Override
                    public void clusterChanged(ClusterChangedEvent changedEvent) {
                        final RestoreInProgress.Entry prevEntry = RestoreService.restoreInProgress(changedEvent.previousState(), snapshot);
                        final RestoreInProgress.Entry newEntry = RestoreService.restoreInProgress(changedEvent.state(), snapshot);
                        if (prevEntry == null) {
                            // When there is a master failure after a restore has been started, this listener might not be registered
                            // on the current master and as such it might miss some intermediary cluster states due to batching.
                            // Clean up listener in that case and acknowledge completion of restore operation to client.
                            clusterService.removeListener(this);
                            handler.onResponse(null);
                        } else if (newEntry == null) {
                            clusterService.removeListener(this);
                            ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards = prevEntry.shards();
                            assert prevEntry.state().completed() : "expected completed snapshot state but was " + prevEntry.state();
                            assert RestoreService.completed(shards) : "expected all restore entries to be completed";
                            RestoreInfo restoreInfo = new RestoreInfo(prevEntry.snapshot().getSnapshotId().getName(), prevEntry.indices(),
                                shards.size(), shards.size() - RestoreService.failedShards(shards));
                            RestoreSnapshotResponse response = null;
                            logger.debug("restore of [{}] completed", snapshot);
                            handler.onResponse(response);
                        } else {
                            // restore not completed yet, wait for next cluster state update
                        }
                    }
                };

                clusterService.addListener(clusterStateListener);
            }

            @Override
            public void onFailure(Exception t) {
                listener.onFailure(t);
            }
        }, false, false);


//         Can't use create index api here, because then index templates can alter the mappings / settings.
//         And index templates could introduce settings / mappings that are incompatible with the leader index.
        clusterService.submitStateUpdateTask("create_following_index", new AckedClusterStateUpdateTask<Boolean>(request, handler2) {

            @Override
            protected Boolean newResponse(final boolean acknowledged) {
                return acknowledged;
            }

            @Override
            public ClusterState execute(final ClusterState currentState) throws Exception {
                String followIndex = request.getFollowRequest().getFollowerIndex();
                IndexMetaData currentIndex = currentState.metaData().index(followIndex);
                if (currentIndex != null) {
                    throw new ResourceAlreadyExistsException(currentIndex.getIndex());
                }

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                IndexMetaData.Builder imdBuilder = IndexMetaData.builder(followIndex);

                // Adding the leader index uuid for each shard as custom metadata:
                Map<String, String> metadata = new HashMap<>();
                metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS, String.join(",", historyUUIDs));
                metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY, leaderIndexMetaData.getIndexUUID());
                metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY, leaderIndexMetaData.getIndex().getName());
                metadata.put(Ccr.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY, request.getRemoteCluster());
                imdBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, metadata);

                // Copy all settings, but overwrite a few settings.
                Settings.Builder settingsBuilder = Settings.builder();
                settingsBuilder.put(leaderIndexMetaData.getSettings());
                // Overwriting UUID here, because otherwise we can't follow indices in the same cluster
                settingsBuilder.put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
                settingsBuilder.put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followIndex);
                settingsBuilder.put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);

                // TODO: Remove as this should be inherited from the leader (must be true - validated)
//                settingsBuilder.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
                imdBuilder.settings(settingsBuilder);

                // Copy mappings from leader IMD to follow IMD
                for (ObjectObjectCursor<String, MappingMetaData> cursor : leaderIndexMetaData.getMappings()) {
                    imdBuilder.putMapping(cursor.value);
                }
                imdBuilder.setRoutingNumShards(leaderIndexMetaData.getRoutingNumShards());
                IndexMetaData followIMD = imdBuilder.build();
                mdBuilder.put(followIMD, false);

                ClusterState.Builder builder = ClusterState.builder(currentState);
                builder.metaData(mdBuilder.build());
                ClusterState updatedState = builder.build();

                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                        .addAsNew(updatedState.metaData().index(request.getFollowRequest().getFollowerIndex()));
                updatedState = allocationService.reroute(
                        ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                        "follow index [" + request.getFollowRequest().getFollowerIndex() + "] created");

                logger.info("[{}] creating index, cause [ccr_create_and_follow], shards [{}]/[{}]",
                        followIndex, followIMD.getNumberOfShards(), followIMD.getNumberOfReplicas());

                return updatedState;
            }
        });
    }

    private void initiateFollowing(
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener) {
        activeShardsObserver.waitForActiveShards(new String[]{request.getFollowRequest().getFollowerIndex()},
            ActiveShardCount.DEFAULT, request.timeout(), result -> {
                if (result) {
                    client.execute(ResumeFollowAction.INSTANCE, request.getFollowRequest(), ActionListener.wrap(
                        r -> listener.onResponse(new PutFollowAction.Response(true, true, r.isAcknowledged())),
                        listener::onFailure
                    ));
                } else {
                    listener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            }, listener::onFailure);
    }

    @Override
    protected ClusterBlockException checkBlock(final PutFollowAction.Request request, final ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowRequest().getFollowerIndex());
    }

}
