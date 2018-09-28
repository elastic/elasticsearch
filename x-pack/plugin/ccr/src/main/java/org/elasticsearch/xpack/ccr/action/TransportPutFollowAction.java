/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public final class TransportPutFollowAction
        extends TransportMasterNodeAction<PutFollowAction.Request, PutFollowAction.Response> {

    private final Client client;
    private final AllocationService allocationService;
    private final RemoteClusterService remoteClusterService;
    private final ActiveShardsObserver activeShardsObserver;
    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportPutFollowAction(
            final Settings settings,
            final ThreadPool threadPool,
            final TransportService transportService,
            final ClusterService clusterService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final Client client,
            final AllocationService allocationService,
            final CcrLicenseChecker ccrLicenseChecker) {
        super(
                settings,
                PutFollowAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                indexNameExpressionResolver,
                PutFollowAction.Request::new);
        this.client = client;
        this.allocationService = allocationService;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.activeShardsObserver = new ActiveShardsObserver(settings, clusterService, threadPool);
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutFollowAction.Response newResponse() {
        return new PutFollowAction.Response();
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
        final String[] indices = new String[]{request.getFollowRequest().getLeaderIndex()};
        final Map<String, List<String>> remoteClusterIndices = remoteClusterService.groupClusterIndices(indices, s -> false);
        if (remoteClusterIndices.containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
            createFollowerIndexAndFollowLocalIndex(request, state, listener);
        } else {
            assert remoteClusterIndices.size() == 1;
            final Map.Entry<String, List<String>> entry = remoteClusterIndices.entrySet().iterator().next();
            assert entry.getValue().size() == 1;
            final String clusterAlias = entry.getKey();
            final String leaderIndex = entry.getValue().get(0);
            createFollowerIndexAndFollowRemoteIndex(request, clusterAlias, leaderIndex, listener);
        }
    }

    private void createFollowerIndexAndFollowLocalIndex(
            final PutFollowAction.Request request,
            final ClusterState state,
            final ActionListener<PutFollowAction.Response> listener) {
        // following an index in local cluster, so use local cluster state to fetch leader index metadata
        final String leaderIndex = request.getFollowRequest().getLeaderIndex();
        final IndexMetaData leaderIndexMetadata = state.getMetaData().index(leaderIndex);
        if (leaderIndexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(leaderIndex));
            return;
        }

        Consumer<String[]> historyUUIDhandler = historyUUIDs -> {
            createFollowerIndex(leaderIndexMetadata, historyUUIDs, request, listener);
        };
        ccrLicenseChecker.hasPrivilegesToFollowIndices(client, new String[] {leaderIndex}, e -> {
            if (e == null) {
                ccrLicenseChecker.fetchHistoryUUIDs(client, leaderIndexMetadata, listener::onFailure, historyUUIDhandler);
            } else {
                listener.onFailure(e);
            }
        });
    }

    private void createFollowerIndexAndFollowRemoteIndex(
            final PutFollowAction.Request request,
            final String clusterAlias,
            final String leaderIndex,
            final ActionListener<PutFollowAction.Response> listener) {
        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
                client,
                clusterAlias,
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
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getFollowRequest().getLeaderIndex() +
                    "] does not exist"));
            return;
        }

        // Can't use create index api here, because then index templates can alter the mappings / settings.
        // And index templates could introduce settings / mappings that are incompatible with the leader index.
        clusterService.submitStateUpdateTask("create_follow_index", new ClusterStateUpdateTask() {

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
                imdBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, metadata);

                // Copy all settings, but overwrite a few settings.
                Settings.Builder settingsBuilder = Settings.builder();
                settingsBuilder.put(leaderIndexMetaData.getSettings());
                // Overwriting UUID here, because otherwise we can't follow indices in the same cluster
                settingsBuilder.put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
                settingsBuilder.put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followIndex);
                settingsBuilder.put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
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

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                IndexMetaData followerIndexMeta = newState.metaData().index(request.getFollowRequest().getFollowerIndex());
                initiateFollowing(request, followerIndexMeta, listener);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void initiateFollowing(
            final PutFollowAction.Request request,
            final IndexMetaData followerIndexMeta,
            final ActionListener<PutFollowAction.Response> listener) {
        activeShardsObserver.waitForActiveShards(new String[]{request.getFollowRequest().getFollowerIndex()},
                ActiveShardCount.DEFAULT, request.timeout(), result -> {
                    if (result) {
                        ccrLicenseChecker.fetchHistoryUUIDs(client, followerIndexMeta, listener::onFailure, historyUUIDs -> {
                            clusterService.submitStateUpdateTask("record-follower-index-uuids", new ClusterStateUpdateTask() {
                                @Override
                                public ClusterState execute(ClusterState currentState) throws Exception {
                                    return recordFollowerShardHistoryUUIDs(currentState, followerIndexMeta.getIndex(), historyUUIDs);
                                }

                                @Override
                                public void onFailure(String source, Exception e) {
                                    listener.onFailure(e);
                                }

                                @Override
                                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                    client.execute(ResumeFollowAction.INSTANCE, request.getFollowRequest(), ActionListener.wrap(
                                        r -> listener.onResponse(new PutFollowAction.Response(true, true, r.isAcknowledged())),
                                        e -> {
                                            logger.error("Failed to initiate the shard following", e);
                                            listener.onResponse(new PutFollowAction.Response(true, true, false));
                                        }
                                    ));
                                }
                            });
                        });
                    } else {
                        listener.onFailure(new ElasticsearchException("creation of leader index was not acked"));
                    }
                }, listener::onFailure);
    }

    @Override
    protected ClusterBlockException checkBlock(final PutFollowAction.Request request, final ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowRequest().getFollowerIndex());
    }

    private static ClusterState recordFollowerShardHistoryUUIDs(ClusterState current,
                                                                Index followerIndex,
                                                                String[] historyUUIDs) {

        IndexMetaData currentFollowerIndexMetaData = current.metaData().index(followerIndex);
        IndexMetaData.Builder followIndexMetaDataBuilder = IndexMetaData.builder(currentFollowerIndexMetaData);
        Map<String, String> ccrCustom = new HashMap<>(currentFollowerIndexMetaData.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY));
        ccrCustom.put(Ccr.CCR_CUSTOM_METADATA_FOLLOWER_INDEX_SHARD_HISTORY_UUIDS, String.join(",", historyUUIDs));
        followIndexMetaDataBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, ccrCustom);

        return ClusterState.builder(current)
            .metaData(MetaData.builder(current.metaData())
                .put(followIndexMetaDataBuilder)
                .build())
            .build();
    }

}
