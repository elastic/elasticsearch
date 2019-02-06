/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.util.HashMap;
import java.util.Map;

final class Pre67PutFollow {

    private static final Logger logger = LogManager.getLogger(Pre67PutFollow.class);

    private final Client client;
    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final ActiveShardsObserver activeShardsObserver;

    Pre67PutFollow(final Client client, final ClusterService clusterService, final AllocationService allocationService,
                   final ActiveShardsObserver activeShardsObserver) {
        this.client = client;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.activeShardsObserver = activeShardsObserver;
    }

    void doPre67PutFollow(final PutFollowAction.Request request, final IndexMetaData leaderIndexMetaData, String[] historyUUIDs,
                          final ActionListener<PutFollowAction.Response> listener) {
        ActionListener<Boolean> handler = ActionListener.wrap(
            result -> {
                if (result) {
                    initiateFollowing(request, listener);
                } else {
                    listener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            },
            listener::onFailure);
        // Can't use create index api here, because then index templates can alter the mappings / settings.
        // And index templates could introduce settings / mappings that are incompatible with the leader index.
        clusterService.submitStateUpdateTask("create_following_index", new AckedClusterStateUpdateTask<Boolean>(request, handler) {

            @Override
            protected Boolean newResponse(final boolean acknowledged) {
                return acknowledged;
            }

            @Override
            public ClusterState execute(final ClusterState currentState) throws Exception {
                String followIndex = request.getFollowerIndex();
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
                settingsBuilder.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
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
                    .addAsNew(updatedState.metaData().index(request.getFollowerIndex()));
                updatedState = allocationService.reroute(
                    ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                    "follow index [" + request.getFollowerIndex() + "] created");

                logger.info("[{}] creating index, cause [ccr_create_and_follow], shards [{}]/[{}]",
                    followIndex, followIMD.getNumberOfShards(), followIMD.getNumberOfReplicas());

                return updatedState;
            }
        });
    }

    private void initiateFollowing(final PutFollowAction.Request request, final ActionListener<PutFollowAction.Response> listener) {
        activeShardsObserver.waitForActiveShards(new String[]{request.getFollowerIndex()},
            ActiveShardCount.DEFAULT, request.timeout(), result -> {
                if (result) {
                    ResumeFollowAction.Request resumeFollowRequest = new ResumeFollowAction.Request();
                    resumeFollowRequest.setFollowerIndex(request.getFollowerIndex());
                    resumeFollowRequest.setParameters(request.getParameters());
                    client.execute(ResumeFollowAction.INSTANCE, resumeFollowRequest, ActionListener.wrap(
                        r -> listener.onResponse(new PutFollowAction.Response(true, true, r.isAcknowledged())),
                        listener::onFailure
                    ));
                } else {
                    listener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            }, listener::onFailure);
    }
}
