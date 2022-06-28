/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CcrAutoFollowInfoFetcher {
    private CcrAutoFollowInfoFetcher() {}

    public static void getAutoFollowedSystemIndices(Client client, ClusterState state, ActionListener<List<String>> listener) {
        final AutoFollowMetadata autoFollowMetadata = state.metadata().custom(AutoFollowMetadata.TYPE);
        final PersistentTasksCustomMetadata persistentTasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);

        if (autoFollowMetadata == null || areShardFollowTasksRunning(persistentTasks) == false) {
            listener.onResponse(Collections.emptyList());
            return;
        }

        final List<String> followedLeaderIndexUUIDs = autoFollowMetadata.getFollowedLeaderIndexUUIDs()
            .values()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        final Map<String, List<AutoFollowedIndex>> remoteClusterAutoFollowedIndices = new HashMap<>();
        for (ObjectObjectCursor<String, IndexMetadata> indexEntry : state.metadata().getIndices()) {
            final IndexMetadata indexMetadata = indexEntry.value;
            final Map<String, String> ccrMetadata = indexMetadata.getCustomData(CcrConstants.CCR_CUSTOM_METADATA_KEY);
            if (ccrMetadata == null) {
                continue;
            }

            final String leaderIndexUUID = ccrMetadata.get(CcrConstants.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
            final String leaderIndexName = ccrMetadata.get(CcrConstants.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY);
            final String remoteCluster = ccrMetadata.get(CcrConstants.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY);

            final Index followerIndex = indexMetadata.getIndex();
            if (followedLeaderIndexUUIDs.contains(leaderIndexUUID) && isCurrentlyFollowed(persistentTasks, followerIndex)) {
                List<AutoFollowedIndex> autoFollowedIndices = remoteClusterAutoFollowedIndices.computeIfAbsent(
                    remoteCluster,
                    unused -> new ArrayList<>()
                );

                autoFollowedIndices.add(new AutoFollowedIndex(leaderIndexName, followerIndex.getName()));
            }
        }

        if (remoteClusterAutoFollowedIndices.isEmpty()) {
            listener.onResponse(Collections.emptyList());
            return;
        }

        GroupedActionListener<List<String>> clusterResponsesListener = new GroupedActionListener<>(
            listener.map(
                (clusterAutoFollowedSystemIndices) -> clusterAutoFollowedSystemIndices.stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList())
            ),
            remoteClusterAutoFollowedIndices.size()
        );

        // When a system index is followed we don't copy over the isSystem flag into IndexMetadata
        // We need to fetch the remote cluster state in order to check whether or not the following index
        // follows a leader system index.
        for (Map.Entry<String, List<AutoFollowedIndex>> remoteAutoFollowedIndicesEntry : remoteClusterAutoFollowedIndices.entrySet()) {
            final String remoteCluster = remoteAutoFollowedIndicesEntry.getKey();
            final List<AutoFollowedIndex> autoFollowedIndices = remoteAutoFollowedIndicesEntry.getValue();

            try {
                client.getRemoteClusterClient(remoteCluster)
                    .admin()
                    .cluster()
                    .prepareState()
                    .clear()
                    .setMetadata(true)
                    .execute(new ActionListener<ClusterStateResponse>() {
                        @Override
                        public void onResponse(ClusterStateResponse stateResponse) {
                            final ClusterState clusterState = stateResponse.getState();
                            List<String> autoFollowedSystemIndices = new ArrayList<>();
                            for (AutoFollowedIndex autoFollowedIndex : autoFollowedIndices) {
                                final IndexAbstraction indexAbstraction = clusterState.metadata()
                                    .getIndicesLookup()
                                    .get(autoFollowedIndex.remoteIndexName);
                                if (indexAbstraction != null && indexAbstraction.isSystem()) {
                                    autoFollowedSystemIndices.add(autoFollowedIndex.localIndexName);
                                }
                            }
                            clusterResponsesListener.onResponse(autoFollowedSystemIndices);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            clusterResponsesListener.onFailure(e);
                        }
                    });
            } catch (IllegalArgumentException e) {
                clusterResponsesListener.onFailure(e);
            }
        }
    }

    private static boolean isCurrentlyFollowed(PersistentTasksCustomMetadata persistentTasks, Index index) {
        return persistentTasks != null
            && persistentTasks.findTasks(ShardFollowTask.NAME, task -> true)
                .stream()
                .map(task -> (ShardFollowTask) task.getParams())
                .anyMatch(shardFollowTask -> index.equals(shardFollowTask.getFollowShardId().getIndex()));
    }

    private static boolean areShardFollowTasksRunning(PersistentTasksCustomMetadata persistentTasks) {
        return persistentTasks != null && persistentTasks.findTasks(ShardFollowTask.NAME, task -> true).isEmpty() == false;
    }

    private static class AutoFollowedIndex {
        private final String remoteIndexName;
        private final String localIndexName;

        private AutoFollowedIndex(String remoteIndexName, String localIndexName) {
            this.remoteIndexName = remoteIndexName;
            this.localIndexName = localIndexName;
        }
    }
}
