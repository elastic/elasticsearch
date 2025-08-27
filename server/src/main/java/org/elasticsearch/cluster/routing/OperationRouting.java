/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OperationRouting {

    public static final Setting<Boolean> USE_ADAPTIVE_REPLICA_SELECTION_SETTING = Setting.boolSetting(
        "cluster.routing.use_adaptive_replica_selection",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private boolean useAdaptiveReplicaSelection;

    @SuppressWarnings("this-escape")
    public OperationRouting(Settings settings, ClusterSettings clusterSettings) {
        this.useAdaptiveReplicaSelection = USE_ADAPTIVE_REPLICA_SELECTION_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(USE_ADAPTIVE_REPLICA_SELECTION_SETTING, this::setUseAdaptiveReplicaSelection);
    }

    void setUseAdaptiveReplicaSelection(boolean useAdaptiveReplicaSelection) {
        this.useAdaptiveReplicaSelection = useAdaptiveReplicaSelection;
    }

    /**
     * Shards to use for a {@code GET} operation.
     * @return A shard iterator that can be used for GETs, or null if e.g. due to preferences no match is found.
     */
    public ShardIterator getShards(
        ProjectState projectState,
        String index,
        String id,
        @Nullable String routing,
        @Nullable String preference
    ) {
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata(projectState.metadata(), index));
        IndexShardRoutingTable shards = projectState.routingTable().shardRoutingTable(index, indexRouting.getShard(id, routing));
        DiscoveryNodes nodes = projectState.cluster().nodes();
        return preferenceActiveShardIterator(shards, nodes.getLocalNodeId(), nodes, preference, null, null);
    }

    public ShardIterator getShards(ProjectState projectState, String index, int shardId, @Nullable String preference) {
        IndexShardRoutingTable indexShard = projectState.routingTable().shardRoutingTable(index, shardId);
        DiscoveryNodes nodes = projectState.cluster().nodes();
        return preferenceActiveShardIterator(indexShard, nodes.getLocalNodeId(), nodes, preference, null, null);
    }

    public List<ShardIterator> searchShards(
        ProjectState projectState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference
    ) {
        return searchShards(projectState, concreteIndices, routing, preference, null, null);
    }

    public List<ShardIterator> searchShards(
        ProjectState projectState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts
    ) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(projectState, concreteIndices, routing);
        DiscoveryNodes nodes = projectState.cluster().nodes();
        List<ShardIterator> res = new ArrayList<>(shards.size());
        for (IndexShardRoutingTable shard : shards) {
            ShardIterator iterator = preferenceActiveShardIterator(
                shard,
                nodes.getLocalNodeId(),
                nodes,
                preference,
                collectorService,
                nodeCounts
            );
            if (iterator != null) {
                res.add(ShardIterator.allSearchableShards(iterator));
            }
        }
        res.sort(ShardIterator::compareTo);
        return res;
    }

    public Iterator<IndexShardRoutingTable> allWritableShards(ProjectState projectState, String index) {
        return allWriteAddressableShards(projectState, index);
    }

    public static ShardIterator getShards(RoutingTable routingTable, ShardId shardId) {
        final IndexShardRoutingTable shard = routingTable.shardRoutingTable(shardId);
        return shard.activeInitializingShardsRandomIt();
    }

    private static Set<IndexShardRoutingTable> computeTargetedShards(
        ProjectState projectState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing
    ) {
        // we use set here and not list since we might get duplicates
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        if (routing == null || routing.isEmpty()) {
            collectTargetShardsNoRouting(projectState, concreteIndices, set);
        } else {
            collectTargetShardsWithRouting(projectState, concreteIndices, routing, set);
        }
        return set;
    }

    private static void collectTargetShardsWithRouting(
        ProjectState projectState,
        String[] concreteIndices,
        Map<String, Set<String>> routing,
        Set<IndexShardRoutingTable> set
    ) {
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRoutingTable = indexRoutingTable(projectState.routingTable(), index);
            final Set<String> indexSearchRouting = routing.get(index);
            if (indexSearchRouting != null) {
                IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata(projectState.metadata(), index));
                for (String r : indexSearchRouting) {
                    indexRouting.collectSearchShards(r, s -> set.add(RoutingTable.shardRoutingTable(indexRoutingTable, s)));
                }
            } else {
                Iterator<IndexShardRoutingTable> iterator = allSearchAddressableShards(projectState, index);
                while (iterator.hasNext()) {
                    set.add(iterator.next());
                }
            }
        }
    }

    private static void collectTargetShardsNoRouting(ProjectState projectState, String[] concreteIndices, Set<IndexShardRoutingTable> set) {
        for (String index : concreteIndices) {
            Iterator<IndexShardRoutingTable> iterator = allSearchAddressableShards(projectState, index);
            while (iterator.hasNext()) {
                set.add(iterator.next());
            }
        }
    }

    /**
     * Returns an iterator of shards that can possibly serve searches. A shard may not be addressable during processes like resharding.
     * This logic is not related to shard state or a recovery process. A shard returned here may f.e. be unassigned.
     */
    private static Iterator<IndexShardRoutingTable> allSearchAddressableShards(ProjectState projectState, String index) {
        return allShardsExceptSplitTargetsInStateBefore(projectState, index, IndexReshardingState.Split.TargetShardState.SPLIT);
    }

    /**
     * Returns an iterator of shards that can possibly serve writes. A shard may not be addressable during processes like resharding.
     * This logic is not related to shard state or a recovery process. A shard returned here may f.e. be unassigned.
     */
    private static Iterator<IndexShardRoutingTable> allWriteAddressableShards(ProjectState projectState, String index) {
        return allShardsExceptSplitTargetsInStateBefore(projectState, index, IndexReshardingState.Split.TargetShardState.HANDOFF);
    }

    /**
     * Filters shards based on their state in resharding metadata. If resharing metadata is not present returns all shards.
     */
    private static Iterator<IndexShardRoutingTable> allShardsExceptSplitTargetsInStateBefore(
        ProjectState projectState,
        String index,
        IndexReshardingState.Split.TargetShardState targetShardState
    ) {
        final IndexRoutingTable indexRoutingTable = indexRoutingTable(projectState.routingTable(), index);
        final IndexMetadata indexMetadata = indexMetadata(projectState.metadata(), index);
        if (indexMetadata.getReshardingMetadata() == null) {
            return indexRoutingTable.allShards().iterator();
        }

        final IndexReshardingMetadata indexReshardingMetadata = indexMetadata.getReshardingMetadata();
        assert indexReshardingMetadata.isSplit();
        final IndexReshardingState.Split splitState = indexReshardingMetadata.getSplit();

        var shards = new ArrayList<IndexShardRoutingTable>();
        for (int i = 0; i < indexRoutingTable.size(); i++) {
            if (splitState.isTargetShard(i) == false || splitState.targetStateAtLeast(i, targetShardState)) {
                shards.add(indexRoutingTable.shard(i));
            }
        }
        return shards.iterator();
    }

    private ShardIterator preferenceActiveShardIterator(
        IndexShardRoutingTable indexShard,
        String localNodeId,
        DiscoveryNodes nodes,
        @Nullable String preference,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts
    ) {
        if (preference == null || preference.isEmpty()) {
            return shardRoutings(indexShard, collectorService, nodeCounts);
        }
        if (preference.charAt(0) == '_') {
            Preference preferenceType = Preference.parse(preference);
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf('|');

                String shards;
                if (index == -1) {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                } else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                for (String id : ids) {
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    return null;
                }
                // no more preference
                if (index == -1 || index == preference.length() - 1) {
                    return shardRoutings(indexShard, collectorService, nodeCounts);
                } else {
                    // update the preference and continue
                    preference = preference.substring(index + 1);
                }
            }
            if (preference.charAt(0) == '_') {
                preferenceType = Preference.parse(preference);
                switch (preferenceType) {
                    case PREFER_NODES:
                        final Set<String> nodesIds = Arrays.stream(
                            preference.substring(Preference.PREFER_NODES.type().length() + 1).split(",")
                        ).collect(Collectors.toSet());
                        return indexShard.preferNodeActiveInitializingShardsIt(nodesIds);
                    case LOCAL:
                        return indexShard.preferNodeActiveInitializingShardsIt(Collections.singleton(localNodeId));
                    case ONLY_LOCAL:
                        return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                    case ONLY_NODES:
                        String nodeAttributes = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                        return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttributes.split(","), nodes);
                    default:
                        throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
                }
            }
        }
        // if not, then use it as the index
        int routingHash = 31 * Murmur3HashFunction.hash(preference) + indexShard.shardId.hashCode();
        return indexShard.activeInitializingShardsIt(routingHash);
    }

    private ShardIterator shardRoutings(
        IndexShardRoutingTable indexShard,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts
    ) {
        if (useAdaptiveReplicaSelection) {
            return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts);
        } else {
            return indexShard.activeInitializingShardsRandomIt();
        }
    }

    protected static IndexRoutingTable indexRoutingTable(RoutingTable routingTable, String index) {
        IndexRoutingTable indexRouting = routingTable.index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }

    private static IndexMetadata indexMetadata(ProjectMetadata project, String index) {
        IndexMetadata indexMetadata = project.index(index);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetadata;
    }

    public static ShardId shardId(ProjectMetadata projectMetadata, String index, String id, @Nullable String routing) {
        IndexMetadata indexMetadata = indexMetadata(projectMetadata, index);
        return new ShardId(indexMetadata.getIndex(), IndexRouting.fromIndexMetadata(indexMetadata).getShard(id, routing));
    }
}
