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

    public List<SearchShardRouting> searchShards(
        ProjectState projectState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference
    ) {
        return searchShards(projectState, concreteIndices, routing, preference, null, null);
    }

    public List<SearchShardRouting> searchShards(
        ProjectState projectState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts
    ) {
        final Set<SearchTargetShard> shards = computeTargetedShards(projectState, concreteIndices, routing);
        DiscoveryNodes nodes = projectState.cluster().nodes();
        var res = new ArrayList<SearchShardRouting>(shards.size());
        for (SearchTargetShard targetShard : shards) {
            ShardIterator iterator = preferenceActiveShardIterator(
                targetShard.shardRoutingTable(),
                nodes.getLocalNodeId(),
                nodes,
                preference,
                collectorService,
                nodeCounts
            );
            if (iterator != null) {
                res.add(
                    SearchShardRouting.fromShardIterator(
                        ShardIterator.allSearchableShards(iterator),
                        targetShard.reshardSplitShardCountSummary()
                    )
                );
            }
        }
        res.sort(SearchShardRouting::compareTo);
        return res;
    }

    public Iterator<IndexShardRoutingTable> allWritableShards(ProjectState projectState, String index) {
        return allWriteAddressableShards(projectState, index);
    }

    public static ShardIterator getShards(RoutingTable routingTable, ShardId shardId) {
        final IndexShardRoutingTable shard = routingTable.shardRoutingTable(shardId);
        return shard.activeInitializingShardsRandomIt();
    }

    private record SearchTargetShard(IndexShardRoutingTable shardRoutingTable, SplitShardCountSummary reshardSplitShardCountSummary) {}

    private static Set<SearchTargetShard> computeTargetedShards(
        ProjectState projectState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing
    ) {
        // we use set here and not list since we might get duplicates
        if (routing == null || routing.isEmpty()) {
            return collectTargetShardsNoRouting(projectState, concreteIndices);
        }
        return collectTargetShardsWithRouting(projectState, concreteIndices, routing);
    }

    private static Set<SearchTargetShard> collectTargetShardsWithRouting(
        ProjectState projectState,
        String[] concreteIndices,
        Map<String, Set<String>> routing
    ) {
        var result = new HashSet<SearchTargetShard>();
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRoutingTable = indexRoutingTable(projectState.routingTable(), index);
            final Set<String> indexSearchRouting = routing.get(index);
            if (indexSearchRouting != null) {
                IndexMetadata indexMetadata = indexMetadata(projectState.metadata(), index);
                IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
                for (String r : indexSearchRouting) {
                    indexRouting.collectSearchShards(
                        r,
                        shardId -> result.add(
                            new SearchTargetShard(
                                RoutingTable.shardRoutingTable(indexRoutingTable, shardId),
                                SplitShardCountSummary.forSearch(indexMetadata, shardId)
                            )
                        )
                    );
                }
            } else {
                Iterator<SearchTargetShard> iterator = allSearchAddressableShards(projectState, index);
                iterator.forEachRemaining(result::add);
            }
        }
        return result;
    }

    private static Set<SearchTargetShard> collectTargetShardsNoRouting(ProjectState projectState, String[] concreteIndices) {
        var result = new HashSet<SearchTargetShard>();
        for (String index : concreteIndices) {
            Iterator<SearchTargetShard> iterator = allSearchAddressableShards(projectState, index);
            iterator.forEachRemaining(result::add);
        }
        return result;
    }

    /**
     * Returns an iterator of shards that can possibly serve searches. A shard may not be addressable during processes like resharding.
     * This logic is not related to shard state or a recovery process. A shard returned here may f.e. be unassigned.
     */
    private static Iterator<SearchTargetShard> allSearchAddressableShards(ProjectState projectState, String index) {
        final IndexRoutingTable indexRoutingTable = indexRoutingTable(projectState.routingTable(), index);
        final IndexMetadata indexMetadata = indexMetadata(projectState.metadata(), index);
        if (indexMetadata.getReshardingMetadata() == null) {
            return indexRoutingTable.allShards()
                .map(srt -> new SearchTargetShard(srt, SplitShardCountSummary.forSearch(indexMetadata, srt.shardId.id())))
                .iterator();
        }

        final IndexReshardingMetadata indexReshardingMetadata = indexMetadata.getReshardingMetadata();
        assert indexReshardingMetadata.isSplit();
        final IndexReshardingState.Split splitState = indexReshardingMetadata.getSplit();

        var shards = new ArrayList<SearchTargetShard>();
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            if (splitState.isTargetShard(shardId) == false
                || splitState.targetStateAtLeast(shardId, IndexReshardingState.Split.TargetShardState.SPLIT)) {
                shards.add(
                    new SearchTargetShard(indexRoutingTable.shard(shardId), SplitShardCountSummary.forSearch(indexMetadata, shardId))
                );
            }
        }
        return shards.iterator();
    }

    /**
     * Returns an iterator of shards that can possibly serve writes. A shard may not be addressable during processes like resharding.
     * This logic is not related to shard state or a recovery process. A shard returned here may f.e. be unassigned.
     */
    private static Iterator<IndexShardRoutingTable> allWriteAddressableShards(ProjectState projectState, String index) {
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
            if (splitState.isTargetShard(i) == false
                || splitState.targetStateAtLeast(i, IndexReshardingState.Split.TargetShardState.HANDOFF)) {
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
