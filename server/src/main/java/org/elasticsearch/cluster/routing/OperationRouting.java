/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class OperationRouting implements ClusterStateApplier {

    public static final Setting<Boolean> USE_ADAPTIVE_REPLICA_SELECTION_SETTING = Setting.boolSetting(
        "cluster.routing.use_adaptive_replica_selection",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private boolean useAdaptiveReplicaSelection;
    private final boolean isStateless;

    public static final Setting<TimeValue> SHARD_SLOW_START_DURATION_SETTING = Setting.positiveTimeSetting(
        "index.routing.slow_start.duration",
        TimeValue.timeValueMillis(0),
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final Setting<Double> SHARD_SLOW_START_AGGRESSION_SETTING = Setting.doubleSetting(
        "index.routing.slow_start.aggression",
        1.0,
        0.1,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    private final AtomicReference<Map<String, ShardSlowStart>> shardSlowStartByAllocationId;

    public OperationRouting(Settings settings, ClusterSettings clusterSettings) {
        this.isStateless = DiscoveryNode.isStateless(settings);
        this.useAdaptiveReplicaSelection = USE_ADAPTIVE_REPLICA_SELECTION_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(USE_ADAPTIVE_REPLICA_SELECTION_SETTING, this::setUseAdaptiveReplicaSelection);
        this.shardSlowStartByAllocationId = new AtomicReference<>();
        this.shardSlowStartByAllocationId.set(new HashMap<>());
    }

    void setUseAdaptiveReplicaSelection(boolean useAdaptiveReplicaSelection) {
        this.useAdaptiveReplicaSelection = useAdaptiveReplicaSelection;
    }

    /**
     * Shards to use for a {@code GET} operation.
     */
    public ShardIterator getShards(
        ClusterState clusterState,
        String index,
        String id,
        @Nullable String routing,
        @Nullable String preference
    ) {
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata(clusterState, index));
        IndexShardRoutingTable shards = clusterState.getRoutingTable().shardRoutingTable(index, indexRouting.getShard(id, routing));
        return preferenceActiveShardIterator(shards, clusterState.nodes().getLocalNodeId(), clusterState.nodes(), preference, null, null);
    }

    public ShardIterator getShards(ClusterState clusterState, String index, int shardId, @Nullable String preference) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(index, shardId);
        return preferenceActiveShardIterator(
            indexShard,
            clusterState.nodes().getLocalNodeId(),
            clusterState.nodes(),
            preference,
            null,
            null
        );
    }

    public ShardIterator useOnlyPromotableShardsForStateless(ShardIterator shards) {
        // If it is stateless, only route promotable shards. This is a temporary workaround until a more cohesive solution can be
        // implemented for search shards.
        if (isStateless && shards != null) {
            return new PlainShardIterator(
                shards.shardId(),
                shards.getShardRoutings().stream().filter(ShardRouting::isPromotableToPrimary).collect(Collectors.toList())
            );
        } else {
            return shards;
        }
    }

    public GroupShardsIterator<ShardIterator> searchShards(
        ClusterState clusterState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference
    ) {
        return searchShards(clusterState, concreteIndices, routing, preference, null, null);
    }

    public GroupShardsIterator<ShardIterator> searchShards(
        ClusterState clusterState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts
    ) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);
        final Set<ShardIterator> set = Sets.newHashSetWithExpectedSize(shards.size());
        for (IndexShardRoutingTable shard : shards) {
            ShardIterator iterator = preferenceActiveShardIterator(
                shard,
                clusterState.nodes().getLocalNodeId(),
                clusterState.nodes(),
                preference,
                collectorService,
                nodeCounts
            );
            if (iterator != null) {
                var searchableShards = iterator.getShardRoutings().stream().filter(ShardRouting::isSearchable).toList();
                set.add(new PlainShardIterator(iterator.shardId(), searchableShards));
            }
        }
        return GroupShardsIterator.sortAndCreate(new ArrayList<>(set));
    }

    public static ShardIterator getShards(ClusterState clusterState, ShardId shardId) {
        final IndexShardRoutingTable shard = clusterState.routingTable().shardRoutingTable(shardId);
        return shard.activeInitializingShardsRandomIt();
    }

    private static final Map<String, Set<String>> EMPTY_ROUTING = Collections.emptyMap();

    private static Set<IndexShardRoutingTable> computeTargetedShards(
        ClusterState clusterState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing
    ) {
        routing = routing == null ? EMPTY_ROUTING : routing; // just use an empty map
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        // we use set here and not list since we might get duplicates
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRoutingTable = indexRoutingTable(clusterState, index);
            final IndexMetadata indexMetadata = indexMetadata(clusterState, index);
            final Set<String> indexSearchRouting = routing.get(index);
            if (indexSearchRouting != null) {
                IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
                for (String r : indexSearchRouting) {
                    indexRouting.collectSearchShards(r, s -> set.add(RoutingTable.shardRoutingTable(indexRoutingTable, s)));
                }
            } else {
                for (int i = 0; i < indexRoutingTable.size(); i++) {
                    set.add(indexRoutingTable.shard(i));
                }
            }
        }
        return set;
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
        return indexShard.activeInitializingShardsIt(routingHash, shardSlowStartByAllocationId.get());
    }

    private ShardIterator shardRoutings(
        IndexShardRoutingTable indexShard,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts
    ) {
        if (useAdaptiveReplicaSelection) {
            return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts, shardSlowStartByAllocationId.get());
        } else {
            return indexShard.activeInitializingShardsRandomIt(shardSlowStartByAllocationId.get());
        }
    }

    protected static IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }

    private static IndexMetadata indexMetadata(ClusterState clusterState, String index) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetadata;
    }

    public ShardId shardId(ClusterState clusterState, String index, String id, @Nullable String routing) {
        IndexMetadata indexMetadata = indexMetadata(clusterState, index);
        return new ShardId(indexMetadata.getIndex(), IndexRouting.fromIndexMetadata(indexMetadata).getShard(id, routing));
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (event.routingTableChanged()) {
            Map<String, ShardSlowStart> previous = shardSlowStartByAllocationId.get();
            Map<String, ShardSlowStart> current = new HashMap<>();
            long currentNanoTime = System.nanoTime();
            for (Map.Entry<String, ShardSlowStart> entry : previous.entrySet()) {
                if (entry.getValue().finished(currentNanoTime) == false) {
                    current.put(entry.getKey(), entry.getValue());
                }
            }
            for (IndexRoutingTable indexRoutingTable : event.state().getRoutingTable().indicesRouting().values()) {
                for (int i = 0; i < indexRoutingTable.size(); i++) {
                    IndexShardRoutingTable indexShard = indexRoutingTable.shard(i);
                    for (int j=0; j < indexShard.size(); j++) {
                        ShardRouting shard = indexShard.shard(j);
                        if (shard.started()) {
                            ShardRouting prev = event.previousState().getRoutingTable()
                                .getByAllocationId(shard.shardId(), shard.allocationId().getId());
                            if (prev == null || prev.initializing()) {
                                IndexMetadata metadata = indexMetadata(event.state(), shard.index().getName());
                                if (SHARD_SLOW_START_DURATION_SETTING.get(metadata.getSettings()).nanos() > 0) {
                                    current.put(
                                        shard.allocationId().getId(),
                                        new ShardSlowStart(
                                            System.nanoTime(),
                                            SHARD_SLOW_START_DURATION_SETTING.get(metadata.getSettings()).nanos(),
                                            SHARD_SLOW_START_AGGRESSION_SETTING.get(metadata.getSettings())
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
            }
            shardSlowStartByAllocationId.set(current);
        }
    }
}
