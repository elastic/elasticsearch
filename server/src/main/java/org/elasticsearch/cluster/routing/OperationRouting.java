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
import org.elasticsearch.common.util.FeatureFlag;
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

    /**
     * Feature flag for ARS probing of stat-less nodes. Controls the default value of
     * {@link #ADAPTIVE_REPLICA_SELECTION_PROBE_ENABLED_SETTING}: enabled in snapshot/test builds,
     * disabled otherwise. Once the feature is stable the flag will be removed and the setting
     * default will become {@code true} unconditionally.
     */
    static final FeatureFlag ARS_PROBING_FEATURE_FLAG = new FeatureFlag("ars_probing");

    /**
     * Bundles the state needed for adaptive replica selection (ARS) routing decisions.
     *
     * @param collector        collects per-node EWMA stats (queue size, response time, service time)
     * @param searchCounts     mutable per-search snapshot of in-flight counts used by the ARS
     *                         formula and incremented locally for multi-shard spreading within a
     *                         single search; each search receives an independent copy, so
     *                         routing-time increments from concurrent searches are invisible to
     *                         each other
     * @param globalCounts     live in-flight counts shared across all searches; used by the probe
     *                         cap to see real concurrent load across searches
     * @param probeEnabled     whether ARS probing of stat-less and warming-up nodes is active;
     *                         when {@code false} the original ARS behavior is preserved
     * @param probeInflightCap per-coordinator cap on in-flight requests to a stat-less or warming-up
     *                         node before probing is suspended; see
     *                         {@link #ADAPTIVE_REPLICA_SELECTION_PROBE_INFLIGHT_CAP_SETTING}
     * @param warmupSamples    minimum observation count for a peer to be considered warm; below
     *                         this threshold a peer is subject to the in-flight cap and the rank
     *                         clamp; {@code 0} disables both warmup protections
     */
    public record ArsContext(
        ResponseCollectorService collector,
        Map<String, Long> searchCounts,
        Map<String, Long> globalCounts,
        boolean probeEnabled,
        long probeInflightCap,
        int warmupSamples
    ) {}

    public static final Setting<Boolean> USE_ADAPTIVE_REPLICA_SELECTION_SETTING = Setting.boolSetting(
        "cluster.routing.use_adaptive_replica_selection",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Controls ARS probing of stat-less and warming-up nodes. When {@code false} the original ARS
     * behavior is preserved: unranked nodes sort first and no probing or warmup smoothing is applied.
     * <p>
     * Defaults to {@link #ARS_PROBING_FEATURE_FLAG}: enabled in snapshot/test builds, disabled in
     * release builds. Once stable, the flag will be removed and the default becomes {@code true}.
     */
    public static final Setting<Boolean> ADAPTIVE_REPLICA_SELECTION_PROBE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.use_adaptive_replica_selection.probe_enabled",
        ARS_PROBING_FEATURE_FLAG.isEnabled(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Per-coordinator cap on concurrent in-flight requests to a stat-less or warming-up data node
     * before probing is suspended. The effective global cap for probes targeting one node is
     * {@code probe_inflight_cap × number_of_coordinators}. Set to {@code 0} to suppress probing
     * while keeping the feature enabled. Internal tuning surface — not advertised in user docs.
     */
    public static final Setting<Long> ADAPTIVE_REPLICA_SELECTION_PROBE_INFLIGHT_CAP_SETTING = Setting.longSetting(
        "cluster.routing.use_adaptive_replica_selection.probe_inflight_cap",
        2L,
        0L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Observation count below which a peer is considered <em>warming up</em>. Warming-up peers
     * are subject to the same in-flight cap as stat-less probes and a rank clamp that pegs their
     * rank to the best warm peer, preventing sparse EWMA stats from making them look artificially
     * fast. Both protections release once the threshold is reached and the peer ranks on standard
     * C3 terms. Internal tuning surface — not advertised in user docs; {@code 0} disables both
     * protections.
     */
    public static final Setting<Integer> ADAPTIVE_REPLICA_SELECTION_WARMUP_SAMPLES_SETTING = Setting.intSetting(
        "cluster.routing.use_adaptive_replica_selection.warmup_samples",
        100,
        0,
        10000,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean useAdaptiveReplicaSelection;
    private volatile boolean adaptiveReplicaSelectionProbeEnabled;
    private volatile long adaptiveReplicaSelectionProbeInflightCap;
    private volatile int adaptiveReplicaSelectionWarmupSamples;

    @SuppressWarnings("this-escape")
    public OperationRouting(Settings settings, ClusterSettings clusterSettings) {
        this.useAdaptiveReplicaSelection = USE_ADAPTIVE_REPLICA_SELECTION_SETTING.get(settings);
        this.adaptiveReplicaSelectionProbeEnabled = ADAPTIVE_REPLICA_SELECTION_PROBE_ENABLED_SETTING.get(settings);
        this.adaptiveReplicaSelectionProbeInflightCap = ADAPTIVE_REPLICA_SELECTION_PROBE_INFLIGHT_CAP_SETTING.get(settings);
        this.adaptiveReplicaSelectionWarmupSamples = ADAPTIVE_REPLICA_SELECTION_WARMUP_SAMPLES_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(USE_ADAPTIVE_REPLICA_SELECTION_SETTING, this::setUseAdaptiveReplicaSelection);
        clusterSettings.addSettingsUpdateConsumer(
            ADAPTIVE_REPLICA_SELECTION_PROBE_ENABLED_SETTING,
            this::setAdaptiveReplicaSelectionProbeEnabled
        );
        clusterSettings.addSettingsUpdateConsumer(
            ADAPTIVE_REPLICA_SELECTION_PROBE_INFLIGHT_CAP_SETTING,
            this::setAdaptiveReplicaSelectionProbeInflightCap
        );
        clusterSettings.addSettingsUpdateConsumer(
            ADAPTIVE_REPLICA_SELECTION_WARMUP_SAMPLES_SETTING,
            this::setAdaptiveReplicaSelectionWarmupSamples
        );
    }

    void setUseAdaptiveReplicaSelection(boolean useAdaptiveReplicaSelection) {
        this.useAdaptiveReplicaSelection = useAdaptiveReplicaSelection;
    }

    void setAdaptiveReplicaSelectionProbeEnabled(boolean probeEnabled) {
        this.adaptiveReplicaSelectionProbeEnabled = probeEnabled;
    }

    void setAdaptiveReplicaSelectionProbeInflightCap(long probeInflightCap) {
        this.adaptiveReplicaSelectionProbeInflightCap = probeInflightCap;
    }

    void setAdaptiveReplicaSelectionWarmupSamples(int adaptiveReplicaSelectionWarmupSamples) {
        this.adaptiveReplicaSelectionWarmupSamples = adaptiveReplicaSelectionWarmupSamples;
    }

    public boolean isAdaptiveReplicaSelectionProbeEnabled() {
        return adaptiveReplicaSelectionProbeEnabled;
    }

    public long getAdaptiveReplicaSelectionProbeInflightCap() {
        return adaptiveReplicaSelectionProbeInflightCap;
    }

    public int getAdaptiveReplicaSelectionWarmupSamples() {
        return adaptiveReplicaSelectionWarmupSamples;
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
        return preferenceActiveShardIterator(shards, nodes.getLocalNodeId(), nodes, preference, null);
    }

    public ShardIterator getShards(ProjectState projectState, String index, int shardId, @Nullable String preference) {
        IndexShardRoutingTable indexShard = projectState.routingTable().shardRoutingTable(index, shardId);
        DiscoveryNodes nodes = projectState.cluster().nodes();
        return preferenceActiveShardIterator(indexShard, nodes.getLocalNodeId(), nodes, preference, null);
    }

    public List<SearchShardRouting> searchShards(
        ProjectState projectState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference
    ) {
        return searchShards(projectState, concreteIndices, routing, preference, null, true);
    }

    public List<SearchShardRouting> searchShards(
        ProjectState projectState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference,
        @Nullable ArsContext arsContext,
        boolean shouldSort
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
                arsContext
            );
            if (iterator != null) {
                res.add(
                    SearchShardRouting.fromShardIterator(ShardIterator.allSearchableShards(iterator), targetShard.splitShardCountSummary())
                );
            }
        }

        if (shouldSort) {
            res.sort(SearchShardRouting::compareTo);
        }

        return res;
    }

    public Iterator<IndexShardRoutingTable> allWritableShards(ProjectState projectState, String index) {
        return allWriteAddressableShards(projectState, index);
    }

    public static ShardIterator getShards(RoutingTable routingTable, ShardId shardId) {
        final IndexShardRoutingTable shard = routingTable.shardRoutingTable(shardId);
        return shard.activeInitializingShardsRandomIt();
    }

    private record SearchTargetShard(IndexShardRoutingTable shardRoutingTable, SplitShardCountSummary splitShardCountSummary) {}

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
        @Nullable ArsContext arsContext
    ) {
        if (preference == null || preference.isEmpty()) {
            return shardRoutings(indexShard, arsContext);
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
                    return shardRoutings(indexShard, arsContext);
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

    private ShardIterator shardRoutings(IndexShardRoutingTable indexShard, @Nullable ArsContext arsContext) {
        if (useAdaptiveReplicaSelection) {
            return indexShard.activeInitializingShardsRankedIt(arsContext);
        }
        return indexShard.activeInitializingShardsRandomIt();
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
