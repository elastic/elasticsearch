/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Target-side memo of per-shard warm volumes fetched from a draining search node.
 * Fetches are fire-and-forget: recovery never waits on the RPC.
 */
public class ShardWarmVolumes implements ClusterStateListener {

    public static final ShardWarmVolumes NOOP = new ShardWarmVolumes();

    public static final String FETCH_TOTAL_METRIC = "es.blob_cache_warming.shard_warm_volumes.fetch.total";
    public static final String FETCH_OUTCOME_ATTRIBUTE_KEY = "es_fetch_outcome";

    @Nullable
    private final LongCounter fetchTotalMetric;
    // Maps from source node ID to the warm-volume snapshot for a specific shutdown generation.
    // Entries are added only while that source has a shutdown record, and dropped when the node leaves
    // or its shutdown is cancelled / replaced with a new generation.
    private final ConcurrentMap<String, Entry> memo = ConcurrentCollections.newConcurrentMap();
    // Tracks source node IDs for which a fetch is in progress, to avoid duplicate fetches.
    private final Set<String> inFlight = ConcurrentCollections.newConcurrentSet();
    private volatile boolean enabled;

    private ShardWarmVolumes() {
        this.fetchTotalMetric = null;
        this.enabled = false;
    }

    public ShardWarmVolumes(ClusterService clusterService) {
        this(clusterService, MeterRegistry.NOOP);
    }

    public ShardWarmVolumes(ClusterService clusterService, MeterRegistry meterRegistry) {
        this.fetchTotalMetric = meterRegistry.registerLongCounter(
            FETCH_TOTAL_METRIC,
            "Fetches of per-shard warm volumes from a draining search node, broken down by [" + FETCH_OUTCOME_ATTRIBUTE_KEY + "]",
            "count"
        );
        clusterService.getClusterSettings()
            .initializeAndWatch(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_WARM_VOLUMES_ENABLED_SETTING, v -> this.enabled = v);
    }

    /**
     * True when {@code routing} is a relocation whose source is marked for removal.
     */
    public static boolean shouldFetch(ShardRouting routing, ClusterState state) {
        String sourceId = routing.relocatingNodeId();
        return sourceId != null && state.metadata().nodeShutdowns().isNodeMarkedForRemoval(sourceId);
    }

    /**
     * Claims the right to request volumes for {@code sourceNodeId} under the current shutdown generation.
     * Returns false when disabled, the min transport version is too old, an entry already exists for this
     * generation (including empty), or a fetch is already in flight.
     */
    public boolean claimFetch(ClusterState state, String sourceNodeId) {
        if (enabled == false || sourceNodeId == null) {
            return false;
        }
        if (state.getMinTransportVersion().supports(TransportFetchSearchShardInformationAction.FETCH_SHARD_WARM_VOLUMES) == false) {
            return false;
        }
        var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        if (shutdown == null) {
            return false;
        }
        long generation = shutdown.getStartedAtMillis();
        Entry existing = memo.get(sourceNodeId);
        if (existing != null && existing.generationStartedAtMillis() == generation) {
            return false;
        }
        return inFlight.add(sourceNodeId);
    }

    /**
     * Usable entry for the timeout formula: matching generation and a non-empty map.
     */
    @Nullable
    public Entry get(ClusterState state, String sourceNodeId) {
        if (enabled == false) {
            return null;
        }
        Entry entry = entryForGeneration(state, sourceNodeId);
        if (entry == null || entry.volumes().isEmpty()) {
            return null;
        }
        return entry;
    }

    /**
     * Any stored entry for this source's current shutdown generation, including empty.
     */
    @Nullable
    public Entry entryForGeneration(ClusterState state, String sourceNodeId) {
        Entry entry = memo.get(sourceNodeId);
        if (entry == null) {
            return null;
        }
        var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        if (shutdown == null || shutdown.getStartedAtMillis() != entry.generationStartedAtMillis()) {
            return null;
        }
        return entry;
    }

    public void completeFetch(
        ClusterState state,
        String claimedId,
        String respondingNodeId,
        long volumesGeneration,
        Map<ShardId, Long> volumes
    ) {
        putIfCurrentGeneration(state, respondingNodeId, volumesGeneration, volumes);
        inFlight.remove(claimedId);
        recordFetchOutcome("success");
    }

    public void releaseClaim(String sourceNodeId) {
        inFlight.remove(sourceNodeId);
    }

    public void recordFetchFailure() {
        recordFetchOutcome("failure");
    }

    private void putIfCurrentGeneration(ClusterState state, String nodeId, long generation, Map<ShardId, Long> volumes) {
        if (state.nodes().nodeExists(nodeId) == false) {
            return;
        }
        var shutdown = state.metadata().nodeShutdowns().get(nodeId);
        if (shutdown == null || shutdown.getStartedAtMillis() != generation) {
            return;
        }
        memo.put(nodeId, new Entry(generation, volumes));
    }

    private void recordFetchOutcome(String outcome) {
        if (fetchTotalMetric != null) {
            fetchTotalMetric.incrementBy(1, Map.of(FETCH_OUTCOME_ATTRIBUTE_KEY, outcome));
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesChanged() == false && event.changedCustomClusterMetadataSet().contains(NodesShutdownMetadata.TYPE) == false) {
            return;
        }
        if (event.nodesChanged()) {
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                memo.remove(node.getId());
                inFlight.remove(node.getId());
            }
        }
        var shutdowns = event.state().metadata().nodeShutdowns();
        memo.entrySet().removeIf(e -> {
            var shutdown = shutdowns.get(e.getKey());
            return shutdown == null || shutdown.getStartedAtMillis() != e.getValue().generationStartedAtMillis();
        });
        inFlight.removeIf(id -> shutdowns.get(id) == null);
    }

    // visible for testing
    void put(String sourceNodeId, Entry entry) {
        memo.put(sourceNodeId, entry);
    }

    // visible for testing
    boolean isInFlight(String sourceNodeId) {
        return inFlight.contains(sourceNodeId);
    }

    // visible for testing
    Entry peek(String sourceNodeId) {
        return memo.get(sourceNodeId);
    }

    /**
     * Completed warm-volume snapshot for one source node.
     *
     * @param generationStartedAtMillis shutdown generation the volumes were collected under
     * @param volumes                   immutable per-shard warm volumes
     */
    public record Entry(long generationStartedAtMillis, Map<ShardId, Long> volumes) {
        public Entry {
            Objects.requireNonNull(volumes);
            volumes = Map.copyOf(volumes);
        }
    }
}
