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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * Target-side memoization of per-shard warm volumes fetched from a draining search node.
 * Fetches are fire-and-forget: recovery never waits on the RPC.
 */
public class ShardWarmVolumes implements ClusterStateListener {

    public static final ShardWarmVolumes NOOP = new ShardWarmVolumes();

    // Maps from source node ID to the warm-volume snapshot for a specific shutdown generation.
    // Entries are added only while that source has a shutdown record, and dropped when the node leaves
    // or its shutdown is cancelled / replaced with a new generation.
    private final ConcurrentMap<String, Entry> memo = ConcurrentCollections.newConcurrentMap();
    // Source node ID to the shutdown start time of the fetch that holds the claim.
    private final ConcurrentMap<String, Long> inFlight = ConcurrentCollections.newConcurrentMap();
    private volatile boolean enabled;

    private ShardWarmVolumes() {
        this.enabled = false;
    }

    public ShardWarmVolumes(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            SearchRecoveryTimeoutCalculationService.SEARCH_OFFLINE_WARMING_WARM_VOLUMES_ENABLED_SETTING,
            v -> this.enabled = v
        );
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
        if (inFlight.putIfAbsent(sourceNodeId, generation) != null) {
            return false;
        }
        Entry existing = memo.get(sourceNodeId);
        if (existing != null && existing.generationStartedAtMillis() == generation) {
            inFlight.remove(sourceNodeId, generation);
            return false;
        }
        return true;
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
    Entry entryForGeneration(ClusterState state, String sourceNodeId) {
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
        String respondingNodeId,
        long volumesGeneration,
        Map<ShardId, Long> volumes
    ) {
        putIfCurrentGeneration(state, respondingNodeId, volumesGeneration, volumes);
    }

    /**
     * Drops the in-flight claim for {@code sourceNodeId} when it is still the claim taken at {@code startedAtMillis}.
     */
    public void releaseClaim(String sourceNodeId, long startedAtMillis) {
        inFlight.remove(sourceNodeId, startedAtMillis);
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

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        boolean shutdownsChanged = event.changedCustomClusterMetadataSet().contains(NodesShutdownMetadata.TYPE);
        if (memo.isEmpty() && inFlight.isEmpty() && shutdownsChanged == false) {
            return;
        }
        var nodes = event.state().nodes();
        var shutdowns = event.state().metadata().nodeShutdowns();
        memo.entrySet().removeIf(e -> nodes.nodeExists(e.getKey()) == false);
        inFlight.entrySet().removeIf(e -> {
            if (nodes.nodeExists(e.getKey()) == false) {
                return true;
            }
            if (shutdownsChanged == false) {
                return false;
            }
            var shutdown = shutdowns.get(e.getKey());
            return shutdown == null || shutdown.getStartedAtMillis() != e.getValue();
        });
        if (shutdownsChanged) {
            memo.entrySet().removeIf(e -> {
                var shutdown = shutdowns.get(e.getKey());
                return shutdown == null || shutdown.getStartedAtMillis() != e.getValue().generationStartedAtMillis();
            });
        }
    }

    // visible for testing
    void put(String sourceNodeId, Entry entry) {
        memo.put(sourceNodeId, entry);
    }

    // visible for testing
    boolean isInFlight(String sourceNodeId) {
        return inFlight.containsKey(sourceNodeId);
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
