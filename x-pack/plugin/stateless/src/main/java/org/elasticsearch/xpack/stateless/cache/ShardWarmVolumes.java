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

    // Maps from source node ID to the warm volumes collected for one shutdown signal timestamp.
    // Values are added only while that source has a shutdown record, and dropped when the node leaves
    // or its shutdown is cancelled / replaced with a new shutdown signal timestamp.
    private final ConcurrentMap<String, CollectedWarmVolumes> memo = ConcurrentCollections.newConcurrentMap();
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
     * Claims the right to request volumes for {@code sourceNodeId} under the current shutdown signal timestamp.
     * Returns false when disabled, the min transport version is too old, collected volumes already exist for this
     * timestamp (including empty), or a fetch is already in flight.
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
        long shutdownSignalTimestamp = shutdown.getStartedAtMillis();
        if (inFlight.putIfAbsent(sourceNodeId, shutdownSignalTimestamp) != null) {
            return false;
        }
        CollectedWarmVolumes existing = memo.get(sourceNodeId);
        if (existing != null && existing.shutdownSignalTimestamp() == shutdownSignalTimestamp) {
            inFlight.remove(sourceNodeId, shutdownSignalTimestamp);
            return false;
        }
        return true;
    }

    /**
     * Usable collected volumes for the timeout formula: matching shutdown signal timestamp and a non-empty map.
     */
    @Nullable
    public CollectedWarmVolumes get(ClusterState state, String sourceNodeId) {
        if (enabled == false) {
            return null;
        }
        CollectedWarmVolumes collected = collectedForShutdownSignal(state, sourceNodeId);
        if (collected == null || collected.volumes().isEmpty()) {
            return null;
        }
        return collected;
    }

    /**
     * Any stored volumes for this source's current shutdown signal timestamp, including empty.
     */
    @Nullable
    CollectedWarmVolumes collectedForShutdownSignal(ClusterState state, String sourceNodeId) {
        CollectedWarmVolumes collected = memo.get(sourceNodeId);
        if (collected == null) {
            return null;
        }
        var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        if (shutdown == null || shutdown.getStartedAtMillis() != collected.shutdownSignalTimestamp()) {
            return null;
        }
        return collected;
    }

    public void completeFetch(ClusterState state, String respondingNodeId, long shutdownSignalTimestamp, Map<ShardId, Long> volumes) {
        putIfCurrentShutdownSignal(state, respondingNodeId, shutdownSignalTimestamp, volumes);
    }

    /**
     * Drops the in-flight claim for {@code sourceNodeId} when it is still the claim taken at {@code shutdownSignalTimestamp}.
     */
    public void releaseClaim(String sourceNodeId, long shutdownSignalTimestamp) {
        inFlight.remove(sourceNodeId, shutdownSignalTimestamp);
    }

    private void putIfCurrentShutdownSignal(ClusterState state, String nodeId, long shutdownSignalTimestamp, Map<ShardId, Long> volumes) {
        if (state.nodes().nodeExists(nodeId) == false) {
            return;
        }
        var shutdown = state.metadata().nodeShutdowns().get(nodeId);
        if (shutdown == null || shutdown.getStartedAtMillis() != shutdownSignalTimestamp) {
            return;
        }
        memo.put(nodeId, new CollectedWarmVolumes(shutdownSignalTimestamp, volumes));
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
                return shutdown == null || shutdown.getStartedAtMillis() != e.getValue().shutdownSignalTimestamp();
            });
        }
    }

    // visible for testing
    void put(String sourceNodeId, CollectedWarmVolumes collected) {
        memo.put(sourceNodeId, collected);
    }

    // visible for testing
    boolean isInFlight(String sourceNodeId) {
        return inFlight.containsKey(sourceNodeId);
    }

    // visible for testing
    CollectedWarmVolumes peek(String sourceNodeId) {
        return memo.get(sourceNodeId);
    }

    /**
     * Warm volumes collected for one source node under one shutdown signal.
     *
     * @param shutdownSignalTimestamp shutdown signal timestamp the volumes were collected under
     * @param volumes                 immutable per-shard warm volumes
     */
    public record CollectedWarmVolumes(long shutdownSignalTimestamp, Map<ShardId, Long> volumes) {
        public CollectedWarmVolumes {
            Objects.requireNonNull(volumes);
            volumes = Map.copyOf(volumes);
        }
    }
}
