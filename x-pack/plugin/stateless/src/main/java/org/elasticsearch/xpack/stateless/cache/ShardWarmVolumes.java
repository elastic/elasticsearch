/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchShardWarmVolumesAction;

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

    private static final Logger logger = LogManager.getLogger(ShardWarmVolumes.class);

    @Nullable
    private final Client client;
    @Nullable
    private final ClusterService clusterService;
    @Nullable
    private final LongCounter fetchTotalMetric;
    // Maps from source node ID to the warm-volume snapshot for a specific shutdown generation.
    private final ConcurrentMap<String, Entry> memo = ConcurrentCollections.newConcurrentMap();
    // Tracks source node IDs for which a fetch is in progress, to avoid duplicate fetches.
    private final Set<String> inFlight = ConcurrentCollections.newConcurrentSet();
    private volatile boolean enabled;

    private ShardWarmVolumes() {
        this.client = null;
        this.clusterService = null;
        this.fetchTotalMetric = null;
        this.enabled = false;
    }

    public ShardWarmVolumes(Client client, ClusterService clusterService) {
        this(client, clusterService, MeterRegistry.NOOP);
    }

    public ShardWarmVolumes(Client client, ClusterService clusterService, MeterRegistry meterRegistry) {
        this.client = client;
        this.clusterService = clusterService;
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

    public void maybeFetch(ClusterState state, String sourceNodeId) {
        if (enabled == false || client == null) {
            return;
        }
        var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        if (shutdown == null) {
            return;
        }
        long generation = shutdown.getStartedAtMillis();
        Entry existing = memo.get(sourceNodeId);
        if (existing != null && existing.generationStartedAtMillis() == generation) {
            return;
        }
        // We also check it at a transport call level per each node. This is an optimisation.
        if (state.getMinTransportVersion().supports(TransportFetchShardWarmVolumesAction.FETCH_SHARD_WARM_VOLUMES) == false) {
            return;
        }
        if (inFlight.add(sourceNodeId) == false) {
            return;
        }
        final ActionListener<TransportFetchShardWarmVolumesAction.Response> listener = ActionListener.runAfter(
            ActionListener.wrap(response -> {
                ClusterState latest = clusterService.state();
                if (latest.nodes().nodeExists(sourceNodeId) == false) {
                    return;
                }
                var currentShutdown = latest.metadata().nodeShutdowns().get(sourceNodeId);
                if (currentShutdown == null || currentShutdown.getStartedAtMillis() != response.generationStartedAtMillis()) {
                    return;
                }
                memo.put(sourceNodeId, response.toEntry());
                recordFetchOutcome("success");
            }, e -> {
                logger.debug(() -> "failed to fetch shard warm volumes from [" + sourceNodeId + "]", e);
                recordFetchOutcome("failure");
            }),
            () -> inFlight.remove(sourceNodeId)
        );
        try {
            client.execute(
                TransportFetchShardWarmVolumesAction.TYPE,
                new TransportFetchShardWarmVolumesAction.Request(sourceNodeId),
                listener
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void recordFetchOutcome(String outcome) {
        if (fetchTotalMetric != null) {
            fetchTotalMetric.incrementBy(1, Map.of(FETCH_OUTCOME_ATTRIBUTE_KEY, outcome));
        }
    }

    @Nullable
    public Entry get(ClusterState state, String sourceNodeId) {
        if (enabled == false) {
            return null;
        }
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

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesChanged()) {
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                memo.remove(node.getId());
                inFlight.remove(node.getId());
            }
        }
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
