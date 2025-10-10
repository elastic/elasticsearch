/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Generates the snapshots-by-state and shards-by-state metrics when polled. Only produces
 * metrics on the master node, and only after it's seen a cluster state applied.
 */
public class SnapshotMetricsService implements ClusterStateListener {

    private final ClusterService clusterService;
    private volatile boolean shouldReturnSnapshotMetrics;
    private CachedSnapshotStateMetrics cachedSnapshotStateMetrics;

    public SnapshotMetricsService(SnapshotMetrics snapshotMetrics, ClusterService clusterService) {
        this.clusterService = clusterService;
        snapshotMetrics.createSnapshotShardsByStateMetric(this::getShardsByState);
        snapshotMetrics.createSnapshotsByStateMetric(this::getSnapshotsByState);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState clusterState = event.state();
        // Only return metrics when the state is recovered and we are the master
        shouldReturnSnapshotMetrics = clusterState.nodes().isLocalNodeElectedMaster()
            && clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false;
    }

    private Collection<LongWithAttributes> getShardsByState() {
        if (shouldReturnSnapshotMetrics == false) {
            return List.of();
        }
        return recalculateIfStale(clusterService.state()).shardStateMetrics();
    }

    private Collection<LongWithAttributes> getSnapshotsByState() {
        if (shouldReturnSnapshotMetrics == false) {
            return List.of();
        }
        return recalculateIfStale(clusterService.state()).snapshotStateMetrics();
    }

    private CachedSnapshotStateMetrics recalculateIfStale(ClusterState currentState) {
        if (cachedSnapshotStateMetrics == null || cachedSnapshotStateMetrics.isStale(currentState)) {
            cachedSnapshotStateMetrics = recalculateSnapshotStats(currentState);
        }
        return cachedSnapshotStateMetrics;
    }

    private CachedSnapshotStateMetrics recalculateSnapshotStats(ClusterState currentState) {
        final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(currentState);
        final List<LongWithAttributes> snapshotStateMetrics = new ArrayList<>();
        final List<LongWithAttributes> shardStateMetrics = new ArrayList<>();

        currentState.metadata().projects().forEach((projectId, project) -> {
            final RepositoriesMetadata repositoriesMetadata = RepositoriesMetadata.get(project);
            if (repositoriesMetadata != null) {
                for (RepositoryMetadata repository : repositoriesMetadata.repositories()) {
                    final Tuple<Map<SnapshotsInProgress.State, Integer>, Map<SnapshotsInProgress.ShardState, Integer>> stateSummaries =
                        snapshotsInProgress.shardStateSummaryForRepository(projectId, repository.name());
                    final Map<String, Object> attributesMap = SnapshotMetrics.createAttributesMap(projectId, repository);
                    stateSummaries.v1()
                        .forEach(
                            (snapshotState, count) -> snapshotStateMetrics.add(
                                new LongWithAttributes(count, Maps.copyMapWithAddedEntry(attributesMap, "state", snapshotState.name()))
                            )
                        );
                    stateSummaries.v2()
                        .forEach(
                            (shardState, count) -> shardStateMetrics.add(
                                new LongWithAttributes(count, Maps.copyMapWithAddedEntry(attributesMap, "state", shardState.name()))
                            )
                        );
                }
            }
        });
        return new CachedSnapshotStateMetrics(currentState, snapshotStateMetrics, shardStateMetrics);
    }

    /**
     * A cached copy of the snapshot and shard state metrics
     */
    private record CachedSnapshotStateMetrics(
        String clusterStateId,
        int snapshotsInProgressIdentityHashcode,
        Collection<LongWithAttributes> snapshotStateMetrics,
        Collection<LongWithAttributes> shardStateMetrics
    ) {
        CachedSnapshotStateMetrics(
            ClusterState sourceState,
            Collection<LongWithAttributes> snapshotStateMetrics,
            Collection<LongWithAttributes> shardStateMetrics
        ) {
            this(
                sourceState.stateUUID(),
                System.identityHashCode(SnapshotsInProgress.get(sourceState)),
                snapshotStateMetrics,
                shardStateMetrics
            );
        }

        /**
         * Are these metrics stale?
         *
         * @param currentClusterState The current cluster state
         * @return true if these metrics were calculated from a prior cluster state and need to be recalculated, false otherwise
         */
        public boolean isStale(ClusterState currentClusterState) {
            return (Objects.equals(clusterStateId, currentClusterState.stateUUID()) == false
                && System.identityHashCode(SnapshotsInProgress.get(currentClusterState)) != snapshotsInProgressIdentityHashcode);
        }
    }
}
