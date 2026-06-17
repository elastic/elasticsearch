/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Generates the snapshots-by-state and shards-by-state metrics when polled. Only produces
 * metrics on the master node, and only while the {@link ClusterService} is started. Will only
 * re-calculate the metrics if the {@link SnapshotsInProgress} has changed since the last time
 * they were calculated.
 */
public class CachingSnapshotAndShardByStateMetricsService {

    private final ClusterService clusterService;
    private volatile CachedSnapshotStateMetrics cachedSnapshotStateMetrics;

    public CachingSnapshotAndShardByStateMetricsService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public Collection<LongWithAttributes> getShardsByState() {
        if (clusterService.lifecycleState() != Lifecycle.State.STARTED) {
            return List.of();
        }
        final ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            // Only the master should report on shards-by-state
            return List.of();
        }
        return recalculateIfStale(state).shardStateMetrics();
    }

    public Collection<LongWithAttributes> getSnapshotsByState() {
        if (clusterService.lifecycleState() != Lifecycle.State.STARTED) {
            return List.of();
        }
        final ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            // Only the master should report on snapshots-by-state
            return List.of();
        }
        return recalculateIfStale(state).snapshotStateMetrics();
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
         * @return true if these metrics were calculated from a prior {@link SnapshotsInProgress} and need to be recalculated, false
         *         otherwise
         */
        public boolean isStale(ClusterState currentClusterState) {
            return System.identityHashCode(SnapshotsInProgress.get(currentClusterState)) != snapshotsInProgressIdentityHashcode;
        }
    }
}
