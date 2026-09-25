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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        return maybeGetCachedSnapshotStateMetrics().map(CachedSnapshotStateMetrics::shardStateMetrics).orElse(List.of());
    }

    public Collection<LongWithAttributes> getSnapshotsByState() {
        return maybeGetCachedSnapshotStateMetrics().map(CachedSnapshotStateMetrics::snapshotStateMetrics).orElse(List.of());
    }

    /// If this node is reporting metrics (i.e. it is master and the cluster service is started), returns a single long value giving the
    /// longest time that any shard snapshot has been in the [org.elasticsearch.cluster.SnapshotsInProgress.ShardState#WAITING] state (in
    /// milliseconds, with some caveats). That value will be zero if no shard snapshots are waiting. If this node is not reporting metrics,
    /// returns an empty collection.
    ///
    /// Caveats:
    /// - The precision of this value is no greater than the interval between calls to this method (because the state is only inspected when
    /// this method is called).
    /// - This value is reset if the master changes or restarts (because the timestamps are held in-memory on the master).
    public Collection<LongWithAttributes> getLongestWaitingTimeMillis() {
        return maybeGetCachedSnapshotStateMetrics().map(metrics -> metrics.longestWaitingTimeMillisMetrics(currentTimeMillis()))
            .orElse(List.of());
    }

    private Optional<CachedSnapshotStateMetrics> maybeGetCachedSnapshotStateMetrics() {
        if (clusterService.lifecycleState() != Lifecycle.State.STARTED) {
            return Optional.empty();
        }
        final ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            // Only the master should report on metrics
            resetCachedState();
            return Optional.empty();
        }
        return Optional.of(recalculateIfStale(state));
    }

    private void resetCachedState() {
        synchronized (waitingTimestamps) {
            waitingTimestamps.clear();
        }
    }

    private CachedSnapshotStateMetrics recalculateIfStale(ClusterState currentState) {
        if (cachedSnapshotStateMetrics == null || cachedSnapshotStateMetrics.isStale(currentState)) {
            cachedSnapshotStateMetrics = recalculateSnapshotStats(currentState);
        }
        return cachedSnapshotStateMetrics;
    }

    private final Map<Tuple<Snapshot, ShardId>, Long> waitingTimestamps = new HashMap<>();

    private CachedSnapshotStateMetrics recalculateSnapshotStats(ClusterState currentState) {
        final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(currentState);
        final List<LongWithAttributes> snapshotStateMetrics = new ArrayList<>();
        final List<LongWithAttributes> shardStateMetrics = new ArrayList<>();
        Set<Tuple<Snapshot, ShardId>> waitingShards = Sets.newHashSet();

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
                    waitingShards.addAll(snapshotsInProgress.waitingShards(projectId, repository.name()));
                }
            }
        });
        return new CachedSnapshotStateMetrics(
            currentState,
            snapshotStateMetrics,
            shardStateMetrics,
            computeEarliestWaitingShardTimestampMillis(waitingShards, currentTimeMillis())
        );
    }

    private long computeEarliestWaitingShardTimestampMillis(Set<Tuple<Snapshot, ShardId>> waitingShards, long nowMillis) {
        synchronized (waitingTimestamps) {
            if (waitingShards.isEmpty()) {
                waitingTimestamps.clear();
                return Long.MAX_VALUE; // ensures that CachedSnapshotStateMetrics.longestWaitingTimeMillisMetrics() will compute zero value
            } else {
                waitingShards.forEach(shardSnapshot -> waitingTimestamps.putIfAbsent(shardSnapshot, nowMillis));
                waitingTimestamps.keySet().retainAll(waitingShards);
                return Collections.min(waitingTimestamps.values());
            }
        }
    }

    private long currentTimeMillis() {
        // We use absoluteTimeInMillis rather than relativeTimeInMillis because the latter has an arbitrary zero point, so there's a chance
        // (though very small!) that it could wrap around while we're running, and then the minimum timestamp wouldn't be the earliest.
        // We deal with the (also very small) chance that we could observe time going backwards in
        // CachedSnapshotStateMetrics.longestWaitingTimeMillisMetrics().
        long currentTimeMillis = clusterService.threadPool().absoluteTimeInMillis();
        assert currentTimeMillis >= 0 : "Current time is before epoch start: " + currentTimeMillis;
        return currentTimeMillis;
    }

    /**
     * A cached copy of the snapshot and shard state metrics
     */
    private record CachedSnapshotStateMetrics(
        String clusterStateId,
        int snapshotsInProgressIdentityHashcode,
        Collection<LongWithAttributes> snapshotStateMetrics,
        Collection<LongWithAttributes> shardStateMetrics,
        long earliestWaitingShardTimestampMillis
    ) {
        CachedSnapshotStateMetrics(
            ClusterState sourceState,
            Collection<LongWithAttributes> snapshotStateMetrics,
            Collection<LongWithAttributes> shardStateMetrics,
            long earliestWaitingShardTimestampMillis
        ) {
            this(
                sourceState.stateUUID(),
                System.identityHashCode(SnapshotsInProgress.get(sourceState)),
                snapshotStateMetrics,
                shardStateMetrics,
                earliestWaitingShardTimestampMillis
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

        public List<LongWithAttributes> longestWaitingTimeMillisMetrics(long nowMillis) {
            long longestWaitingTimeMillis = Math.max(nowMillis - earliestWaitingShardTimestampMillis, 0L);
            return List.of(new LongWithAttributes(longestWaitingTimeMillis));
        }
    }
}
