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
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.metric.LongAsyncMeasurement;

import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Computes the {@link RestoreMetrics} gauges from the current cluster state, in the same way that
 * {@link CachingSnapshotAndShardByStateMetricsService} computes the snapshot gauges. Only the elected master reports anything, so that the
 * cluster reports each restore once.
 * <p>
 * The values are calculated when the gauges are polled, and cached until {@link RestoreInProgress} or the routing table changes. Because
 * they only read the cluster state, they hold nothing that a change of master would lose.
 */
public class CachingRestoreMetricsService {

    private final ClusterService clusterService;
    private final LongSupplier absoluteTimeMillisSupplier;
    private volatile CachedRestoreMetrics cachedRestoreMetrics;

    /**
     * @param absoluteTimeMillisSupplier the current wall-clock time, which the age of a restoring shard is measured against because the
     *                                   time it became unassigned is recorded in the cluster state as wall-clock time
     */
    public CachingRestoreMetricsService(ClusterService clusterService, LongSupplier absoluteTimeMillisSupplier) {
        this.clusterService = clusterService;
        this.absoluteTimeMillisSupplier = absoluteTimeMillisSupplier;
    }

    /** Records the number of restores in progress, by whether they report their restoring shards. */
    public void recordRestoresCurrent(LongAsyncMeasurement measurement) {
        maybeGetCachedRestoreMetrics().ifPresent(metrics -> {
            measurement.record(metrics.restoresReportingShardRestoring(), Map.of(RestoreMetrics.REPORT_SHARD_RESTORING_ATTRIBUTE, true));
            measurement.record(
                metrics.restoresNotReportingShardRestoring(),
                Map.of(RestoreMetrics.REPORT_SHARD_RESTORING_ATTRIBUTE, false)
            );
        });
    }

    /** Records the number of indices that the restores in progress are restoring. */
    public void recordIndicesCurrent(LongAsyncMeasurement measurement) {
        maybeGetCachedRestoreMetrics().ifPresent(metrics -> measurement.record(metrics.indices()));
    }

    /** Records the number of shards of the restores in progress in each {@link RestoreMetrics.ShardState}. */
    public void recordShardsByState(LongAsyncMeasurement measurement) {
        maybeGetCachedRestoreMetrics().ifPresent(metrics -> {
            for (RestoreMetrics.ShardState state : RestoreMetrics.ShardState.values()) {
                measurement.record(metrics.shardsByState()[state.ordinal()], Map.of(RestoreMetrics.SHARD_STATE_ATTRIBUTE, state.value()));
            }
        });
    }

    /** Records the number of shards still restoring that have failed to recover at least once and are being retried. */
    public void recordShardsRetrying(LongAsyncMeasurement measurement) {
        maybeGetCachedRestoreMetrics().ifPresent(metrics -> measurement.record(metrics.shardsRetrying()));
    }

    /**
     * Records the longest time that any shard still restoring has been waiting for or recovering in its current attempt, or zero if no
     * shard is restoring. The age is measured from the time in the shard's {@link UnassignedInfo}, so it survives a change of master, and
     * it grows with the clock even while the cached values are reused.
     */
    public void recordOldestShardAge(LongAsyncMeasurement measurement) {
        maybeGetCachedRestoreMetrics().ifPresent(metrics -> {
            final long oldest = metrics.oldestShardUnassignedTimeMillis();
            // the times are wall-clock times, possibly set by a different master, so never report a negative age
            measurement.record(oldest == Long.MAX_VALUE ? 0 : Math.max(0, absoluteTimeMillisSupplier.getAsLong() - oldest));
        });
    }

    private Optional<CachedRestoreMetrics> maybeGetCachedRestoreMetrics() {
        if (clusterService.lifecycleState() != Lifecycle.State.STARTED) {
            return Optional.empty();
        }
        final ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            // Only the master should report on metrics
            return Optional.empty();
        }
        return Optional.of(recalculateIfStale(state));
    }

    private CachedRestoreMetrics recalculateIfStale(ClusterState currentState) {
        CachedRestoreMetrics metrics = cachedRestoreMetrics;
        if (metrics == null || metrics.isStale(currentState)) {
            metrics = recalculateRestoreMetrics(currentState);
            cachedRestoreMetrics = metrics;
        }
        return metrics;
    }

    private static CachedRestoreMetrics recalculateRestoreMetrics(ClusterState state) {
        final RestoreInProgress restoreInProgress = RestoreInProgress.get(state);
        long reporting = 0;
        long notReporting = 0;
        long indices = 0;
        long retrying = 0;
        long oldestUnassignedTimeMillis = Long.MAX_VALUE;
        final long[] shardsByState = new long[RestoreMetrics.ShardState.values().length];
        for (RestoreInProgress.Entry entry : restoreInProgress) {
            if (entry.state().completed()) {
                // completed restores are only waiting to be removed from the cluster state
                continue;
            }
            if (entry.reportShardRestoring()) {
                reporting++;
            } else {
                notReporting++;
            }
            indices += entry.indices().size();
            for (Map.Entry<ShardId, ShardRestoreStatus> shard : entry.shards().entrySet()) {
                final RestoreMetrics.ShardState shardState = switch (shard.getValue().state()) {
                    case SUCCESS -> RestoreMetrics.ShardState.SUCCEEDED;
                    case FAILURE -> RestoreMetrics.ShardState.FAILED;
                    case INIT, STARTED -> {
                        final ShardRouting primary = primary(state, shard.getKey());
                        // a restoring primary keeps the unassigned info of its current attempt until it starts
                        final UnassignedInfo unassignedInfo = primary == null ? null : primary.unassignedInfo();
                        if (unassignedInfo != null) {
                            if (unassignedInfo.failedAllocations() > 0) {
                                retrying++;
                            }
                            oldestUnassignedTimeMillis = Math.min(oldestUnassignedTimeMillis, unassignedInfo.unassignedTimeMillis());
                        }
                        yield primary != null && primary.assignedToNode()
                            ? RestoreMetrics.ShardState.RECOVERING
                            : RestoreMetrics.ShardState.QUEUED;
                    }
                };
                shardsByState[shardState.ordinal()]++;
            }
        }
        return new CachedRestoreMetrics(
            restoreInProgress,
            state.globalRoutingTable(),
            reporting,
            notReporting,
            indices,
            shardsByState,
            retrying,
            oldestUnassignedTimeMillis
        );
    }

    @Nullable
    private static ShardRouting primary(ClusterState state, ShardId shardId) {
        final IndexRoutingTable indexRoutingTable = state.globalRoutingTable()
            .indexRouting(state.metadata(), shardId.getIndex())
            .orElse(null);
        if (indexRoutingTable == null || shardId.id() >= indexRoutingTable.size()) {
            return null;
        }
        return indexRoutingTable.shard(shardId.id()).primaryShard();
    }

    /**
     * A cached copy of the restore gauge values, and the cluster state parts they were calculated from.
     *
     * @param oldestShardUnassignedTimeMillis the earliest wall-clock time at which a shard still restoring became unassigned for its current
     *                                        attempt, or {@link Long#MAX_VALUE} if no shard is restoring
     */
    private record CachedRestoreMetrics(
        RestoreInProgress restoreInProgress,
        GlobalRoutingTable routingTable,
        long restoresReportingShardRestoring,
        long restoresNotReportingShardRestoring,
        long indices,
        long[] shardsByState,
        long shardsRetrying,
        long oldestShardUnassignedTimeMillis
    ) {
        /**
         * @return true if these metrics were calculated from a prior {@link RestoreInProgress} or routing table, both of which are
         *         immutable, and need to be recalculated
         */
        boolean isStale(ClusterState currentState) {
            return RestoreInProgress.get(currentState) != restoreInProgress || currentState.globalRoutingTable() != routingTable;
        }
    }
}
