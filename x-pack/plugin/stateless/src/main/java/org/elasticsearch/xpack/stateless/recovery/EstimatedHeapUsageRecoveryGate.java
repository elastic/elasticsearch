/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.RecoveryGate;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.EstimatedHeapSettings;
import org.elasticsearch.xpack.stateless.memory.ShardsMappingSizeCollector;
import org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsService;

import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/// A node-wide [RecoveryGate] for stateless index nodes: defers starting new recoveries while this node's estimated heap usage
/// is above the high watermark — the threshold above which [EstimatedHeapUsageAllocationDecider]'s `canRemain` moves started
/// shards away. The master's view ([org.elasticsearch.cluster.ClusterInfo]) refreshes only every few tens of seconds while data
/// nodes start recoveries at their own pace — this gate is the fresher, local safety valve.
///
/// The estimate covers only the shards already residing on this node, computed from the exact values the node publishes
/// to the master ([ShardsMappingSizeCollector#collectShardMappingSizes]) — the same values that, once published, feed the estimates
/// [EstimatedHeapUsageAllocationDecider] uses — through the master's own summation
/// ([StatelessMemoryMetricsService#estimateNodeHeapUsage]).
public class EstimatedHeapUsageRecoveryGate implements RecoveryGate {

    private static final Logger logger = LogManager.getLogger(EstimatedHeapUsageRecoveryGate.class);
    static final String NAME = "estimated_heap";

    /// How long a computed estimate is cached. The heap estimate needs to loop through every shard on the node, so the result is
    /// cached.
    private static final TimeValue ESTIMATE_VALIDITY = TimeValue.timeValueSeconds(1);

    private final Supplier<ClusterState> clusterStateSupplier;
    private final ToLongFunction<ClusterState> estimatedHeapUsageBytes;
    private final long maxHeapBytes;
    private final EstimatedHeapSettings heapSettings;
    private final LongSupplier relativeTimeInNanos;
    private final long estimateValidityNanos;
    private volatile EstimateSnapshot lastEstimate;

    private record EstimateSnapshot(long estimatedBytes, long computedAtNanos) {}

    /// Builds a gate wired to the node's real services and JVM max heap: the estimate is computed from the exact shard values the
    /// collector publishes to the master, fed through the master's own summation.
    public static EstimatedHeapUsageRecoveryGate create(
        ClusterService clusterService,
        StatelessMemoryMetricsService memoryMetricsService,
        ShardsMappingSizeCollector shardsMappingSizeCollector,
        ThreadPool threadPool,
        EstimatedHeapSettings heapSettings
    ) {
        return new EstimatedHeapUsageRecoveryGate(
            heapSettings,
            clusterService::state,
            JvmInfo.jvmInfo().getMem().getHeapMax().getBytes(),
            state -> memoryMetricsService.estimateNodeHeapUsage(
                state.metadata().getTotalNumberOfIndices(),
                // large-indexing-ops heap: this is for serverless autoscaling signal, not real resident shard heap usage.
                // data node does not consider it for gating recoveries.
                0L,
                // TODO: add merge memory estimate, only exists in serverless, may need a new SPI
                0L,
                // collect shard heap usage estimate from local node
                shardsMappingSizeCollector.collectShardMappingSizes()
            ).totalHeapUsage(),
            threadPool::relativeTimeInNanos,
            ESTIMATE_VALIDITY
        );
    }

    // Visible for testing
    EstimatedHeapUsageRecoveryGate(
        EstimatedHeapSettings heapSettings,
        Supplier<ClusterState> clusterStateSupplier,
        long maxHeapBytes,
        ToLongFunction<ClusterState> estimatedHeapUsageBytes,
        LongSupplier relativeTimeInNanos,
        TimeValue estimateValidity
    ) {
        assert maxHeapBytes >= 0 : "negative max heap size: " + maxHeapBytes;
        this.clusterStateSupplier = clusterStateSupplier;
        this.maxHeapBytes = maxHeapBytes;
        this.estimatedHeapUsageBytes = estimatedHeapUsageBytes;
        this.heapSettings = heapSettings;
        this.relativeTimeInNanos = relativeTimeInNanos;
        this.estimateValidityNanos = estimateValidity.nanos();
    }

    /// The estimate this gate decides on: the last computed value while it is still within [#ESTIMATE_VALIDITY], else recomputed
    /// inline.
    long currentEstimateBytes() {
        final EstimateSnapshot cached = lastEstimate;
        final long nowNanos = relativeTimeInNanos.getAsLong();
        if (cached != null && nowNanos - cached.computedAtNanos() < estimateValidityNanos) {
            return cached.estimatedBytes();
        }
        final long estimatedBytes = estimatedHeapUsageBytes.applyAsLong(clusterStateSupplier.get());
        lastEstimate = new EstimateSnapshot(estimatedBytes, nowNanos);
        return estimatedBytes;
    }

    @Override
    public Decision evaluate() {
        if (heapSettings.enabled() == false) {
            return Decision.RUN;
        }
        if (heapSettings.highWatermarkEnabled() == false) {
            return Decision.RUN;
        }
        // A zero max heap means the JVM did not report one; the used percentage is meaningless, so do not gate, and keep the
        // division below well-defined regardless of the minimum-heap setting.
        if (maxHeapBytes == 0) {
            return Decision.RUN;
        }
        // maxHeapBytes never changes during the node's lifetime, but the enablement threshold is dynamic, so this can flip.
        if (heapSettings.belowMinimumHeapForEnablement(maxHeapBytes)) {
            return Decision.RUN;
        }
        final long estimatedBytes;
        try {
            estimatedBytes = currentEstimateBytes();
        } catch (Exception e) {
            // Fail open: the gate is a safety valve, and a failed estimate (e.g. a shard closed mid-walk) must not break dispatch.
            logger.warn("failed to compute the estimated heap usage; allowing recoveries", e);
            return Decision.RUN;
        }
        final double usedPercent = 100.0 * estimatedBytes / maxHeapBytes;
        if (heapSettings.exceedsHighWatermark(usedPercent)) {
            // The block reason (and the eventual resume) is logged by the recovery scheduler on the blocked <-> may-run transitions.
            return Decision.block(
                NAME,
                Strings.format(
                    "estimated heap usage [%.1f%%] (%d of %d bytes) exceeds high watermark [%.1f%%]",
                    usedPercent,
                    estimatedBytes,
                    maxHeapBytes,
                    heapSettings.highWatermarkPercent()
                )
            );
        }
        return Decision.RUN;
    }
}
