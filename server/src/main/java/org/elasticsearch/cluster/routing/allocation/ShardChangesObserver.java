/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Arrays;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.MOVE_CANNOT_REMAIN_REASON;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.MOVE_NOT_PREFERRED_REASON;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.REBALANCE_REASON;

/// Observes shard state transitions during allocation rounds, logging them and emitting APM timing metrics.
class ShardChangesObserver implements RoutingChangesObserver {
    private static final Logger logger = LogManager.getLogger(ShardChangesObserver.class);

    static final String UNASSIGNED_TO_INITIALIZING_METRIC = "es.allocator.shards.unassigned_to_initializing.duration.histogram";
    static final String UNASSIGNED_TO_STARTED_METRIC = "es.allocator.shards.unassigned_to_started.duration.histogram";
    static final String RELOCATION_STARTED_METRIC = "es.allocator.shards.relocation_started.total";
    static final String SHARD_INITIALIZING_METRIC = "es.allocator.shards.initializing.total";

    private static final Map<UnassignedInfo.Reason, Map<String, Object>> PRIMARY_ATTRIBUTES = buildAttributesByReason(true);
    private static final Map<UnassignedInfo.Reason, Map<String, Object>> REPLICA_ATTRIBUTES = buildAttributesByReason(false);
    // Pre-calculate attributes for the most common move reasons
    static final Map<String, Map<String, Object>> PRIMARY_RELOCATION_ATTRIBUTES = buildRelocationAttributes(true);
    static final Map<String, Map<String, Object>> REPLICA_RELOCATION_ATTRIBUTES = buildRelocationAttributes(false);

    private static Map<UnassignedInfo.Reason, Map<String, Object>> buildAttributesByReason(boolean primary) {
        return Arrays.stream(UnassignedInfo.Reason.values())
            .collect(Collectors.toUnmodifiableMap(r -> r, r -> Map.of("es_shard_primary", primary, "es_shard_reason", r.name())));
    }

    private static Map<String, Map<String, Object>> buildRelocationAttributes(boolean primary) {
        return Map.of(
            REBALANCE_REASON,
            Map.of("es_relocation_reason", REBALANCE_REASON, "es_shard_primary", primary),
            MOVE_CANNOT_REMAIN_REASON,
            Map.of("es_relocation_reason", MOVE_CANNOT_REMAIN_REASON, "es_shard_primary", primary),
            MOVE_NOT_PREFERRED_REASON,
            Map.of("es_relocation_reason", MOVE_NOT_PREFERRED_REASON, "es_shard_primary", primary)
        );
    }

    private final LongHistogram unassignedToInitializingDuration;
    private final LongHistogram unassignedToStartedDuration;
    private final LongCounter shardInitializingCounter;
    private final LongCounter relocationStartedCounter;
    private final LongSupplier currentTimeMillisSupplier;

    ShardChangesObserver(MeterRegistry meterRegistry) {
        this(meterRegistry, System::currentTimeMillis);
    }

    ShardChangesObserver(MeterRegistry meterRegistry, LongSupplier currentTimeMillisSupplier) {
        this.unassignedToInitializingDuration = meterRegistry.registerLongHistogram(
            UNASSIGNED_TO_INITIALIZING_METRIC,
            "Duration a shard spent in UNASSIGNED state before being assigned to a node",
            "ms"
        );
        this.unassignedToStartedDuration = meterRegistry.registerLongHistogram(
            UNASSIGNED_TO_STARTED_METRIC,
            "Total duration from when a shard became UNASSIGNED to when it became STARTED",
            "ms"
        );
        this.shardInitializingCounter = meterRegistry.registerLongCounter(
            SHARD_INITIALIZING_METRIC,
            "Total number of shards moved from UNASSIGNED to INITIALIZING by the allocator",
            "{shard}"
        );
        this.relocationStartedCounter = meterRegistry.registerLongCounter(
            RELOCATION_STARTED_METRIC,
            "Total number of shard relocations started by the allocator",
            "{shard}"
        );
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
    }

    @Override
    public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
        logger.trace(
            "{} initializing from {} on node [{}]",
            shardIdentifier(initializedShard),
            initializedShard.recoverySource().getType(),
            initializedShard.currentNodeId()
        );
        UnassignedInfo info = unassignedShard.unassignedInfo();
        if (info != null) {
            long durationMillis = currentTimeMillisSupplier.getAsLong() - info.unassignedTimeMillis();
            final var attrs = attributes(info, initializedShard);
            unassignedToInitializingDuration.record(Math.max(0, durationMillis), attrs);
            shardInitializingCounter.incrementBy(1, attrs);
        }
    }

    @Override
    public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
        logger.debug(
            "{} started from {} on node [{}]",
            shardIdentifier(startedShard),
            initializingShard.recoverySource().getType(),
            startedShard.currentNodeId()
        );
        // Relocation target shards have no unassignedInfo
        UnassignedInfo info = initializingShard.unassignedInfo();
        if (info != null) {
            long durationMillis = currentTimeMillisSupplier.getAsLong() - info.unassignedTimeMillis();
            unassignedToStartedDuration.record(Math.max(0, durationMillis), attributes(info, startedShard));
        }
    }

    @Override
    public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard, String reason) {
        logger.debug(
            "{} is relocating ({}) from [{}] to [{}]",
            shardIdentifier(startedShard),
            reason,
            startedShard.currentNodeId(),
            targetRelocatingShard.currentNodeId()
        );
        relocationStartedCounter.incrementBy(1, relocationAttributes(reason, startedShard.primary()));
    }

    private static Map<String, Object> relocationAttributes(String reason, boolean primary) {
        final var attrs = (primary ? PRIMARY_RELOCATION_ATTRIBUTES : REPLICA_RELOCATION_ATTRIBUTES).get(reason);
        if (attrs != null) {
            return attrs;
        }
        return Map.of("es_relocation_reason", reason, "es_shard_primary", primary);
    }

    @Override
    public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
        logger.debug("{} has failed on [{}]: {}", shardIdentifier(failedShard), failedShard.currentNodeId(), unassignedInfo.reason());
    }

    @Override
    public void replicaPromoted(ShardRouting replicaShard) {
        logger.debug("{} is promoted to primary on [{}]", shardIdentifier(replicaShard), replicaShard.currentNodeId());
    }

    private static String shardIdentifier(ShardRouting shardRouting) {
        return shardRouting.shardId().toString() + '[' + (shardRouting.primary() ? 'P' : 'R') + ']';
    }

    private static Map<String, Object> attributes(UnassignedInfo info, ShardRouting shard) {
        return (shard.primary() ? PRIMARY_ATTRIBUTES : REPLICA_ATTRIBUTES).get(info.reason());
    }
}
