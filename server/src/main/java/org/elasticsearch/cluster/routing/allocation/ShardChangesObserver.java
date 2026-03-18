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
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Arrays;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/// Observes shard state transitions during allocation rounds, logging them and emitting APM timing metrics.
public class ShardChangesObserver implements RoutingChangesObserver {

    public static final String UNASSIGNED_TO_INITIALIZING_METRIC = "es.allocator.shards.unassigned_to_initializing.duration.histogram";
    public static final String UNASSIGNED_TO_STARTED_METRIC = "es.allocator.shards.unassigned_to_started.duration.histogram";

    private static final Map<Boolean, Map<UnassignedInfo.Reason, Map<String, Object>>> PRIMARY_ATTRIBUTES = Map.of(
        false,
        buildAttributesByReason(true, false),
        true,
        buildAttributesByReason(true, true)
    );
    private static final Map<Boolean, Map<UnassignedInfo.Reason, Map<String, Object>>> REPLICA_ATTRIBUTES = Map.of(
        false,
        buildAttributesByReason(false, false),
        true,
        buildAttributesByReason(false, true)
    );

    private static Map<UnassignedInfo.Reason, Map<String, Object>> buildAttributesByReason(boolean primary, boolean delayed) {
        return Arrays.stream(UnassignedInfo.Reason.values())
            .collect(Collectors.toUnmodifiableMap(r -> r, r -> Map.of("primary", primary, "reason", r.name(), "delayed", delayed)));
    }

    public static final ShardChangesObserver NOOP = new ShardChangesObserver(MeterRegistry.NOOP);

    private static final Logger logger = LogManager.getLogger(ShardChangesObserver.class);

    private final LongHistogram unassignedToInitializingDuration;
    private final LongHistogram unassignedToStartedDuration;
    private final LongSupplier currentTimeMillisSupplier;

    public ShardChangesObserver(MeterRegistry meterRegistry) {
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
            unassignedToInitializingDuration.record(Math.max(0, durationMillis), attributes(info, initializedShard));
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
        return (shard.primary() ? PRIMARY_ATTRIBUTES : REPLICA_ATTRIBUTES).get(info.delayed()).get(info.reason());
    }
}
