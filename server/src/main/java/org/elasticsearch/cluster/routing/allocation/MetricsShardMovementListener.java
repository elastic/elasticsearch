/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.MoveReason;
import org.elasticsearch.cluster.routing.MoveType;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Publishes the aggregated APM metrics of the allocator's shard movements: how long shards spend unassigned, how many are initialized
 * and how many relocations are started, by reason.
 */
public class MetricsShardMovementListener implements ShardMovementListener {

    static final String UNASSIGNED_TO_INITIALIZING_METRIC = "es.allocator.shards.unassigned_to_initializing.duration.histogram";
    static final String UNASSIGNED_TO_STARTED_METRIC = "es.allocator.shards.unassigned_to_started.duration.histogram";
    static final String RELOCATION_STARTED_METRIC = "es.allocator.shards.relocation_started.total";
    static final String SHARD_INITIALIZING_METRIC = "es.allocator.shards.initializing.total";

    private static final Map<UnassignedInfo.Reason, Map<String, Object>> PRIMARY_ATTRIBUTES = buildAttributesByReason(true);
    private static final Map<UnassignedInfo.Reason, Map<String, Object>> REPLICA_ATTRIBUTES = buildAttributesByReason(false);

    private static Map<UnassignedInfo.Reason, Map<String, Object>> buildAttributesByReason(boolean primary) {
        return Arrays.stream(UnassignedInfo.Reason.values())
            .collect(Collectors.toUnmodifiableMap(r -> r, r -> Map.of("es_shard_primary", primary, "es_shard_reason", r.name())));
    }

    /**
     * The attributes of {@link #RELOCATION_STARTED_METRIC}
     */
    record RelocationAttributes(MoveType type, @Nullable String canRemainDecider, boolean primary) {

        static RelocationAttributes of(MoveReason reason, boolean primary) {
            return new RelocationAttributes(reason.type(), reason.canRemainDecider(), primary);
        }

        Map<String, Object> toMap() {
            return Map.of(
                "es_relocation_reason",
                type.reason(),
                "es_can_remain_decider",
                canRemainDecider == null ? "missing" : canRemainDecider,
                "es_shard_primary",
                primary
            );
        }
    }

    // Cache the attribute maps: the set of distinct combinations is small (move types x deciders x 2)
    private final Map<RelocationAttributes, Map<String, Object>> relocationAttributes = new ConcurrentHashMap<>();

    private final LongHistogram unassignedToInitializingDuration;
    private final LongHistogram unassignedToStartedDuration;
    private final LongCounter shardInitializingCounter;
    private final LongCounter relocationStartedCounter;
    private final LongSupplier currentTimeMillisSupplier;

    public MetricsShardMovementListener(MeterRegistry meterRegistry) {
        this(meterRegistry, System::currentTimeMillis);
    }

    MetricsShardMovementListener(MeterRegistry meterRegistry, LongSupplier currentTimeMillisSupplier) {
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
        // Relocation target shards have no unassignedInfo
        UnassignedInfo info = initializingShard.unassignedInfo();
        if (info != null) {
            long durationMillis = currentTimeMillisSupplier.getAsLong() - info.unassignedTimeMillis();
            unassignedToStartedDuration.record(Math.max(0, durationMillis), attributes(info, startedShard));
        }
    }

    @Override
    public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard, MoveReason reason) {
        relocationStartedCounter.incrementBy(
            1,
            relocationAttributes.computeIfAbsent(RelocationAttributes.of(reason, startedShard.primary()), RelocationAttributes::toMap)
        );
    }

    private static Map<String, Object> attributes(UnassignedInfo info, ShardRouting shard) {
        return (shard.primary() ? PRIMARY_ATTRIBUTES : REPLICA_ATTRIBUTES).get(info.reason());
    }
}
