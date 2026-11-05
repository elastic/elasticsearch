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
import org.elasticsearch.cluster.routing.MoveReason;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.logging.ESLogMessage;

/**
 * Logs every shard movement event as a structured {@link ESLogMessage} at DEBUG. Enable with
 * {@code logger.org.elasticsearch.cluster.routing.allocation.LoggingShardMovementEventSink: DEBUG}. Event fields are prefixed
 * {@code es.shard.} (the copy concerned) and {@code es.relocation.} (relocation details).
 */
public class LoggingShardMovementEventSink implements ShardMovementEventSink {

    private static final Logger logger = LogManager.getLogger(LoggingShardMovementEventSink.class);

    public static final String EVENT_ACTION = "event.action";
    public static final String INDEX = "es.shard.index";
    public static final String INDEX_UUID = "es.shard.index_uuid";
    public static final String SHARD = "es.shard";
    public static final String SHARD_ID = "es.shard.id";
    public static final String PRIMARY = "es.shard.primary";
    public static final String ROLE = "es.shard.role";
    public static final String ALLOCATION_ID = "es.shard.allocation_id";
    public static final String NODE = "es.shard.node";
    public static final String TARGET_NODE = "es.relocation.target_node";
    public static final String REASON = "es.relocation.reason";
    public static final String CAN_REMAIN_DECIDER = "es.relocation.can_remain_decider";
    public static final String RECOVERY_PRIORITY = "es.relocation.recovery_priority";
    public static final String UNASSIGNED_REASON = "es.shard.unassigned.reason";
    public static final String UNASSIGNED_MESSAGE = "es.shard.unassigned.message";
    public static final String UNASSIGNED_FAILURE = "es.shard.unassigned.failure";
    public static final String RECOVERY_SOURCE = "es.shard.recovery_source";

    @Override
    public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                message("shard_initialized", unassignedShard, "{} initializing on [{}] from {}: {}").argAndField(
                    NODE,
                    initializedShard.currentNodeId()
                )
                    .argAndField(RECOVERY_SOURCE, initializedShard.recoverySource().getType())
                    .argAndField(
                        UNASSIGNED_REASON,
                        unassignedShard.unassignedInfo() == null ? "unknown" : unassignedShard.unassignedInfo().reason()
                    )
                    .field(RECOVERY_PRIORITY, initializedShard.recoveryPriority())
            );
        }
    }

    @Override
    public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
        if (logger.isDebugEnabled()) {
            logger.debug(message("shard_started", initializingShard, "{} started on [{}]").argAndField(NODE, startedShard.currentNodeId()));
        }
    }

    @Override
    public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard, MoveReason reason) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                message("shard_relocation_started", startedShard, "{} relocating from [{}] to [{}]: {}").argAndField(
                    NODE,
                    startedShard.currentNodeId()
                )
                    .argAndField(TARGET_NODE, targetRelocatingShard.currentNodeId())
                    .argAndField(REASON, reason.type().reason())
                    .field(CAN_REMAIN_DECIDER, reason.canRemainDecider())
                    .field(RECOVERY_PRIORITY, targetRelocatingShard.recoveryPriority())
            );
        }
    }

    @Override
    public void relocationCompleted(ShardRouting removedRelocationSource) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                message("shard_relocation_completed", removedRelocationSource, "{} relocated from [{}] to [{}]").argAndField(
                    NODE,
                    removedRelocationSource.currentNodeId()
                ).argAndField(TARGET_NODE, removedRelocationSource.relocatingNodeId())
            );
        }
    }

    @Override
    public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                message("shard_failed", failedShard, "{} failed on [{}]: {}").argAndField(NODE, failedShard.currentNodeId())
                    .argAndField(UNASSIGNED_REASON, unassignedInfo.reason())
                    .field(UNASSIGNED_MESSAGE, unassignedInfo.message())
                    .field(UNASSIGNED_FAILURE, unassignedInfo.failure() == null ? null : unassignedInfo.failure().getClass().getName())
                    .field(TARGET_NODE, failedShard.relocatingNodeId())
            );
        }
    }

    private static ESLogMessage message(String action, ShardRouting shard, String pattern) {
        return new ESLogMessage(pattern).argAndField(SHARD, shard.shardId().toString() + '[' + (shard.primary() ? 'P' : 'R') + ']')
            .field(EVENT_ACTION, action)
            .field(INDEX, shard.getIndexName())
            .field(SHARD_ID, shard.id())
            .field(INDEX_UUID, shard.index().getUUID())
            .field(PRIMARY, shard.primary())
            .field(ROLE, shard.role())
            .field(ALLOCATION_ID, shard.allocationId() == null ? null : shard.allocationId().getId());
    }
}
