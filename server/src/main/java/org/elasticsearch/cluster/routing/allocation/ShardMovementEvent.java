/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.RelocationReason;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;

/**
 * A change to the location of a shard copy, made by the master while computing a new routing table. Events are delivered to
 * {@link ShardMovementEventSink}s as they are made, i.e. before the resulting cluster state is published: if publication fails the
 * change is discarded and a later allocation round may emit the same event again.
 * <p>
 * Each event names the shard copy it concerns via {@link #shard()}, which is the copy <em>before</em> the change was applied.
 */
public sealed interface ShardMovementEvent {

    /**
     * The shard copy this event concerns, as it was before the change.
     */
    ShardRouting shard();

    /**
     * A started shard copy began relocating from {@link #shard()}'s node to {@link #target()}'s node.
     */
    record RelocationStarted(ShardRouting shard, ShardRouting target, RelocationReason reason) implements ShardMovementEvent {}

    /**
     * A relocation completed: the relocation target started and the relocation source {@link #shard()} was removed.
     */
    record RelocationCompleted(ShardRouting shard) implements ShardMovementEvent {}

    /**
     * An unassigned shard copy {@link #shard()} was assigned to a node, as {@link #initializedShard()}. Does not include relocation
     * targets, which are reported by {@link RelocationStarted}.
     */
    record ShardInitialized(ShardRouting shard, ShardRouting initializedShard) implements ShardMovementEvent {}

    /**
     * An initializing shard copy {@link #shard()} started, as {@link #startedShard()}. Includes relocation targets.
     */
    record ShardStarted(ShardRouting shard, ShardRouting startedShard) implements ShardMovementEvent {}

    /**
     * An assigned shard copy {@link #shard()} failed or was cancelled. Cancelled relocations are reported here too, with the
     * cancellation cause in {@link UnassignedInfo#message()} and {@link UnassignedInfo#failure()}.
     */
    record ShardFailed(ShardRouting shard, UnassignedInfo unassignedInfo) implements ShardMovementEvent {}
}
