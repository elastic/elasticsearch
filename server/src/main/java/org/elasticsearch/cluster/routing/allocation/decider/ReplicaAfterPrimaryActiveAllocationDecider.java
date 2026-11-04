/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

/**
 * An allocation strategy that only allows for a replica to be allocated when the primary is active.
 */
public class ReplicaAfterPrimaryActiveAllocationDecider extends AllocationDecider {

    private static final String NAME = "replica_after_primary_active";

    /**
     * Fast activation prototype (stateless only, default off): an unpromotable copy (stateless search shard) reads its data from the object
     * store and never recovers from the primary, so it may be allocated and recovered while the primary is unassigned (e.g. a parked
     * indexing shard). Also read by {@link ThrottlingAllocationDecider} and {@code IndicesClusterStateService}.
     */
    public static final Setting<Boolean> UNPROMOTABLE_WITHOUT_PRIMARY_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.unpromotable_without_primary.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Node-wide copy of {@link #UNPROMOTABLE_WITHOUT_PRIMARY_ENABLED_SETTING} for the relaxed assertions and recovery accounting in
     * {@code RoutingNodes} and {@code IndexShardRoutingTable}, which have no {@link ClusterSettings}: with it off they run their original
     * code. Prototype shortcut: a mutable static (one value per JVM, last writer wins) rather than threading the setting through.
     */
    public static volatile boolean UNPROMOTABLE_WITHOUT_PRIMARY = false;

    private volatile boolean unpromotableWithoutPrimary = false;

    public ReplicaAfterPrimaryActiveAllocationDecider() {}

    public ReplicaAfterPrimaryActiveAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(UNPROMOTABLE_WITHOUT_PRIMARY_ENABLED_SETTING, value -> {
            this.unpromotableWithoutPrimary = value;
            UNPROMOTABLE_WITHOUT_PRIMARY = value;
        });
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            return allocation.decision(Decision.YES, NAME, "shard is primary and can be allocated");
        }
        ShardRouting primary = allocation.routingNodes().activePrimary(shardRouting.shardId());
        if (primary == null && unpromotableWithoutPrimary && shardRouting.isPromotableToPrimary() == false) {
            return allocation.decision(Decision.YES, NAME, "unpromotable copy may be allocated without an active primary");
        }
        if (primary == null) {
            return allocation.decision(Decision.NO, NAME, "primary shard for this replica is not yet active");
        }
        return allocation.decision(Decision.YES, NAME, "primary shard for this replica is already active");
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node, allocation);
    }
}
