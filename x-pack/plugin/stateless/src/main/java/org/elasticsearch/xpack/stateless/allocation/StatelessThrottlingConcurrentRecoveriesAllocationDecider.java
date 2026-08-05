/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;

import static java.lang.Math.max;
import static java.lang.Math.round;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.THROTTLE;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.initializingShard;

/**
 * This allocation decider limits the number of concurrent relocation of indexing shards depending on the node's heap size. On small
 * nodes (as defined by the setting {@code cluster.routing.allocation.min_heap_required_for_concurrent_primary_recoveries}) we allow
 * only one relocation at a time.
 */
public class StatelessThrottlingConcurrentRecoveriesAllocationDecider extends AllocationDecider {
    private static final String NAME = "stateless_throttling_concurrent_recoveries";

    public static final Setting<ByteSizeValue> MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING = Setting.memorySizeSetting(
        "cluster.routing.allocation.min_heap_required_for_concurrent_primary_recoveries",
        // default is 2.5GB heap which prevents concurrent recoveries on nodes with 2GB and 4GB memory
        ByteSizeValue.ofMb(2560),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Double> CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB = Setting.doubleSetting(
        "cluster.routing.allocation.concurrent_primary_recoveries_per_heap_gb",
        1.5,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile long minNodeHeapRequiredForNodeConcurrentRecoveries;
    private volatile double recoveriesPerHeapGB;

    public StatelessThrottlingConcurrentRecoveriesAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING,
            value -> this.minNodeHeapRequiredForNodeConcurrentRecoveries = value.getBytes()
        );
        clusterSettings.initializeAndWatch(CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB, value -> this.recoveriesPerHeapGB = value);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.isPromotableToPrimary() && shardRouting.unassigned() == false) {
            // Peer recovery
            assert initializingShard(shardRouting, node.nodeId()).recoverySource().getType() == RecoverySource.Type.PEER;
            int currentInRecoveries = allocation.routingNodes().getIncomingRecoveries(node.nodeId());
            ByteSizeValue heapSize = allocation.clusterInfo().getMaxHeapSizePerNode().get(node.nodeId());
            if (currentInRecoveries > 0) {
                if (throttleOnMinRequiredHeapSize(heapSize)) {
                    return allocation.decision(
                        THROTTLE,
                        NAME,
                        "node is not large enough (node max heap size: %s) to do concurrent recoveries "
                            + "[incoming shard recoveries: %d], cluster setting [%s=%s]",
                        allocation.clusterInfo().getMaxHeapSizePerNode().get(node.nodeId()),
                        currentInRecoveries,
                        MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey(),
                        ByteSizeValue.ofBytes(minNodeHeapRequiredForNodeConcurrentRecoveries)
                    );
                }
                if (throttleOnAllowedConcurrentRecoveriesForHeapSize(heapSize, currentInRecoveries)) {
                    long allowedRecoveries = allowedConcurrentRecoveriesForHeapSize(heapSize);
                    return allocation.decision(
                        THROTTLE,
                        NAME,
                        "Node is not allowed to do more concurrent recoveries. "
                            + "Incoming shard recoveries: [%d]. Allowed recoveries: [%d], "
                            + "based on node max heap size [%s] and setting [%s=%f]. "
                            + "Need larger node or update setting.",
                        currentInRecoveries,
                        allowedRecoveries,
                        heapSize,
                        CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey(),
                        recoveriesPerHeapGB
                    );
                }
            }
        }
        return Decision.YES;
    }

    // Whether we should throttle based on min required heap size for concurrent recoveries
    private boolean throttleOnMinRequiredHeapSize(ByteSizeValue nodeHeapSize) {
        if (nodeHeapSize == null) {
            // Not enough information to decide!
            return false;
        }
        return nodeHeapSize.getBytes() < minNodeHeapRequiredForNodeConcurrentRecoveries;
    }

    // Whether we should throttle based on allowed concurrent recoveries for given heap size
    private boolean throttleOnAllowedConcurrentRecoveriesForHeapSize(ByteSizeValue nodeHeap, int currentInRecoveries) {
        if (nodeHeap == null) {
            // Not enough information to decide!
            return false;
        }

        return allowedConcurrentRecoveriesForHeapSize(nodeHeap) <= currentInRecoveries;
    }

    private long allowedConcurrentRecoveriesForHeapSize(ByteSizeValue nodeHeap) {
        return max(1, round(nodeHeap.getGbFrac() * recoveriesPerHeapGB));
    }
}
