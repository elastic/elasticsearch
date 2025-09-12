/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;

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

    private volatile long minNodeHeapRequiredForNodeConcurrentRecoveries;

    public StatelessThrottlingConcurrentRecoveriesAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING,
            value -> this.minNodeHeapRequiredForNodeConcurrentRecoveries = value.getBytes()
        );
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.isPromotableToPrimary() && shardRouting.unassigned() == false) {
            // Peer recovery
            assert initializingShard(shardRouting, node.nodeId()).recoverySource().getType() == RecoverySource.Type.PEER;
            int currentInRecoveries = allocation.routingNodes().getIncomingRecoveries(node.nodeId());
            if (currentInRecoveries > 0 && allowConcurrentRecoveries(allocation.clusterInfo(), node.nodeId()) == false) {
                // In stateless, limit concurrent recoveries of indexing shards on small nodes
                return allocation.decision(
                    THROTTLE,
                    NAME,
                    "node is not large enough (node max heap size: %s) to do concurrent recoveries "
                        + "[incoming shard recoveries: %d], cluster setting [%s=%d]",
                    allocation.clusterInfo().getMaxHeapSizePerNode().get(node.nodeId()),
                    currentInRecoveries,
                    MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey(),
                    minNodeHeapRequiredForNodeConcurrentRecoveries
                );
            }
        }
        return Decision.YES;
    }

    // Whether a node is allowed to do concurrent recoveries based on its heap size
    private boolean allowConcurrentRecoveries(ClusterInfo clusterInfo, String nodeId) {
        ByteSizeValue nodeHeapSize = clusterInfo.getMaxHeapSizePerNode().get(nodeId);
        if (nodeHeapSize == null) {
            // Not enough information to decide!
            return true;
        }
        return nodeHeapSize.getBytes() >= minNodeHeapRequiredForNodeConcurrentRecoveries;
    }
}
