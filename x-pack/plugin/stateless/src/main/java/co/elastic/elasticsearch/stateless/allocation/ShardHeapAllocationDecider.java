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

import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.ShardHeapUsage;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * The shard heap allocation decider is similar to {@link org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider}
 * but for the estimated shard heap usage. The dynamic setting <code>cluster.routing.allocation.shard_heap.watermark.low</code>
 * prevents shard allocation to a node if the estimated shard heap usage exceeds the configured value. Note it is possible that the
 * estimated shard heap usage goes over the low watermark after allocating a shard. The decider currently does not actively move
 * shards away from a node which will be a future work. The decider as a whole can be disabled by setting
 * <code>cluster.routing.allocation.shard_heap.threshold_enabled</code> to <code>false</code>.
 */
public class ShardHeapAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(ShardHeapAllocationDecider.class);
    private static final String NAME = "shard_heap";

    public static final Setting<RatioValue> CLUSTER_ROUTING_ALLOCATION_SHARD_HEAP_LOW_WATERMARK = new Setting<>(
        "cluster.routing.allocation.shard_heap.watermark.low",
        "100%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Decision YES_SHARD_HEAP_DECIDER_DISABLED = Decision.single(
        Decision.Type.YES,
        NAME,
        "shard heap allocation decider is disabled"
    );

    private static final Decision YES_SHARD_HEAP_FOR_INDEX_NODE_ONLY = Decision.single(
        Decision.Type.YES,
        NAME,
        "shard heap allocation decider is applicable only to index nodes"
    );

    private volatile boolean enabled;
    private volatile RatioValue shardHeapLowWatermark;

    public ShardHeapAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_SHARD_HEAP_THRESHOLD_DECIDER_ENABLED,
            this::setEnabled
        );
        clusterSettings.initializeAndWatch(CLUSTER_ROUTING_ALLOCATION_SHARD_HEAP_LOW_WATERMARK, this::setShardHeapLowWatermark);
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setShardHeapLowWatermark(RatioValue shardHeapLowWatermark) {
        this.shardHeapLowWatermark = shardHeapLowWatermark;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (enabled == false) {
            return YES_SHARD_HEAP_DECIDER_DISABLED;
        }

        if (node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
            return YES_SHARD_HEAP_FOR_INDEX_NODE_ONLY;
        }

        final ShardHeapUsage shardHeapUsage = allocation.clusterInfo().getShardHeapUsages().get(node.nodeId());
        if (shardHeapUsage == null) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "no shard heap estimation available for node [%s], either a new or restarted node",
                node.nodeId()
            );
        }

        final double heapUsedPercentage = shardHeapUsage.estimatedUsageAsPercentage();
        final double lowWaterMarkPercentage = shardHeapLowWatermark.getAsPercent();
        if (heapUsedPercentage > lowWaterMarkPercentage) {
            final String message = Strings.format(
                "insufficient shard heap available on node [%s]: used percentage [%.2f] exceeds low watermark [%.2f]",
                node.nodeId(),
                heapUsedPercentage,
                lowWaterMarkPercentage
            );
            logger.debug(message);
            return allocation.decision(Decision.NO, NAME, message);
        } else {
            return allocation.decision(
                Decision.YES,
                NAME,
                "sufficient shard heap available on node [%s]: used percentage [%.2f] is below low watermark [%.2f]",
                node.nodeId(),
                heapUsedPercentage,
                lowWaterMarkPercentage
            );
        }
    }
}
