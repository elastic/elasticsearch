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

import org.elasticsearch.cluster.EstimatedHeapUsage;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.FrequencyCappedAction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * The allocation decider is similar to {@link org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider}
 * but for the estimated heap usage. The dynamic setting <code>cluster.routing.allocation.estimated_heap.watermark.low</code>
 * prevents shard allocation to a node if the estimated heap usage exceeds the configured value. Note it is possible that the
 * estimated heap usage goes over the low watermark after allocating a shard. The decider currently does not actively move
 * shards away from a node which will be a future work. The decider as a whole can be disabled by setting
 * <code>cluster.routing.allocation.estimated_heap.threshold_enabled</code> to <code>false</code>.
 */
public class EstimatedHeapUsageAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(EstimatedHeapUsageAllocationDecider.class);
    private static final String NAME = "estimated_heap";

    /**
     * Below the specified heap size the decider will not intervene
     */
    public static final Setting<ByteSizeValue> MINIMUM_HEAP_SIZE_FOR_ENABLEMENT = Setting.byteSizeSetting(
        "cluster.routing.allocation.estimated_heap.minimum_heap_size_for_enablement",
        ByteSizeValue.ofGb(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<RatioValue> CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK = new Setting<>(
        "cluster.routing.allocation.estimated_heap.watermark.low",
        "100%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MINIMUM_LOGGING_INTERVAL = Setting.timeSetting(
        "cluster.routing.allocation.estimated_heap.log_interval",
        TimeValue.timeValueMinutes(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Decision YES_ESTIMATED_HEAP_USAGE_DECIDER_DISABLED = Decision.single(
        Decision.Type.YES,
        NAME,
        "estimated heap allocation decider is disabled"
    );

    private static final Decision YES_ESTIMATED_HEAP_USAGE_FOR_INDEX_NODE_ONLY = Decision.single(
        Decision.Type.YES,
        NAME,
        "estimated heap allocation decider is applicable only to index nodes"
    );

    private final FrequencyCappedAction logInterventionMessage;
    private volatile boolean enabled;
    private volatile RatioValue estimatedHeapLowWatermark;
    private volatile ByteSizeValue minimumHeapSizeForEnabled;

    public EstimatedHeapUsageAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
            this::setEnabled
        );
        logInterventionMessage = new FrequencyCappedAction(System::currentTimeMillis, TimeValue.ZERO);
        clusterSettings.initializeAndWatch(MINIMUM_LOGGING_INTERVAL, logInterventionMessage::setMinInterval);
        clusterSettings.initializeAndWatch(CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK, this::setEstimatedHeapLowWatermark);
        clusterSettings.initializeAndWatch(
            MINIMUM_HEAP_SIZE_FOR_ENABLEMENT,
            byteSizeValue -> this.minimumHeapSizeForEnabled = byteSizeValue
        );
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setEstimatedHeapLowWatermark(RatioValue estimatedHeapLowWatermark) {
        this.estimatedHeapLowWatermark = estimatedHeapLowWatermark;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (enabled == false) {
            return YES_ESTIMATED_HEAP_USAGE_DECIDER_DISABLED;
        }

        if (node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
            return YES_ESTIMATED_HEAP_USAGE_FOR_INDEX_NODE_ONLY;
        }

        final EstimatedHeapUsage estimatedHeapUsage = allocation.clusterInfo().getEstimatedHeapUsages().get(node.nodeId());
        if (estimatedHeapUsage == null) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "no estimated heap estimation available for node [%s], either a new or restarted node",
                node.nodeId()
            );
        }

        if (estimatedHeapUsage.totalBytes() < minimumHeapSizeForEnabled.getBytes()) {
            return new Decision.Single(
                Decision.Type.YES,
                NAME,
                "estimated heap decider will not intervene if heap size is below [%s]",
                minimumHeapSizeForEnabled
            );
        }

        final double heapUsedPercentage = estimatedHeapUsage.estimatedUsageAsPercentage();
        final double lowWaterMarkPercentage = estimatedHeapLowWatermark.getAsPercent();
        if (heapUsedPercentage > lowWaterMarkPercentage) {
            final String message = Strings.format(
                "insufficient estimated heap available on node [%s]: used percentage [%.2f] exceeds low watermark [%.2f]",
                node.nodeId(),
                heapUsedPercentage,
                lowWaterMarkPercentage
            );

            if (logger.isDebugEnabled()) {
                logInterventionMessage.maybeExecute(() -> logger.debug(message));
            }

            return allocation.decision(Decision.NO, NAME, message);
        } else {
            return allocation.decision(
                Decision.YES,
                NAME,
                "sufficient estimated heap available on node [%s]: used percentage [%.2f] is below low watermark [%.2f]",
                node.nodeId(),
                heapUsedPercentage,
                lowWaterMarkPercentage
            );
        }
    }
}
