/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * Base class for heap-usage-based allocation deciders. Provides the full allocation decision skeleton:
 * <ol>
 *   <li>common node guard (enabled, index-role, metrics, minimum-heap)</li>
 *   <li>current-usage vs. low/high watermark</li>
 *   <li>projected-usage vs. low watermark (in {@code canAllocate})</li>
 * </ol>
 * Subclasses supply what varies: the capacity source, the current-usage metric, the projected addition,
 * and the watermark/enabled settings, via abstract methods.
 */
public abstract class AbstractEstimatedHeapAllocationDecider extends AllocationDecider {

    /**
     * Below the specified heap size the decider will not intervene.
     */
    public static final Setting<ByteSizeValue> MINIMUM_HEAP_SIZE_FOR_ENABLEMENT = Setting.byteSizeSetting(
        "cluster.routing.allocation.estimated_heap.minimum_heap_size_for_enablement",
        ByteSizeValue.ofGb(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> MINIMUM_LOGGING_INTERVAL = Setting.timeSetting(
        "cluster.routing.allocation.estimated_heap.log_interval",
        TimeValue.timeValueMinutes(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final Logger logger;
    private final String name;
    private final String deciderDescription;
    private volatile ByteSizeValue minimumHeapSizeForEnabled;

    // Pre-built decisions for cases whose explanation is fixed per decider instance. Avoids String
    // concatenation and Decision.Single allocation on every call through these guard checks.
    private final Decision disabledDecision;
    private final Decision notIndexNodeDecision;
    private final Decision canRemainDisabledDecision;

    protected final FrequencyCappedAction logCanRemainMessage;
    protected final FrequencyCappedAction logCanAllocateMessage;

    /**
     * @param name              allocation-decider name passed to {@link RoutingAllocation#decision}
     * @param deciderDescription human-readable description used in decision messages, e.g. {@code "estimated heap"}
     */
    protected AbstractEstimatedHeapAllocationDecider(String name, String deciderDescription, ClusterSettings clusterSettings) {
        this.logger = LogManager.getLogger(getClass());
        this.name = name;
        this.deciderDescription = deciderDescription;
        this.disabledDecision = Decision.single(Decision.Type.YES, name, deciderDescription + " allocation decider is disabled");
        this.notIndexNodeDecision = Decision.single(
            Decision.Type.YES,
            name,
            deciderDescription + " allocation decider is applicable only to index nodes"
        );
        this.canRemainDisabledDecision = Decision.single(Decision.Type.YES, name, deciderDescription + " decider can remain disabled");
        logCanRemainMessage = new FrequencyCappedAction(System::currentTimeMillis, TimeValue.ZERO);
        logCanAllocateMessage = new FrequencyCappedAction(System::currentTimeMillis, TimeValue.ZERO);
        clusterSettings.initializeAndWatch(MINIMUM_LOGGING_INTERVAL, timeValue -> {
            logCanRemainMessage.setMinInterval(timeValue);
            logCanAllocateMessage.setMinInterval(timeValue);
        });
        clusterSettings.initializeAndWatch(MINIMUM_HEAP_SIZE_FOR_ENABLEMENT, v -> this.minimumHeapSizeForEnabled = v);
    }

    // --- Abstract hooks ---

    /** Whether this decider is currently enabled. */
    protected abstract boolean isEnabled();

    /** Low-watermark threshold as a percentage (0–100). */
    protected abstract double getLowWatermarkPercent();

    /** High-watermark threshold as a percentage (0–100). */
    protected abstract double getHighWatermarkPercent();

    /** Whether the high-watermark check in {@code canRemain} is active. */
    protected abstract boolean isHighWatermarkEnabled();

    /**
     * Returns the capacity of the resource being measured in bytes, or {@code null} if unavailable
     * (the decider will yield YES and defer to other deciders).
     * <p>
     * Called only after the common guard has passed (so {@code metrics} is guaranteed non-null).
     */
    protected abstract @Nullable Long resolveCapacityBytes(NodeHeapMetrics metrics, RoutingNode node, RoutingAllocation allocation);

    /**
     * Returns the current bytes of the measured resource in use on the node.
     * <p>
     * Called only after the common guard has passed (so {@code metrics} is guaranteed non-null).
     */
    protected abstract long getCurrentUsageBytes(NodeHeapMetrics metrics);

    /**
     * Returns the additional bytes that allocating {@code shard} onto {@code node} would add to the
     * measured resource. Only shard-heap costs should be included here; index-metadata costs are
     * included only when the node does not yet host the index.
     */
    protected abstract long getProjectedAdditionalBytes(ShardRouting shard, RoutingNode node, ShardAndIndexHeapUsage usage);

    // --- Template methods ---

    @Override
    public final Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final Decision guard = guardDecision(node, allocation);
        if (guard != null) {
            return guard;
        }

        final NodeHeapMetrics nodeHeapMetrics = allocation.clusterInfo().getNodeHeapMetrics().get(node.nodeId());
        assert nodeHeapMetrics != null : "expected heap metrics for node after guard passed";

        final Long capacityBytes = resolveCapacityBytes(nodeHeapMetrics, node, allocation);
        if (capacityBytes == null) {
            return allocation.decision(
                Decision.YES,
                name,
                "no %s capacity data available for node [%s]",
                deciderDescription,
                node.nodeId()
            );
        }

        final double lowWatermarkPercent = getLowWatermarkPercent();
        final long currentUsageBytes = getCurrentUsageBytes(nodeHeapMetrics);
        final double currentUsagePercent = 100.0 * currentUsageBytes / capacityBytes;

        if (currentUsagePercent > lowWatermarkPercent) {
            if (logger.isDebugEnabled() || allocation.debugDecision()) {
                final String message = Strings.format(
                    "insufficient %s available on node [%s]: usage percentage [%.2f] exceeds low watermark [%.2f]",
                    deciderDescription,
                    node.nodeId(),
                    currentUsagePercent,
                    lowWatermarkPercent
                );
                if (logger.isDebugEnabled()) {
                    logCanAllocateMessage.maybeExecute(() -> logger.debug(message));
                }
                return allocation.decision(Decision.NO, name, message);
            } else {
                return Decision.NO;
            }
        }

        final var shardAndIndexHeapUsage = allocation.clusterInfo().getEstimatedShardHeapUsages().get(shardRouting.shardId());
        if (shardAndIndexHeapUsage == null) {
            return allocation.decision(
                Decision.YES,
                name,
                "sufficient %s available on node [%s]: usage percentage [%.2f] is below low watermark [%.2f]",
                deciderDescription,
                node.nodeId(),
                currentUsagePercent,
                lowWatermarkPercent
            );
        }

        final long additionalBytes = getProjectedAdditionalBytes(shardRouting, node, shardAndIndexHeapUsage);
        final double projectedUsagePercent = 100.0 * (currentUsageBytes + additionalBytes) / capacityBytes;

        if (projectedUsagePercent > lowWatermarkPercent) {
            if (logger.isDebugEnabled() || allocation.debugDecision()) {
                final String message = Strings.format(
                    "insufficient %s available on node [%s]: shard [%s] would add [%d] bytes, increasing the %s usage percentage "
                        + "from [%.2f] to [%.2f], which exceeds low watermark [%.2f]",
                    deciderDescription,
                    node.nodeId(),
                    shardRouting.shardId(),
                    additionalBytes,
                    deciderDescription,
                    currentUsagePercent,
                    projectedUsagePercent,
                    lowWatermarkPercent
                );
                if (logger.isDebugEnabled()) {
                    logCanAllocateMessage.maybeExecute(() -> logger.debug(message));
                }
                return allocation.decision(Decision.NO, name, message);
            } else {
                return Decision.NO;
            }
        }

        return allocation.decision(
            Decision.YES,
            name,
            "sufficient %s available on node [%s]: projected usage percentage [%.2f] is below low watermark [%.2f]",
            deciderDescription,
            node.nodeId(),
            projectedUsagePercent,
            lowWatermarkPercent
        );
    }

    @Override
    public final Decision canRemain(
        IndexMetadata indexMetadata,
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation
    ) {
        final Decision guard = guardDecision(node, allocation);
        if (guard != null) {
            return guard;
        }

        if (isHighWatermarkEnabled() == false) {
            return allocation.debugDecision() ? canRemainDisabledDecision : Decision.YES;
        }

        final NodeHeapMetrics nodeHeapMetrics = allocation.clusterInfo().getNodeHeapMetrics().get(node.nodeId());
        assert nodeHeapMetrics != null : "expected heap metrics for node after guard passed";

        final Long capacityBytes = resolveCapacityBytes(nodeHeapMetrics, node, allocation);
        if (capacityBytes == null) {
            return allocation.decision(
                Decision.YES,
                name,
                "no %s capacity data available for node [%s]",
                deciderDescription,
                node.nodeId()
            );
        }

        final double highWatermarkPercent = getHighWatermarkPercent();
        final long currentUsageBytes = getCurrentUsageBytes(nodeHeapMetrics);
        final double currentUsagePercent = 100.0 * currentUsageBytes / capacityBytes;

        if (currentUsagePercent > highWatermarkPercent) {
            if (logger.isDebugEnabled() || allocation.debugDecision()) {
                final String message = Strings.format(
                    "insufficient %s available on node [%s]: usage percentage [%.2f] exceeds high watermark [%.2f]",
                    deciderDescription,
                    node.nodeId(),
                    currentUsagePercent,
                    highWatermarkPercent
                );
                if (logger.isDebugEnabled()) {
                    logCanRemainMessage.maybeExecute(() -> logger.debug(message));
                }
                return allocation.decision(Decision.NO, name, message);
            } else {
                return Decision.NO;
            }
        }

        return allocation.decision(
            Decision.YES,
            name,
            "sufficient %s available on node [%s]: usage percentage [%.2f] is below high watermark [%.2f]",
            deciderDescription,
            node.nodeId(),
            currentUsagePercent,
            highWatermarkPercent
        );
    }

    /**
     * Checks the common preconditions that apply to both {@code canAllocate} and {@code canRemain}.
     *
     * @return a YES {@link Decision} if any precondition is not met (caller must return it immediately),
     *         or {@code null} if all preconditions pass and the caller should proceed with its decision logic
     */
    private @Nullable Decision guardDecision(RoutingNode node, RoutingAllocation allocation) {
        if (isEnabled() == false) {
            return allocation.debugDecision() ? disabledDecision : Decision.YES;
        }

        if (node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
            return allocation.debugDecision() ? notIndexNodeDecision : Decision.YES;
        }

        final NodeHeapMetrics nodeHeapMetrics = allocation.clusterInfo().getNodeHeapMetrics().get(node.nodeId());
        if (nodeHeapMetrics == null) {
            return allocation.decision(
                Decision.YES,
                name,
                "no estimated heap estimation available for node [%s], either a new or restarted node",
                node.nodeId()
            );
        }

        if (nodeHeapMetrics.totalBytes() < minimumHeapSizeForEnabled.getBytes()) {
            return allocation.decision(
                Decision.YES,
                name,
                "estimated heap decider will not intervene if heap size is below [%s]",
                minimumHeapSizeForEnabled
            );
        }

        return null;
    }
}
