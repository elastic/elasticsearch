/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Map;

import static org.elasticsearch.cluster.InternalClusterInfoService.shardIdentifierFromRouting;

/**
 * The {@link DiskThresholdDecider} checks that the node a shard is potentially
 * being allocated to has enough disk space.
 *
 * It has three configurable settings, all of which can be changed dynamically:
 *
 * <code>cluster.routing.allocation.disk.watermark.low</code> is the low disk
 * watermark. New shards will not allocated to a node with usage higher than this,
 * although this watermark may be passed by allocating a shard. It defaults to
 * 0.70 (70.0%).
 *
 * <code>cluster.routing.allocation.disk.watermark.high</code> is the high disk
 * watermark. If a node has usage higher than this, shards are not allowed to
 * remain on the node. In addition, if allocating a shard to a node causes the
 * node to pass this watermark, it will not be allowed. It defaults to
 * 0.85 (85.0%).
 *
 * Both watermark settings are expressed in terms of used disk percentage, or
 * exact byte values for free space (like "500mb")
 *
 * <code>cluster.routing.allocation.disk.threshold_enabled</code> is used to
 * enable or disable this decider. It defaults to false (disabled).
 */
public class DiskThresholdDecider extends AllocationDecider {

    private volatile Double freeDiskThresholdLow;
    private volatile Double freeDiskThresholdHigh;
    private volatile ByteSizeValue freeBytesThresholdLow;
    private volatile ByteSizeValue freeBytesThresholdHigh;
    private volatile boolean enabled;

    public static final String CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED = "cluster.routing.allocation.disk.threshold_enabled";
    public static final String CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK = "cluster.routing.allocation.disk.watermark.low";
    public static final String CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK = "cluster.routing.allocation.disk.watermark.high";

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String newLowWatermark = settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, null);
            String newHighWatermark = settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, null);
            Boolean newEnableSetting =  settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, null);

            if (newEnableSetting != null) {
                logger.info("updating [{}] from [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED,
                        DiskThresholdDecider.this.enabled, newEnableSetting);
                DiskThresholdDecider.this.enabled = newEnableSetting;
            }
            if (newLowWatermark != null) {
                if (!validWatermarkSetting(newLowWatermark)) {
                    throw new ElasticSearchParseException("Unable to parse low watermark: [" + newLowWatermark + "]");
                }
                logger.info("updating [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, newLowWatermark);
                DiskThresholdDecider.this.freeDiskThresholdLow = 100.0 - thresholdPercentageFromWatermark(newLowWatermark);
                DiskThresholdDecider.this.freeBytesThresholdLow = thresholdBytesFromWatermark(newLowWatermark);
            }
            if (newHighWatermark != null) {
                if (!validWatermarkSetting(newHighWatermark)) {
                    throw new ElasticSearchParseException("Unable to parse high watermark: [" + newHighWatermark + "]");
                }
                logger.info("updating [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, newHighWatermark);
                DiskThresholdDecider.this.freeDiskThresholdHigh = 100.0 - thresholdPercentageFromWatermark(newHighWatermark);
                DiskThresholdDecider.this.freeBytesThresholdHigh = thresholdBytesFromWatermark(newHighWatermark);
            }
        }
    }

    public DiskThresholdDecider() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public DiskThresholdDecider(Settings settings) {
        this(settings, new NodeSettingsService(settings));
    }

    @Inject
    protected DiskThresholdDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        String lowWatermark = settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "0.7");
        String highWatermark = settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "0.85");

        if (!validWatermarkSetting(lowWatermark)) {
            throw new ElasticSearchParseException("Unable to parse low watermark: [" + lowWatermark + "]");
        }
        if (!validWatermarkSetting(highWatermark)) {
            throw new ElasticSearchParseException("Unable to parse high watermark: [" + highWatermark + "]");
        }
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeDiskThresholdLow = 100.0 - thresholdPercentageFromWatermark(lowWatermark);
        this.freeDiskThresholdHigh = 100.0 - thresholdPercentageFromWatermark(highWatermark);

        this.freeBytesThresholdLow = thresholdBytesFromWatermark(lowWatermark);
        this.freeBytesThresholdHigh = thresholdBytesFromWatermark(highWatermark);

        this.enabled = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, false);
        nodeSettingsService.addListener(new ApplySettings());
    }

    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (!enabled) {
            return Decision.YES;
        }
        // Allow allocation regardless if only a single node is available
        if (allocation.nodes().size() <= 1) {
            return Decision.YES;
        }

        ClusterInfo clusterInfo = allocation.clusterInfo();
        if (clusterInfo == null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Cluster info unavailable for disk threshold decider, allowing allocation.");
            }
            return Decision.YES;
        }

        Map<String, DiskUsage> usages = clusterInfo.getNodeDiskUsages();
        Map<String, Long> shardSizes = clusterInfo.getShardSizes();
        if (usages.isEmpty()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Unable to determine disk usages for disk-aware allocation, allowing allocation");
            }
            return Decision.YES;
        }

        DiskUsage usage = usages.get(node.nodeId());
        if (usage == null) {
            // If there is no usage, and we have other nodes in the cluster,
            // use the average usage for all nodes as the usage for this node
            usage = averageUsage(node, usages);
            if (logger.isDebugEnabled()) {
                logger.debug("Unable to determine disk usage for [{}], defaulting to average across nodes [{} total] [{} free] [{}% free]",
                        node.nodeId(), usage.getTotalBytes(), usage.getFreeBytes(), usage.getFreeDiskAsPercentage());
            }
        }

        // First, check that the node currently over the low watermark
        double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        long freeBytes = usage.getFreeBytes();
        if (logger.isDebugEnabled()) {
            logger.debug("Node [{}] has {}% free disk", node.nodeId(), freeDiskPercentage);
        }
        if (freeBytes < freeBytesThresholdLow.bytes()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Less than the required {} free bytes threshold ({} bytes free) on node {}, preventing allocation",
                        freeBytesThresholdLow, freeBytes, node.nodeId());
            }
            return Decision.NO;
        }
        if (freeDiskPercentage < freeDiskThresholdLow) {
            if (logger.isDebugEnabled()) {
                logger.debug("Less than the required {}% free disk threshold ({}% free) on node [{}], preventing allocation",
                        freeDiskThresholdLow, freeDiskPercentage, node.nodeId());
            }
            return Decision.NO;
        }

        // Secondly, check that allocating the shard to this node doesn't put it above the high watermark
        Long shardSize = shardSizes.get(shardIdentifierFromRouting(shardRouting));
        shardSize = shardSize == null ? 0 : shardSize;
        double freeSpaceAfterShard = this.freeDiskPercentageAfterShardAssigned(usage, shardSize);
        long freeBytesAfterShard = freeBytes - shardSize;
        if (freeBytesAfterShard < freeBytesThresholdHigh.bytes()) {
            logger.warn("After allocating, node [{}] would have less than the required {} free bytes threshold ({} bytes free), preventing allocation",
                    node.nodeId(), freeBytesThresholdHigh, freeBytesAfterShard);
            return Decision.NO;
        }
        if (freeSpaceAfterShard < freeDiskThresholdHigh) {
            logger.warn("After allocating, node [{}] would have less than the required {}% free disk threshold ({}% free), preventing allocation",
                    node.nodeId(), freeDiskThresholdHigh, freeSpaceAfterShard);
            return Decision.NO;
        }

        return Decision.YES;
    }

    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (!enabled) {
            return Decision.YES;
        }
        // Allow allocation regardless if only a single node is available
        if (allocation.nodes().size() <= 1) {
            return Decision.YES;
        }

        ClusterInfo clusterInfo = allocation.clusterInfo();
        if (clusterInfo == null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Cluster info unavailable for disk threshold decider, allowing allocation.");
            }
            return Decision.YES;
        }

        Map<String, DiskUsage> usages = clusterInfo.getNodeDiskUsages();
        if (usages.isEmpty()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Unable to determine disk usages for disk-aware allocation, allowing allocation");
            }
            return Decision.YES;
        }

        DiskUsage usage = usages.get(node.nodeId());
        if (usage == null) {
            // If there is no usage, and we have other nodes in the cluster,
            // use the average usage for all nodes as the usage for this node
            usage = averageUsage(node, usages);
            if (logger.isDebugEnabled()) {
                logger.debug("Unable to determine disk usage for {}, defaulting to average across nodes [{} total] [{} free] [{}% free]",
                        node.nodeId(), usage.getTotalBytes(), usage.getFreeBytes(), usage.getFreeDiskAsPercentage());
            }
        }

        // If this node is already above the high threshold, the shard cannot remain (get it off!)
        double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        long freeBytes = usage.getFreeBytes();
        if (logger.isDebugEnabled()) {
            logger.debug("Node [{}] has {}% free disk ({} bytes)", node.nodeId(), freeDiskPercentage, freeBytes);
        }
        if (freeBytes < freeBytesThresholdHigh.bytes()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Less than the required {} free bytes threshold ({} bytes free) on node {}, shard cannot remain",
                        freeBytesThresholdHigh, freeBytes, node.nodeId());
            }
            return Decision.NO;
        }
        if (freeDiskPercentage < freeDiskThresholdHigh) {
            if (logger.isDebugEnabled()) {
                logger.debug("Less than the required {}% free disk threshold ({}% free) on node {}, shard cannot remain",
                        freeDiskThresholdHigh, freeDiskPercentage, node.nodeId());
            }
            return Decision.NO;
        }

        return Decision.YES;
    }

    /**
     * Returns a {@link DiskUsage} for the {@link RoutingNode} using the
     * average usage of other nodes in the disk usage map.
     * @param node Node to return an averaged DiskUsage object for
     * @param usages Map of nodeId to DiskUsage for all known nodes
     * @return DiskUsage representing given node using the average disk usage
     */
    public DiskUsage averageUsage(RoutingNode node, Map<String, DiskUsage> usages) {
        long totalBytes = 0;
        long freeBytes = 0;
        for (DiskUsage du : usages.values()) {
            totalBytes += du.getTotalBytes();
            freeBytes += du.getFreeBytes();
        }
        return new DiskUsage(node.nodeId(), totalBytes / usages.size(), freeBytes / usages.size());
    }

    /**
     * Given the DiskUsage for a node and the size of the shard, return the
     * percentage of free disk if the shard were to be allocated to the node.
     * @param usage A DiskUsage for the node to have space computed for
     * @param shardSize Size in bytes of the shard
     * @return Percentage of free space after the shard is assigned to the node
     */
    public double freeDiskPercentageAfterShardAssigned(DiskUsage usage, Long shardSize) {
        shardSize = (shardSize == null) ? 0 : shardSize;
        return 100.0 - (((double)(usage.getUsedBytes() + shardSize) / usage.getTotalBytes()) * 100.0);
    }

    /**
     * Attempts to parse the watermark into a percentage, returning 100.0% if
     * it cannot be parsed.
     */
    public double thresholdPercentageFromWatermark(String watermark) {
        try {
            return 100.0 * Double.parseDouble(watermark);
        } catch (NumberFormatException ex) {
            return 100.0;
        }
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning
     * a ByteSizeValue of 0 bytes if the value cannot be parsed.
     */
    public ByteSizeValue thresholdBytesFromWatermark(String watermark) {
        try {
            return ByteSizeValue.parseBytesSizeValue(watermark);
        } catch (ElasticSearchParseException ex) {
            return ByteSizeValue.parseBytesSizeValue("0b");
        }
    }

    /**
     * Checks if a watermark string is a valid percentage or byte size value,
     * returning true if valid, false if invalid.
     */
    public boolean validWatermarkSetting(String watermark) {
        try {
            double w = Double.parseDouble(watermark);
            if (w < 0 || w > 1.0) {
                return false;
            }
            return true;
        } catch (NumberFormatException e) {
            try {
                ByteSizeValue.parseBytesSizeValue(watermark);
                return true;
            } catch (ElasticSearchParseException ex) {
                return false;
            }
        }
    }
}
