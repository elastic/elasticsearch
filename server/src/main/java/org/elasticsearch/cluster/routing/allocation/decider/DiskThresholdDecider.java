/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.Set;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;

/**
 * The {@link DiskThresholdDecider} checks that the node a shard is potentially
 * being allocated to has enough disk space.
 *
 * It has three configurable settings, all of which can be changed dynamically:
 *
 * <code>cluster.routing.allocation.disk.watermark.low</code> is the low disk
 * watermark. New shards will not allocated to a node with usage higher than this,
 * although this watermark may be passed by allocating a shard. It defaults to
 * 0.85 (85.0%).
 *
 * <code>cluster.routing.allocation.disk.watermark.high</code> is the high disk
 * watermark. If a node has usage higher than this, shards are not allowed to
 * remain on the node. In addition, if allocating a shard to a node causes the
 * node to pass this watermark, it will not be allowed. It defaults to
 * 0.90 (90.0%).
 *
 * Both watermark settings are expressed in terms of used disk percentage, or
 * exact byte values for free space (like "500mb")
 *
 * <code>cluster.routing.allocation.disk.threshold_enabled</code> is used to
 * enable or disable this decider. It defaults to false (disabled).
 */
public class DiskThresholdDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(DiskThresholdDecider.class);

    public static final String NAME = "disk_threshold";

    private final DiskThresholdSettings diskThresholdSettings;

    public DiskThresholdDecider(Settings settings, ClusterSettings clusterSettings) {
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
    }

    /**
     * Returns the size of all shards that are currently being relocated to
     * the node, but may not be finished transferring yet.
     *
     * If subtractShardsMovingAway is true then the size of shards moving away is subtracted from the total size of all shards
     */
    static long sizeOfRelocatingShards(RoutingNode node, RoutingAllocation allocation,
                                       boolean subtractShardsMovingAway, String dataPath) {
        ClusterInfo clusterInfo = allocation.clusterInfo();
        long totalSize = 0;

        for (ShardRouting routing : node.shardsWithState(ShardRoutingState.INITIALIZING)) {
            if (routing.relocatingNodeId() == null) {
                // in practice the only initializing-but-not-relocating shards with a nonzero expected shard size will be ones created
                // by a resize (shrink/split/clone) operation which we expect to happen using hard links, so they shouldn't be taking
                // any additional space and can be ignored here
                continue;
            }

            final String actualPath = clusterInfo.getDataPath(routing);
            // if we don't yet know the actual path of the incoming shard then conservatively assume it's going to the path with the least
            // free space
            if (actualPath == null || actualPath.equals(dataPath)) {
                totalSize += getExpectedShardSize(routing, allocation, 0);
            }
        }

        if (subtractShardsMovingAway) {
            for (ShardRouting routing : node.shardsWithState(ShardRoutingState.RELOCATING)) {
                String actualPath = clusterInfo.getDataPath(routing);
                if (actualPath == null) {
                    // we might know the path of this shard from before when it was relocating
                    actualPath = clusterInfo.getDataPath(routing.cancelRelocation());
                }
                if (dataPath.equals(actualPath)) {
                    totalSize -= getExpectedShardSize(routing, allocation, 0);
                }
            }
        }

        return totalSize;
    }


    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        ClusterInfo clusterInfo = allocation.clusterInfo();
        ImmutableOpenMap<String, DiskUsage> usages = clusterInfo.getNodeMostAvailableDiskUsages();
        final Decision decision = earlyTerminate(allocation, usages);
        if (decision != null) {
            return decision;
        }

        final double usedDiskThresholdLow = 100.0 - diskThresholdSettings.getFreeDiskThresholdLow();
        final double usedDiskThresholdHigh = 100.0 - diskThresholdSettings.getFreeDiskThresholdHigh();

        // subtractLeavingShards is passed as false here, because they still use disk space, and therefore should we should be extra careful
        // and take the size into account
        DiskUsage usage = getDiskUsage(node, allocation, usages, false);
        // First, check that the node currently over the low watermark
        double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        // Cache the used disk percentage for displaying disk percentages consistent with documentation
        double usedDiskPercentage = usage.getUsedDiskAsPercentage();
        long freeBytes = usage.getFreeBytes();
        ByteSizeValue freeBytesValue = new ByteSizeValue(freeBytes);
        if (logger.isTraceEnabled()) {
            logger.trace("node [{}] has {}% used disk", node.nodeId(), usedDiskPercentage);
        }

        // flag that determines whether the low threshold checks below can be skipped. We use this for a primary shard that is freshly
        // allocated and empty.
        boolean skipLowThresholdChecks = shardRouting.primary() &&
            shardRouting.active() == false && shardRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE;

        // checks for exact byte comparisons
        if (freeBytes < diskThresholdSettings.getFreeBytesThresholdLow().getBytes()) {
            if (skipLowThresholdChecks == false) {
                if (logger.isDebugEnabled()) {
                    logger.debug("less than the required {} free bytes threshold ({} free) on node {}, preventing allocation",
                            diskThresholdSettings.getFreeBytesThresholdLow(), freeBytesValue, node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                    "the node is above the low watermark cluster setting [%s=%s], having less than the minimum required [%s] free " +
                    "space, actual free: [%s]",
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getLowWatermarkRaw(),
                    diskThresholdSettings.getFreeBytesThresholdLow(), freeBytesValue);
            } else if (freeBytes > diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (logger.isDebugEnabled()) {
                    logger.debug("less than the required {} free bytes threshold ({} free) on node {}, " +
                                    "but allowing allocation because primary has never been allocated",
                            diskThresholdSettings.getFreeBytesThresholdLow(), freeBytesValue, node.nodeId());
                }
                return allocation.decision(Decision.YES, NAME,
                        "the node is above the low watermark, but less than the high watermark, and this primary shard has " +
                        "never been allocated before");
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (logger.isDebugEnabled()) {
                    logger.debug("less than the required {} free bytes threshold ({} free) on node {}, " +
                                    "preventing allocation even though primary has never been allocated",
                            diskThresholdSettings.getFreeBytesThresholdHigh(), freeBytesValue, node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                    "the node is above the high watermark cluster setting [%s=%s], having less than the minimum required [%s] free " +
                    "space, actual free: [%s]",
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getHighWatermarkRaw(),
                    diskThresholdSettings.getFreeBytesThresholdHigh(), freeBytesValue);
            }
        }

        // checks for percentage comparisons
        if (freeDiskPercentage < diskThresholdSettings.getFreeDiskThresholdLow()) {
            // If the shard is a replica or is a non-empty primary, check the low threshold
            if (skipLowThresholdChecks == false) {
                if (logger.isDebugEnabled()) {
                    logger.debug("more than the allowed {} used disk threshold ({} used) on node [{}], preventing allocation",
                            Strings.format1Decimals(usedDiskThresholdLow, "%"),
                            Strings.format1Decimals(usedDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                    "the node is above the low watermark cluster setting [%s=%s], using more disk space than the maximum allowed " +
                    "[%s%%], actual free: [%s%%]",
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getLowWatermarkRaw(), usedDiskThresholdLow, freeDiskPercentage);
            } else if (freeDiskPercentage > diskThresholdSettings.getFreeDiskThresholdHigh()) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (logger.isDebugEnabled()) {
                    logger.debug("more than the allowed {} used disk threshold ({} used) on node [{}], " +
                                    "but allowing allocation because primary has never been allocated",
                            Strings.format1Decimals(usedDiskThresholdLow, "%"),
                            Strings.format1Decimals(usedDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.YES, NAME,
                    "the node is above the low watermark, but less than the high watermark, and this primary shard has " +
                    "never been allocated before");
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (logger.isDebugEnabled()) {
                    logger.debug("less than the required {} free bytes threshold ({} bytes free) on node {}, " +
                                    "preventing allocation even though primary has never been allocated",
                            Strings.format1Decimals(diskThresholdSettings.getFreeDiskThresholdHigh(), "%"),
                            Strings.format1Decimals(freeDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                    "the node is above the high watermark cluster setting [%s=%s], using more disk space than the maximum allowed " +
                    "[%s%%], actual free: [%s%%]",
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getHighWatermarkRaw(), usedDiskThresholdHigh, freeDiskPercentage);
            }
        }

        // Secondly, check that allocating the shard to this node doesn't put it above the high watermark
        final long shardSize = getExpectedShardSize(shardRouting, allocation, 0);
        double freeSpaceAfterShard = freeDiskPercentageAfterShardAssigned(usage, shardSize);
        long freeBytesAfterShard = freeBytes - shardSize;
        if (freeBytesAfterShard < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
            logger.warn("after allocating, node [{}] would have less than the required threshold of " +
                    "{} free (currently {} free, estimated shard size is {}), preventing allocation",
                    node.nodeId(), diskThresholdSettings.getFreeBytesThresholdHigh(), freeBytesValue, new ByteSizeValue(shardSize));
            return allocation.decision(Decision.NO, NAME,
                "allocating the shard to this node will bring the node above the high watermark cluster setting [%s=%s] " +
                    "and cause it to have less than the minimum required [%s] of free space (free: [%s], estimated shard size: [%s])",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                diskThresholdSettings.getFreeBytesThresholdHigh(),
                freeBytesValue, new ByteSizeValue(shardSize));
        }
        if (freeSpaceAfterShard < diskThresholdSettings.getFreeDiskThresholdHigh()) {
            logger.warn("after allocating, node [{}] would have more than the allowed " +
                            "{} free disk threshold ({} free), preventing allocation",
                    node.nodeId(), Strings.format1Decimals(diskThresholdSettings.getFreeDiskThresholdHigh(), "%"),
                                                           Strings.format1Decimals(freeSpaceAfterShard, "%"));
            return allocation.decision(Decision.NO, NAME,
                "allocating the shard to this node will bring the node above the high watermark cluster setting [%s=%s] " +
                    "and cause it to use more disk space than the maximum allowed [%s%%] (free space after shard added: [%s%%])",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(), usedDiskThresholdHigh, freeSpaceAfterShard);
        }

        return allocation.decision(Decision.YES, NAME,
                "enough disk for shard on node, free: [%s], shard size: [%s], free after allocating shard: [%s]",
                freeBytesValue,
                new ByteSizeValue(shardSize),
                new ByteSizeValue(freeBytesAfterShard));
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.currentNodeId().equals(node.nodeId()) == false) {
            throw new IllegalArgumentException("Shard [" + shardRouting + "] is not allocated on node: [" + node.nodeId() + "]");
        }
        final ClusterInfo clusterInfo = allocation.clusterInfo();
        final ImmutableOpenMap<String, DiskUsage> usages = clusterInfo.getNodeLeastAvailableDiskUsages();
        final Decision decision = earlyTerminate(allocation, usages);
        if (decision != null) {
            return decision;
        }

        // subtractLeavingShards is passed as true here, since this is only for shards remaining, we will *eventually* have enough disk
        // since shards are moving away. No new shards will be incoming since in canAllocate we pass false for this check.
        final DiskUsage usage = getDiskUsage(node, allocation, usages, true);
        final String dataPath = clusterInfo.getDataPath(shardRouting);
        // If this node is already above the high threshold, the shard cannot remain (get it off!)
        final double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        final long freeBytes = usage.getFreeBytes();
        if (logger.isTraceEnabled()) {
            logger.trace("node [{}] has {}% free disk ({} bytes)", node.nodeId(), freeDiskPercentage, freeBytes);
        }
        if (dataPath == null || usage.getPath().equals(dataPath) == false) {
            return allocation.decision(Decision.YES, NAME,
                    "this shard is not allocated on the most utilized disk and can remain");
        }
        if (freeBytes < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
            if (logger.isDebugEnabled()) {
                logger.debug("less than the required {} free bytes threshold ({} bytes free) on node {}, shard cannot remain",
                        diskThresholdSettings.getFreeBytesThresholdHigh(), freeBytes, node.nodeId());
            }
            return allocation.decision(Decision.NO, NAME,
                "the shard cannot remain on this node because it is above the high watermark cluster setting [%s=%s] " +
                    "and there is less than the required [%s] free space on node, actual free: [%s]",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                diskThresholdSettings.getFreeBytesThresholdHigh(), new ByteSizeValue(freeBytes));
        }
        if (freeDiskPercentage < diskThresholdSettings.getFreeDiskThresholdHigh()) {
            if (logger.isDebugEnabled()) {
                logger.debug("less than the required {}% free disk threshold ({}% free) on node {}, shard cannot remain",
                        diskThresholdSettings.getFreeDiskThresholdHigh(), freeDiskPercentage, node.nodeId());
            }
            return allocation.decision(Decision.NO, NAME,
                "the shard cannot remain on this node because it is above the high watermark cluster setting [%s=%s] " +
                    "and there is less than the required [%s%%] free disk on node, actual free: [%s%%]",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                diskThresholdSettings.getFreeDiskThresholdHigh(), freeDiskPercentage);
        }

        return allocation.decision(Decision.YES, NAME,
                "there is enough disk on this node for the shard to remain, free: [%s]", new ByteSizeValue(freeBytes));
    }

    private DiskUsage getDiskUsage(RoutingNode node, RoutingAllocation allocation,
                                   ImmutableOpenMap<String, DiskUsage> usages, boolean subtractLeavingShards) {
        DiskUsage usage = usages.get(node.nodeId());
        if (usage == null) {
            // If there is no usage, and we have other nodes in the cluster,
            // use the average usage for all nodes as the usage for this node
            usage = averageUsage(node, usages);
            if (logger.isDebugEnabled()) {
                logger.debug("unable to determine disk usage for {}, defaulting to average across nodes [{} total] [{} free] [{}% free]",
                        node.nodeId(), usage.getTotalBytes(), usage.getFreeBytes(), usage.getFreeDiskAsPercentage());
            }
        }

        if (diskThresholdSettings.includeRelocations()) {
            long relocatingShardsSize = sizeOfRelocatingShards(node, allocation, subtractLeavingShards, usage.getPath());
            DiskUsage usageIncludingRelocations = new DiskUsage(node.nodeId(), node.node().getName(), usage.getPath(),
                    usage.getTotalBytes(), usage.getFreeBytes() - relocatingShardsSize);
            if (logger.isTraceEnabled()) {
                logger.trace("usage without relocations: {}", usage);
                logger.trace("usage with relocations: [{} bytes] {}", relocatingShardsSize, usageIncludingRelocations);
            }
            usage = usageIncludingRelocations;
        }
        return usage;
    }

    /**
     * Returns a {@link DiskUsage} for the {@link RoutingNode} using the
     * average usage of other nodes in the disk usage map.
     * @param node Node to return an averaged DiskUsage object for
     * @param usages Map of nodeId to DiskUsage for all known nodes
     * @return DiskUsage representing given node using the average disk usage
     */
    DiskUsage averageUsage(RoutingNode node, ImmutableOpenMap<String, DiskUsage> usages) {
        if (usages.size() == 0) {
            return new DiskUsage(node.nodeId(), node.node().getName(), "_na_", 0, 0);
        }
        long totalBytes = 0;
        long freeBytes = 0;
        for (ObjectCursor<DiskUsage> du : usages.values()) {
            totalBytes += du.value.getTotalBytes();
            freeBytes += du.value.getFreeBytes();
        }
        return new DiskUsage(node.nodeId(), node.node().getName(), "_na_", totalBytes / usages.size(), freeBytes / usages.size());
    }

    /**
     * Given the DiskUsage for a node and the size of the shard, return the
     * percentage of free disk if the shard were to be allocated to the node.
     * @param usage A DiskUsage for the node to have space computed for
     * @param shardSize Size in bytes of the shard
     * @return Percentage of free space after the shard is assigned to the node
     */
    double freeDiskPercentageAfterShardAssigned(DiskUsage usage, Long shardSize) {
        shardSize = (shardSize == null) ? 0 : shardSize;
        DiskUsage newUsage = new DiskUsage(usage.getNodeId(), usage.getNodeName(), usage.getPath(),
                usage.getTotalBytes(),  usage.getFreeBytes() - shardSize);
        return newUsage.getFreeDiskAsPercentage();
    }

    private Decision earlyTerminate(RoutingAllocation allocation, ImmutableOpenMap<String, DiskUsage> usages) {
        // Always allow allocation if the decider is disabled
        if (diskThresholdSettings.isEnabled() == false) {
            return allocation.decision(Decision.YES, NAME, "the disk threshold decider is disabled");
        }

        // Allow allocation regardless if only a single data node is available
        if (allocation.nodes().getDataNodes().size() <= 1) {
            if (logger.isTraceEnabled()) {
                logger.trace("only a single data node is present, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "there is only a single data node present");
        }

        // Fail open there is no info available
        final ClusterInfo clusterInfo = allocation.clusterInfo();
        if (clusterInfo == null) {
            if (logger.isTraceEnabled()) {
                logger.trace("cluster info unavailable for disk threshold decider, allowing allocation.");
            }
            return allocation.decision(Decision.YES, NAME, "the cluster info is unavailable");
        }

        // Fail open if there are no disk usages available
        if (usages.isEmpty()) {
            if (logger.isTraceEnabled()) {
                logger.trace("unable to determine disk usages for disk-aware allocation, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "disk usages are unavailable");
        }
        return null;
    }

    /**
     * Returns the expected shard size for the given shard or the default value provided if not enough information are available
     * to estimate the shards size.
     */
    public static long getExpectedShardSize(ShardRouting shard, RoutingAllocation allocation, long defaultValue) {
        final IndexMetaData metaData = allocation.metaData().getIndexSafe(shard.index());
        final ClusterInfo info = allocation.clusterInfo();
        if (metaData.getResizeSourceIndex() != null && shard.active() == false &&
            shard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            // in the shrink index case we sum up the source index shards since we basically make a copy of the shard in
            // the worst case
            long targetShardSize = 0;
            final Index mergeSourceIndex = metaData.getResizeSourceIndex();
            final IndexMetaData sourceIndexMeta = allocation.metaData().index(mergeSourceIndex);
            if (sourceIndexMeta != null) {
                final Set<ShardId> shardIds = IndexMetaData.selectRecoverFromShards(shard.id(),
                    sourceIndexMeta, metaData.getNumberOfShards());
                for (IndexShardRoutingTable shardRoutingTable : allocation.routingTable().index(mergeSourceIndex.getName())) {
                    if (shardIds.contains(shardRoutingTable.shardId())) {
                        targetShardSize += info.getShardSize(shardRoutingTable.primaryShard(), 0);
                    }
                }
            }
            return targetShardSize == 0 ? defaultValue : targetShardSize;
        } else {
            return info.getShardSize(shard, defaultValue);
        }

    }
}
