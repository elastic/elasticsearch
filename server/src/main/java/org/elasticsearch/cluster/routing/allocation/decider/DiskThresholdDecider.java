/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting.Role;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Map;
import java.util.Set;

/**
 * The {@link DiskThresholdDecider} checks that the node a shard is potentially
 * being allocated to has enough disk space.
 *
 * It has the following configurable settings, all of which can be changed dynamically:
 *
 * <code>cluster.routing.allocation.disk.watermark.low</code> is the low disk
 * watermark. New shards will not allocated to a node with usage higher than this,
 * although this watermark may be passed by allocating a shard. It defaults to
 * 0.85 (85.0%).
 *
 * <code>cluster.routing.allocation.disk.watermark.low.max_headroom</code> is the
 * max headroom for the low watermark. Defaults to 200GB when the low watermark
 * is not explicitly set. This caps the amount of free space required.
 *
 * <code>cluster.routing.allocation.disk.watermark.high</code> is the high disk
 * watermark. If a node has usage higher than this, shards are not allowed to
 * remain on the node. In addition, if allocating a shard to a node causes the
 * node to pass this watermark, it will not be allowed. It defaults to
 * 0.90 (90.0%).
 *
 * <code>cluster.routing.allocation.disk.watermark.high.max_headroom</code> is the
 * max headroom for the high watermark. Defaults to 150GB when the high watermark
 * is not explicitly set. This caps the amount of free space required.
 *
 * The watermark settings are expressed in terms of used disk percentage/ratio, or
 * exact byte values for free space (like "500mb").
 *
 * <code>cluster.routing.allocation.disk.threshold_enabled</code> is used to
 * enable or disable this decider. It defaults to true (enabled).
 */
public class DiskThresholdDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(DiskThresholdDecider.class);

    public static final String NAME = "disk_threshold";

    public static final Setting<Boolean> ENABLE_FOR_SINGLE_DATA_NODE = Setting.boolSetting(
        "cluster.routing.allocation.disk.watermark.enable_for_single_data_node",
        true,
        new Setting.Validator<>() {
            @Override
            public void validate(Boolean value) {
                if (value == Boolean.FALSE) {
                    throw new SettingsException(
                        "setting [{}=false] is not allowed, only true is valid",
                        ENABLE_FOR_SINGLE_DATA_NODE.getKey()
                    );
                }
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.DeprecatedWarning
    );

    public static final Setting<Boolean> SETTING_IGNORE_DISK_WATERMARKS = Setting.boolSetting(
        "index.routing.allocation.disk.watermark.ignore",
        false,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );

    private final DiskThresholdSettings diskThresholdSettings;

    public DiskThresholdDecider(Settings settings, ClusterSettings clusterSettings) {
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        assert Version.CURRENT.major < 9 : "remove enable_for_single_data_node in 9";
        // get deprecation warnings.
        boolean enabledForSingleDataNode = ENABLE_FOR_SINGLE_DATA_NODE.get(settings);
        assert enabledForSingleDataNode;
    }

    /**
     * Returns the size of all unaccounted shards that are currently being relocated to
     * the node, but may not be finished transferring yet. Also accounts for started searchable
     * snapshot shards that have been allocated, but not present in the stale cluster info.
     *
     * If subtractShardsMovingAway is true then the size of shards moving away is subtracted from the total size of all shards
     */
    public static long sizeOfUnaccountedShards(
        RoutingNode node,
        boolean subtractShardsMovingAway,
        String dataPath,
        ClusterInfo clusterInfo,
        Metadata metadata,
        RoutingTable routingTable,
        long sizeOfUnaccountableSearchableSnapshotShards
    ) {
        // Account for reserved space wherever it is available
        final ClusterInfo.ReservedSpace reservedSpace = clusterInfo.getReservedSpace(node.nodeId(), dataPath);
        long totalSize = reservedSpace.getTotal();
        // NB this counts all shards on the node when the ClusterInfoService retrieved the node stats, which may include shards that are
        // no longer initializing because their recovery failed or was cancelled.

        // Where reserved space is unavailable (e.g. stats are out-of-sync) compute a conservative estimate for initialising shards
        for (ShardRouting routing : node.initializing()) {
            if (routing.relocatingNodeId() == null && metadata.getIndexSafe(routing.index()).isSearchableSnapshot() == false) {
                // in practice the only initializing-but-not-relocating non-searchable-snapshot shards with a nonzero expected shard size
                // will be ones created
                // by a resize (shrink/split/clone) operation which we expect to happen using hard links, so they shouldn't be taking
                // any additional space and can be ignored here
                continue;
            }
            if (reservedSpace.containsShardId(routing.shardId())) {
                continue;
            }
            final String actualPath = clusterInfo.getDataPath(routing);
            // if we don't yet know the actual path of the incoming shard then conservatively assume it's going to the path with the least
            // free space
            if (actualPath == null || actualPath.equals(dataPath)) {
                totalSize += getExpectedShardSize(
                    routing,
                    Math.max(routing.getExpectedShardSize(), 0L),
                    clusterInfo,
                    null,
                    metadata,
                    routingTable
                );
            }
        }

        totalSize += sizeOfUnaccountableSearchableSnapshotShards;

        if (subtractShardsMovingAway) {
            for (ShardRouting routing : node.relocating()) {
                if (dataPath.equals(clusterInfo.getDataPath(routing))) {
                    totalSize -= getExpectedShardSize(routing, 0L, clusterInfo, null, metadata, routingTable);
                }
            }
        }

        return totalSize;
    }

    private static final Decision YES_UNALLOCATED_PRIMARY_BETWEEN_WATERMARKS = Decision.single(
        Decision.Type.YES,
        NAME,
        "the node " + "is above the low watermark, but less than the high watermark, and this primary shard has never been allocated before"
    );

    private static final Decision YES_DISK_WATERMARKS_IGNORED = Decision.single(
        Decision.Type.YES,
        NAME,
        "disk watermarks are ignored on this index"
    );

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        Map<String, DiskUsage> usages = allocation.clusterInfo().getNodeMostAvailableDiskUsages();
        final Decision decision = earlyTerminate(usages);
        if (decision != null) {
            return decision;
        }

        if (ignoreDiskWatermarksForShard(allocation, shardRouting)) {
            return YES_DISK_WATERMARKS_IGNORED;
        }

        // subtractLeavingShards is passed as false here, because they still use disk space, and therefore we should be extra careful
        // and take the size into account
        final DiskUsageWithRelocations usage = getDiskUsage(node, allocation, usages, false);
        // Cache the used disk percentage for displaying disk percentages consistent with documentation
        double usedDiskPercentage = usage.getUsedDiskAsPercentage();
        long freeBytes = usage.getFreeBytes();
        final ByteSizeValue total = ByteSizeValue.ofBytes(usage.getTotalBytes());
        if (freeBytes < 0L) {
            final long sizeOfRelocatingShards = sizeOfUnaccountedShards(
                node,
                false,
                usage.getPath(),
                allocation.clusterInfo(),
                allocation.metadata(),
                allocation.routingTable(),
                allocation.unaccountedSearchableSnapshotSize(node)
            );
            logger.debug(
                "fewer free bytes remaining than the size of all incoming shards: "
                    + "usage {} on node {} including {} bytes of relocations, preventing allocation",
                usage,
                node.nodeId(),
                sizeOfRelocatingShards
            );

            return allocation.decision(
                Decision.NO,
                NAME,
                "the node has fewer free bytes remaining than the total size of all incoming shards: "
                    + "free space [%sB], relocating shards [%sB]",
                freeBytes + sizeOfRelocatingShards,
                sizeOfRelocatingShards
            );
        }

        ByteSizeValue freeBytesValue = ByteSizeValue.ofBytes(freeBytes);
        if (logger.isTraceEnabled()) {
            logger.trace("node [{}] has {}% used disk", node.nodeId(), usedDiskPercentage);
        }

        // flag that determines whether the low threshold checks below can be skipped. We use this for a primary shard that is freshly
        // allocated and empty.
        boolean skipLowThresholdChecks = shardRouting.primary()
            && shardRouting.active() == false
            && shardRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE;

        if (freeBytes < diskThresholdSettings.getFreeBytesThresholdLowStage(total).getBytes()) {
            if (skipLowThresholdChecks == false) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "less than the required {} free bytes threshold ({} free) on node {}, preventing allocation",
                        diskThresholdSettings.getFreeBytesThresholdLowStage(total).getBytes(),
                        freeBytesValue,
                        node.nodeId()
                    );
                }
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "the node is above the low watermark cluster setting [%s], having less than the minimum required [%s] free "
                        + "space, actual free: [%s], actual used: [%s]",
                    diskThresholdSettings.describeLowThreshold(total, true),
                    diskThresholdSettings.getFreeBytesThresholdLowStage(total),
                    freeBytesValue,
                    Strings.format1Decimals(usedDiskPercentage, "%")
                );
            } else if (freeBytes > diskThresholdSettings.getFreeBytesThresholdHighStage(total).getBytes()) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "less than the required {} free bytes threshold ({} free) on node {}, "
                            + "but allowing allocation because primary has never been allocated",
                        diskThresholdSettings.getFreeBytesThresholdLowStage(total),
                        freeBytesValue,
                        node.nodeId()
                    );
                }
                return YES_UNALLOCATED_PRIMARY_BETWEEN_WATERMARKS;
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "less than the required {} free bytes threshold ({} free) on node {}, "
                            + "preventing allocation even though primary has never been allocated",
                        diskThresholdSettings.getFreeBytesThresholdHighStage(total).getBytes(),
                        freeBytesValue,
                        node.nodeId()
                    );
                }
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "the node is above the high watermark cluster setting [%s], having less than the minimum required [%s] free "
                        + "space, actual free: [%s], actual used: [%s]",
                    diskThresholdSettings.describeHighThreshold(total, true),
                    diskThresholdSettings.getFreeBytesThresholdHighStage(total),
                    freeBytesValue,
                    Strings.format1Decimals(usedDiskPercentage, "%")
                );
            }
        }

        // Secondly, check that allocating the shard to this node doesn't put it above the high watermark
        final long shardSize = getExpectedShardSize(shardRouting, 0L, allocation);
        assert shardSize >= 0 : shardSize;
        long freeBytesAfterShard = freeBytes - shardSize;
        if (freeBytesAfterShard < diskThresholdSettings.getFreeBytesThresholdHighStage(total).getBytes()) {
            logger.warn(
                "after allocating [{}] node [{}] would be above the high watermark setting [{}], having less than the minimum "
                    + "required {} of free space (actual free: {}, actual used: {}, estimated shard size: {}), preventing allocation",
                shardRouting,
                node.nodeId(),
                diskThresholdSettings.describeHighThreshold(total, false),
                diskThresholdSettings.getFreeBytesThresholdHighStage(total),
                freeBytesValue,
                Strings.format1Decimals(usedDiskPercentage, "%"),
                ByteSizeValue.ofBytes(shardSize)
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "allocating the shard to this node will bring the node above the high watermark cluster setting [%s] "
                    + "and cause it to have less than the minimum required [%s] of free space (free: [%s], used: [%s], estimated "
                    + "shard size: [%s])",
                diskThresholdSettings.describeHighThreshold(total, true),
                diskThresholdSettings.getFreeBytesThresholdHighStage(total),
                freeBytesValue,
                Strings.format1Decimals(usedDiskPercentage, "%"),
                ByteSizeValue.ofBytes(shardSize)
            );
        }

        assert freeBytesAfterShard >= 0 : freeBytesAfterShard;
        return allocation.decision(
            Decision.YES,
            NAME,
            "enough disk for shard on node, free: [%s], used: [%s], shard size: [%s], free after allocating shard: [%s]",
            freeBytesValue,
            Strings.format1Decimals(usedDiskPercentage, "%"),
            ByteSizeValue.ofBytes(shardSize),
            ByteSizeValue.ofBytes(freeBytesAfterShard)
        );
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        Map<String, DiskUsage> usages = allocation.clusterInfo().getNodeMostAvailableDiskUsages();
        final Decision decision = earlyTerminate(usages);
        if (decision != null) {
            return decision;
        }

        if (ignoreDiskWatermarksForShard(allocation, shardRouting)) {
            return YES_DISK_WATERMARKS_IGNORED;
        }

        final DiskUsageWithRelocations usage = getDiskUsage(node, allocation, usages, false);
        final long shardSize = getExpectedShardSize(shardRouting, 0L, allocation);
        assert shardSize >= 0 : shardSize;
        final long freeBytesAfterShard = usage.getFreeBytes() - shardSize;
        if (freeBytesAfterShard < 0) {
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "unable to force allocate shard to [%s] during replacement, "
                    + "as allocating to this node would cause disk usage to exceed 100%% ([%s] bytes above available disk space)",
                node.nodeId(),
                -freeBytesAfterShard
            );
        } else {
            return super.canForceAllocateDuringReplace(shardRouting, node, allocation);
        }
    }

    private static final Decision YES_NOT_MOST_UTILIZED_DISK = Decision.single(
        Decision.Type.YES,
        NAME,
        "this shard is not allocated on the most utilized disk and can remain"
    );

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.currentNodeId().equals(node.nodeId()) == false) {
            throw new IllegalArgumentException("Shard [" + shardRouting + "] is not allocated on node: [" + node.nodeId() + "]");
        }
        final ClusterInfo clusterInfo = allocation.clusterInfo();
        final Map<String, DiskUsage> usages = clusterInfo.getNodeLeastAvailableDiskUsages();
        final Decision decision = earlyTerminate(usages);
        if (decision != null) {
            return decision;
        }

        if (indexMetadata.ignoreDiskWatermarks() || ignoreDiskWatermarksForShard(allocation, shardRouting)) {
            return YES_DISK_WATERMARKS_IGNORED;
        }

        // subtractLeavingShards is passed as true here, since this is only for shards remaining, we will *eventually* have enough disk
        // since shards are moving away. No new shards will be incoming since in canAllocate we pass false for this check.
        final DiskUsageWithRelocations usage = getDiskUsage(node, allocation, usages, true);
        final String dataPath = clusterInfo.getDataPath(shardRouting);
        // If this node is already above the high threshold, the shard cannot remain (get it off!)
        final double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        final long freeBytes = usage.getFreeBytes();
        double usedDiskPercentage = usage.getUsedDiskAsPercentage();
        final ByteSizeValue total = ByteSizeValue.ofBytes(usage.getTotalBytes());
        if (logger.isTraceEnabled()) {
            logger.trace("node [{}] has {}% free disk ({} bytes)", node.nodeId(), freeDiskPercentage, freeBytes);
        }
        if (dataPath == null || usage.getPath().equals(dataPath) == false) {
            return YES_NOT_MOST_UTILIZED_DISK;
        }
        if (freeBytes < 0L) {
            final long sizeOfRelocatingShards = sizeOfUnaccountedShards(
                node,
                true,
                usage.getPath(),
                allocation.clusterInfo(),
                allocation.metadata(),
                allocation.routingTable(),
                allocation.unaccountedSearchableSnapshotSize(node)
            );
            logger.debug(
                "fewer free bytes remaining than the size of all incoming shards: "
                    + "usage {} on node {} including {} bytes of relocations, shard cannot remain",
                usage,
                node.nodeId(),
                sizeOfRelocatingShards
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "the shard cannot remain on this node because the node has fewer free bytes remaining than the total size of all "
                    + "incoming shards: free space [%s], relocating shards [%s]",
                freeBytes + sizeOfRelocatingShards,
                sizeOfRelocatingShards
            );
        }
        if (freeBytes < diskThresholdSettings.getFreeBytesThresholdHighStage(total).getBytes()) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "node {} is over the high watermark setting [{}], having less than the required {} free space "
                        + "(actual free: {}, actual used: {}), shard cannot remain",
                    node.nodeId(),
                    diskThresholdSettings.describeHighThreshold(total, false),
                    diskThresholdSettings.getFreeBytesThresholdHighStage(total),
                    freeBytes,
                    Strings.format1Decimals(usedDiskPercentage, "%")
                );
            }
            return allocation.decision(
                Decision.NO,
                NAME,
                "the shard cannot remain on this node because it is above the high watermark cluster setting [%s] "
                    + "and there is less than the required [%s] free space on node, actual free: [%s], actual used: [%s]",
                diskThresholdSettings.describeHighThreshold(total, true),
                diskThresholdSettings.getFreeBytesThresholdHighStage(total),
                ByteSizeValue.ofBytes(freeBytes),
                Strings.format1Decimals(usedDiskPercentage, "%")
            );
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "there is enough disk on this node for the shard to remain, free: [%s]",
            ByteSizeValue.ofBytes(freeBytes)
        );
    }

    private static boolean ignoreDiskWatermarksForShard(RoutingAllocation allocation, ShardRouting shardRouting) {
        return shardRouting.role() == Role.INDEX_ONLY || allocation.metadata().index(shardRouting.index()).ignoreDiskWatermarks();
    }

    private static DiskUsageWithRelocations getDiskUsage(
        RoutingNode node,
        RoutingAllocation allocation,
        Map<String, DiskUsage> usages,
        boolean subtractLeavingShards
    ) {
        DiskUsage usage = usages.get(node.nodeId());
        if (usage == null) {
            // If there is no usage, and we have other nodes in the cluster,
            // use the average usage for all nodes as the usage for this node
            usage = averageUsage(node, usages);
            logger.debug(
                "unable to determine disk usage for {}, defaulting to average across nodes [{} total] [{} free] [{}% free]",
                node.nodeId(),
                usage.getTotalBytes(),
                usage.getFreeBytes(),
                usage.getFreeDiskAsPercentage()
            );
        }

        final DiskUsageWithRelocations diskUsageWithRelocations = new DiskUsageWithRelocations(
            usage,
            sizeOfUnaccountedShards(
                node,
                subtractLeavingShards,
                usage.getPath(),
                allocation.clusterInfo(),
                allocation.metadata(),
                allocation.routingTable(),
                allocation.unaccountedSearchableSnapshotSize(node)
            )
        );
        logger.trace("getDiskUsage(subtractLeavingShards={}) returning {}", subtractLeavingShards, diskUsageWithRelocations);
        return diskUsageWithRelocations;
    }

    /**
     * Returns a {@link DiskUsage} for the {@link RoutingNode} using the
     * average usage of other nodes in the disk usage map.
     * @param node Node to return an averaged DiskUsage object for
     * @param usages Map of nodeId to DiskUsage for all known nodes
     * @return DiskUsage representing given node using the average disk usage
     */
    static DiskUsage averageUsage(RoutingNode node, Map<String, DiskUsage> usages) {
        if (usages.size() == 0) {
            return new DiskUsage(node.nodeId(), node.node().getName(), "_na_", 0, 0);
        }
        long totalBytes = 0;
        long freeBytes = 0;
        for (DiskUsage du : usages.values()) {
            totalBytes += du.getTotalBytes();
            freeBytes += du.getFreeBytes();
        }
        return new DiskUsage(node.nodeId(), node.node().getName(), "_na_", totalBytes / usages.size(), freeBytes / usages.size());
    }

    private static final Decision YES_DISABLED = Decision.single(Decision.Type.YES, NAME, "the disk threshold decider is disabled");

    private static final Decision YES_USAGES_UNAVAILABLE = Decision.single(Decision.Type.YES, NAME, "disk usages are unavailable");

    private Decision earlyTerminate(Map<String, DiskUsage> usages) {
        // Always allow allocation if the decider is disabled
        if (diskThresholdSettings.isEnabled() == false) {
            return YES_DISABLED;
        }

        // Fail open if there are no disk usages available
        if (usages.isEmpty()) {
            logger.trace("unable to determine disk usages for disk-aware allocation, allowing allocation");
            return YES_USAGES_UNAVAILABLE;
        }
        return null;
    }

    public static long getExpectedShardSize(ShardRouting shardRouting, long defaultSize, RoutingAllocation allocation) {
        return DiskThresholdDecider.getExpectedShardSize(
            shardRouting,
            defaultSize,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );
    }

    /**
     * Returns the expected shard size for the given shard or the default value provided if not enough information are available
     * to estimate the shards size.
     */
    public static long getExpectedShardSize(
        ShardRouting shard,
        long defaultValue,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo snapshotShardSizeInfo,
        Metadata metadata,
        RoutingTable routingTable
    ) {
        final IndexMetadata indexMetadata = metadata.getIndexSafe(shard.index());
        if (indexMetadata.getResizeSourceIndex() != null
            && shard.active() == false
            && shard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            // in the shrink index case we sum up the source index shards since we basically make a copy of the shard in
            // the worst case
            long targetShardSize = 0;
            final Index mergeSourceIndex = indexMetadata.getResizeSourceIndex();
            final IndexMetadata sourceIndexMeta = metadata.index(mergeSourceIndex);
            if (sourceIndexMeta != null) {
                final Set<ShardId> shardIds = IndexMetadata.selectRecoverFromShards(
                    shard.id(),
                    sourceIndexMeta,
                    indexMetadata.getNumberOfShards()
                );
                final IndexRoutingTable indexRoutingTable = routingTable.index(mergeSourceIndex.getName());
                for (int i = 0; i < indexRoutingTable.size(); i++) {
                    IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
                    if (shardIds.contains(shardRoutingTable.shardId())) {
                        targetShardSize += clusterInfo.getShardSize(shardRoutingTable.primaryShard(), 0);
                    }
                }
            }
            return targetShardSize == 0 ? defaultValue : targetShardSize;
        } else {
            if (shard.unassigned() && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                return snapshotShardSizeInfo.getShardSize(shard, defaultValue);
            }
            return clusterInfo.getShardSize(shard, defaultValue);
        }
    }

    record DiskUsageWithRelocations(DiskUsage diskUsage, long relocatingShardSize) {

        double getFreeDiskAsPercentage() {
            if (getTotalBytes() == 0L) {
                return 100.0;
            }
            return 100.0 * getFreeBytes() / getTotalBytes();
        }

        double getUsedDiskAsPercentage() {
            return 100.0 - getFreeDiskAsPercentage();
        }

        long getFreeBytes() {
            try {
                return Math.subtractExact(diskUsage.getFreeBytes(), relocatingShardSize);
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
        }

        String getPath() {
            return diskUsage.getPath();
        }

        long getTotalBytes() {
            return diskUsage.getTotalBytes();
        }
    }

}
