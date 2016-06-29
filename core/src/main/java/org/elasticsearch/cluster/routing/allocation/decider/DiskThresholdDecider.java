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

import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.Set;

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

    public static final String NAME = "disk_threshold";

    private volatile Double freeDiskThresholdLow;
    private volatile Double freeDiskThresholdHigh;
    private volatile ByteSizeValue freeBytesThresholdLow;
    private volatile ByteSizeValue freeBytesThresholdHigh;
    private volatile boolean includeRelocations;
    private volatile boolean enabled;
    private volatile TimeValue rerouteInterval;

    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING =
        Setting.boolSetting("cluster.routing.allocation.disk.threshold_enabled", true, Property.Dynamic, Property.NodeScope);
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING =
        new Setting<>("cluster.routing.allocation.disk.watermark.low", "85%",
            (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.low"),
            Property.Dynamic, Property.NodeScope);
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING =
        new Setting<>("cluster.routing.allocation.disk.watermark.high", "90%",
            (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.high"),
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING =
        Setting.boolSetting("cluster.routing.allocation.disk.include_relocations", true,
            Property.Dynamic, Property.NodeScope);;
    public static final Setting<TimeValue> CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING =
        Setting.positiveTimeSetting("cluster.routing.allocation.disk.reroute_interval", TimeValue.timeValueSeconds(60),
            Property.Dynamic, Property.NodeScope);

    /**
     * Listens for a node to go over the high watermark and kicks off an empty
     * reroute if it does. Also responsible for logging about nodes that have
     * passed the disk watermarks
     */
    class DiskListener implements ClusterInfoService.Listener {
        private final Client client;
        private final Set<String> nodeHasPassedWatermark = Sets.newConcurrentHashSet();

        private long lastRunNS;

        DiskListener(Client client) {
            this.client = client;
        }

        /**
         * Warn about the given disk usage if the low or high watermark has been passed
         */
        private void warnAboutDiskIfNeeded(DiskUsage usage) {
            // Check absolute disk values
            if (usage.getFreeBytes() < DiskThresholdDecider.this.freeBytesThresholdHigh.bytes()) {
                logger.warn("high disk watermark [{}] exceeded on {}, shards will be relocated away from this node",
                        DiskThresholdDecider.this.freeBytesThresholdHigh, usage);
            } else if (usage.getFreeBytes() < DiskThresholdDecider.this.freeBytesThresholdLow.bytes()) {
                logger.info("low disk watermark [{}] exceeded on {}, replicas will not be assigned to this node",
                        DiskThresholdDecider.this.freeBytesThresholdLow, usage);
            }

            // Check percentage disk values
            if (usage.getFreeDiskAsPercentage() < DiskThresholdDecider.this.freeDiskThresholdHigh) {
                logger.warn("high disk watermark [{}] exceeded on {}, shards will be relocated away from this node",
                        Strings.format1Decimals(100.0 - DiskThresholdDecider.this.freeDiskThresholdHigh, "%"), usage);
            } else if (usage.getFreeDiskAsPercentage() < DiskThresholdDecider.this.freeDiskThresholdLow) {
                logger.info("low disk watermark [{}] exceeded on {}, replicas will not be assigned to this node",
                        Strings.format1Decimals(100.0 - DiskThresholdDecider.this.freeDiskThresholdLow, "%"), usage);
            }
        }

        @Override
        public void onNewInfo(ClusterInfo info) {
            ImmutableOpenMap<String, DiskUsage> usages = info.getNodeLeastAvailableDiskUsages();
            if (usages != null) {
                boolean reroute = false;
                String explanation = "";

                // Garbage collect nodes that have been removed from the cluster
                // from the map that tracks watermark crossing
                ObjectLookupContainer<String> nodes = usages.keys();
                for (String node : nodeHasPassedWatermark) {
                    if (nodes.contains(node) == false) {
                        nodeHasPassedWatermark.remove(node);
                    }
                }

                for (ObjectObjectCursor<String, DiskUsage> entry : usages) {
                    String node = entry.key;
                    DiskUsage usage = entry.value;
                    warnAboutDiskIfNeeded(usage);
                    if (usage.getFreeBytes() < DiskThresholdDecider.this.freeBytesThresholdHigh.bytes() ||
                            usage.getFreeDiskAsPercentage() < DiskThresholdDecider.this.freeDiskThresholdHigh) {
                        if ((System.nanoTime() - lastRunNS) > DiskThresholdDecider.this.rerouteInterval.nanos()) {
                            lastRunNS = System.nanoTime();
                            reroute = true;
                            explanation = "high disk watermark exceeded on one or more nodes";
                        } else {
                            logger.debug("high disk watermark exceeded on {} but an automatic reroute has occurred " +
                                            "in the last [{}], skipping reroute",
                                    node, DiskThresholdDecider.this.rerouteInterval);
                        }
                        nodeHasPassedWatermark.add(node);
                    } else if (usage.getFreeBytes() < DiskThresholdDecider.this.freeBytesThresholdLow.bytes() ||
                            usage.getFreeDiskAsPercentage() < DiskThresholdDecider.this.freeDiskThresholdLow) {
                        nodeHasPassedWatermark.add(node);
                    } else {
                        if (nodeHasPassedWatermark.contains(node)) {
                            // The node has previously been over the high or
                            // low watermark, but is no longer, so we should
                            // reroute so any unassigned shards can be allocated
                            // if they are able to be
                            if ((System.nanoTime() - lastRunNS) > DiskThresholdDecider.this.rerouteInterval.nanos()) {
                                lastRunNS = System.nanoTime();
                                reroute = true;
                                explanation = "one or more nodes has gone under the high or low watermark";
                                nodeHasPassedWatermark.remove(node);
                            } else {
                                logger.debug("{} has gone below a disk threshold, but an automatic reroute has occurred " +
                                                "in the last [{}], skipping reroute",
                                        node, DiskThresholdDecider.this.rerouteInterval);
                            }
                        }
                    }
                }
                if (reroute) {
                    logger.info("rerouting shards: [{}]", explanation);
                    // Execute an empty reroute, but don't block on the response
                    client.admin().cluster().prepareReroute().execute();
                }
            }
        }
    }

    public DiskThresholdDecider(Settings settings) {
        // It's okay the Client is null here, because the empty cluster info
        // service will never actually call the listener where the client is
        // needed. Also this constructor is only used for tests
        this(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), EmptyClusterInfoService.INSTANCE, null);
    }

    @Inject
    public DiskThresholdDecider(Settings settings, ClusterSettings clusterSettings, ClusterInfoService infoService, Client client) {
        super(settings);
        final String lowWatermark = CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.get(settings);
        final String highWatermark = CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.get(settings);
        setHighWatermark(highWatermark);
        setLowWatermark(lowWatermark);
        this.includeRelocations = CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING.get(settings);
        this.rerouteInterval = CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(settings);
        this.enabled = CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING, this::setLowWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING, this::setHighWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING, this::setIncludeRelocations);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING, this::setRerouteInterval);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING, this::setEnabled);
        infoService.addListener(new DiskListener(client));
    }

    private void setIncludeRelocations(boolean includeRelocations) {
        this.includeRelocations = includeRelocations;
    }

    private void setRerouteInterval(TimeValue rerouteInterval) {
        this.rerouteInterval = rerouteInterval;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setLowWatermark(String lowWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeDiskThresholdLow = 100.0 - thresholdPercentageFromWatermark(lowWatermark);
        this.freeBytesThresholdLow = thresholdBytesFromWatermark(lowWatermark,
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey());
    }

    private void setHighWatermark(String highWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeDiskThresholdHigh = 100.0 - thresholdPercentageFromWatermark(highWatermark);
        this.freeBytesThresholdHigh = thresholdBytesFromWatermark(highWatermark,
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey());
    }

    // For Testing
    public Double getFreeDiskThresholdLow() {
        return freeDiskThresholdLow;
    }

    // For Testing
    public Double getFreeDiskThresholdHigh() {
        return freeDiskThresholdHigh;
    }

    // For Testing
    public Double getUsedDiskThresholdLow() {
        return 100.0 - freeDiskThresholdLow;
    }

    // For Testing
    public Double getUsedDiskThresholdHigh() {
        return 100.0 - freeDiskThresholdHigh;
    }

    // For Testing
    public ByteSizeValue getFreeBytesThresholdLow() {
        return freeBytesThresholdLow;
    }

    // For Testing
    public ByteSizeValue getFreeBytesThresholdHigh() {
        return freeBytesThresholdHigh;
    }

    // For Testing
    public boolean isIncludeRelocations() {
        return includeRelocations;
    }

    // For Testing
    public boolean isEnabled() {
        return enabled;
    }

    // For Testing
    public TimeValue getRerouteInterval() {
        return rerouteInterval;
    }

    /**
     * Returns the size of all shards that are currently being relocated to
     * the node, but may not be finished transferring yet.
     *
     * If subtractShardsMovingAway is set then the size of shards moving away is subtracted from the total size
     * of all shards
     */
    public static long sizeOfRelocatingShards(RoutingNode node, RoutingAllocation allocation,
                                              boolean subtractShardsMovingAway, String dataPath) {
        ClusterInfo clusterInfo = allocation.clusterInfo();
        long totalSize = 0;
        for (ShardRouting routing : node.shardsWithState(ShardRoutingState.RELOCATING, ShardRoutingState.INITIALIZING)) {
            String actualPath = clusterInfo.getDataPath(routing);
            if (dataPath.equals(actualPath)) {
                if (routing.initializing() && routing.relocatingNodeId() != null) {
                    totalSize += getExpectedShardSize(routing, allocation, 0);
                } else if (subtractShardsMovingAway && routing.relocating()) {
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

        final double usedDiskThresholdLow = 100.0 - DiskThresholdDecider.this.freeDiskThresholdLow;
        final double usedDiskThresholdHigh = 100.0 - DiskThresholdDecider.this.freeDiskThresholdHigh;

        DiskUsage usage = getDiskUsage(node, allocation, usages);
        // First, check that the node currently over the low watermark
        double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        // Cache the used disk percentage for displaying disk percentages consistent with documentation
        double usedDiskPercentage = usage.getUsedDiskAsPercentage();
        long freeBytes = usage.getFreeBytes();
        if (logger.isTraceEnabled()) {
            logger.trace("node [{}] has {}% used disk", node.nodeId(), usedDiskPercentage);
        }

        // a flag for whether the primary shard has been previously allocated
        IndexMetaData indexMetaData = allocation.metaData().getIndexSafe(shardRouting.index());
        boolean primaryHasBeenAllocated = shardRouting.primary() && shardRouting.allocatedPostIndexCreate(indexMetaData);

        // checks for exact byte comparisons
        if (freeBytes < freeBytesThresholdLow.bytes()) {
            // If the shard is a replica or has a primary that has already been allocated before, check the low threshold
            if (!shardRouting.primary() || (shardRouting.primary() && primaryHasBeenAllocated)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("less than the required {} free bytes threshold ({} bytes free) on node {}, preventing allocation",
                            freeBytesThresholdLow, freeBytes, node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                        "the node is above the low watermark and has less than required [%s] free, free: [%s]",
                        freeBytesThresholdLow, new ByteSizeValue(freeBytes));
            } else if (freeBytes > freeBytesThresholdHigh.bytes()) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (logger.isDebugEnabled()) {
                    logger.debug("less than the required {} free bytes threshold ({} bytes free) on node {}, " +
                                    "but allowing allocation because primary has never been allocated",
                            freeBytesThresholdLow, freeBytes, node.nodeId());
                }
                return allocation.decision(Decision.YES, NAME,
                        "the node is above the low watermark, but this primary shard has never been allocated before");
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (logger.isDebugEnabled()) {
                    logger.debug("less than the required {} free bytes threshold ({} bytes free) on node {}, " +
                                    "preventing allocation even though primary has never been allocated",
                            freeBytesThresholdHigh, freeBytes, node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                        "the node is above the high watermark even though this shard has never been allocated " +
                                "and has less than required [%s] free on node, free: [%s]",
                        freeBytesThresholdHigh, new ByteSizeValue(freeBytes));
            }
        }

        // checks for percentage comparisons
        if (freeDiskPercentage < freeDiskThresholdLow) {
            // If the shard is a replica or has a primary that has already been allocated before, check the low threshold
            if (!shardRouting.primary() || (shardRouting.primary() && primaryHasBeenAllocated)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("more than the allowed {} used disk threshold ({} used) on node [{}], preventing allocation",
                            Strings.format1Decimals(usedDiskThresholdLow, "%"),
                            Strings.format1Decimals(usedDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                        "the node is above the low watermark and has more than allowed [%s%%] used disk, free: [%s%%]",
                        usedDiskThresholdLow, freeDiskPercentage);
            } else if (freeDiskPercentage > freeDiskThresholdHigh) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (logger.isDebugEnabled()) {
                    logger.debug("more than the allowed {} used disk threshold ({} used) on node [{}], " +
                                    "but allowing allocation because primary has never been allocated",
                            Strings.format1Decimals(usedDiskThresholdLow, "%"),
                            Strings.format1Decimals(usedDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.YES, NAME,
                        "the node is above the low watermark, but this primary shard has never been allocated before");
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (logger.isDebugEnabled()) {
                    logger.debug("less than the required {} free bytes threshold ({} bytes free) on node {}, " +
                                    "preventing allocation even though primary has never been allocated",
                            Strings.format1Decimals(freeDiskThresholdHigh, "%"),
                            Strings.format1Decimals(freeDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                        "the node is above the high watermark even though this shard has never been allocated " +
                                "and has more than allowed [%s%%] used disk, free: [%s%%]",
                        usedDiskThresholdHigh, freeDiskPercentage);
            }
        }

        // Secondly, check that allocating the shard to this node doesn't put it above the high watermark
        final long shardSize = getExpectedShardSize(shardRouting, allocation, 0);
        double freeSpaceAfterShard = freeDiskPercentageAfterShardAssigned(usage, shardSize);
        long freeBytesAfterShard = freeBytes - shardSize;
        if (freeBytesAfterShard < freeBytesThresholdHigh.bytes()) {
            logger.warn("after allocating, node [{}] would have less than the required " +
                    "{} free bytes threshold ({} bytes free), preventing allocation",
                    node.nodeId(), freeBytesThresholdHigh, freeBytesAfterShard);
            return allocation.decision(Decision.NO, NAME,
                    "after allocating the shard to this node, it would be above the high watermark " +
                            "and have less than required [%s] free, free: [%s]",
                    freeBytesThresholdLow, new ByteSizeValue(freeBytesAfterShard));
        }
        if (freeSpaceAfterShard < freeDiskThresholdHigh) {
            logger.warn("after allocating, node [{}] would have more than the allowed " +
                            "{} free disk threshold ({} free), preventing allocation",
                    node.nodeId(), Strings.format1Decimals(freeDiskThresholdHigh, "%"), Strings.format1Decimals(freeSpaceAfterShard, "%"));
            return allocation.decision(Decision.NO, NAME,
                    "after allocating the shard to this node, it would be above the high watermark " +
                            "and have more than allowed [%s%%] used disk, free: [%s%%]",
                    usedDiskThresholdLow, freeSpaceAfterShard);
        }

        return allocation.decision(Decision.YES, NAME,
                "enough disk for shard on node, free: [%s], shard size: [%s], free after allocating shard: [%s]",
                new ByteSizeValue(freeBytes),
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

        final DiskUsage usage = getDiskUsage(node, allocation, usages);
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
        if (freeBytes < freeBytesThresholdHigh.bytes()) {
            if (logger.isDebugEnabled()) {
                logger.debug("less than the required {} free bytes threshold ({} bytes free) on node {}, shard cannot remain",
                        freeBytesThresholdHigh, freeBytes, node.nodeId());
            }
            return allocation.decision(Decision.NO, NAME,
                    "after allocating this shard this node would be above the high watermark " +
                            "and there would be less than required [%s] free on node, free: [%s]",
                    freeBytesThresholdHigh, new ByteSizeValue(freeBytes));
        }
        if (freeDiskPercentage < freeDiskThresholdHigh) {
            if (logger.isDebugEnabled()) {
                logger.debug("less than the required {}% free disk threshold ({}% free) on node {}, shard cannot remain",
                        freeDiskThresholdHigh, freeDiskPercentage, node.nodeId());
            }
            return allocation.decision(Decision.NO, NAME,
                    "after allocating this shard this node would be above the high watermark " +
                            "and there would be less than required [%s%%] free disk on node, free: [%s%%]",
                    freeDiskThresholdHigh, freeDiskPercentage);
        }

        return allocation.decision(Decision.YES, NAME,
                "there is enough disk on this node for the shard to remain, free: [%s]", new ByteSizeValue(freeBytes));
    }

    private DiskUsage getDiskUsage(RoutingNode node, RoutingAllocation allocation, ImmutableOpenMap<String, DiskUsage> usages) {
        ClusterInfo clusterInfo = allocation.clusterInfo();
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

        if (includeRelocations) {
            long relocatingShardsSize = sizeOfRelocatingShards(node, allocation, true, usage.getPath());
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
    public DiskUsage averageUsage(RoutingNode node, ImmutableOpenMap<String, DiskUsage> usages) {
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
    public double freeDiskPercentageAfterShardAssigned(DiskUsage usage, Long shardSize) {
        shardSize = (shardSize == null) ? 0 : shardSize;
        DiskUsage newUsage = new DiskUsage(usage.getNodeId(), usage.getNodeName(), usage.getPath(),
                usage.getTotalBytes(),  usage.getFreeBytes() - shardSize);
        return newUsage.getFreeDiskAsPercentage();
    }

    /**
     * Attempts to parse the watermark into a percentage, returning 100.0% if
     * it cannot be parsed.
     */
    public double thresholdPercentageFromWatermark(String watermark) {
        try {
            return RatioValue.parseRatioValue(watermark).getAsPercent();
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            return 100.0;
        }
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning
     * a ByteSizeValue of 0 bytes if the value cannot be parsed.
     */
    public ByteSizeValue thresholdBytesFromWatermark(String watermark, String settingName) {
        try {
            return ByteSizeValue.parseBytesSizeValue(watermark, settingName);
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            return ByteSizeValue.parseBytesSizeValue("0b", settingName);
        }
    }

    /**
     * Checks if a watermark string is a valid percentage or byte size value,
     * @return the watermark value given
     */
    public static String validWatermarkSetting(String watermark, String settingName) {
        try {
            RatioValue.parseRatioValue(watermark);
        } catch (ElasticsearchParseException e) {
            try {
                ByteSizeValue.parseBytesSizeValue(watermark, settingName);
            } catch (ElasticsearchParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
        return watermark;

    }

    private Decision earlyTerminate(RoutingAllocation allocation, ImmutableOpenMap<String, DiskUsage> usages) {
        // Always allow allocation if the decider is disabled
        if (!enabled) {
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
    public static final long getExpectedShardSize(ShardRouting shard, RoutingAllocation allocation, long defaultValue) {
        final IndexMetaData metaData = allocation.metaData().getIndexSafe(shard.index());
        final ClusterInfo info = allocation.clusterInfo();
        if (metaData.getMergeSourceIndex() != null && shard.allocatedPostIndexCreate(metaData) == false) {
            // in the shrink index case we sum up the source index shards since we basically make a copy of the shard in
            // the worst case
            long targetShardSize = 0;
            final Index mergeSourceIndex = metaData.getMergeSourceIndex();
            final IndexMetaData sourceIndexMeta = allocation.metaData().getIndexSafe(metaData.getMergeSourceIndex());
            final Set<ShardId> shardIds = IndexMetaData.selectShrinkShards(shard.id(), sourceIndexMeta, metaData.getNumberOfShards());
            for (IndexShardRoutingTable shardRoutingTable : allocation.routingTable().index(mergeSourceIndex.getName())) {
                if (shardIds.contains(shardRoutingTable.shardId())) {
                    targetShardSize += info.getShardSize(shardRoutingTable.primaryShard(), 0);
                }
            }
            return targetShardSize == 0 ? defaultValue : targetShardSize;
        } else {
            return info.getShardSize(shard, defaultValue);
        }

    }
}
