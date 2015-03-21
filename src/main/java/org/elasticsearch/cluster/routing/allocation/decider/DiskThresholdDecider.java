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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.TimeValue;
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

    public static final String CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED = "cluster.routing.allocation.disk.threshold_enabled";
    public static final String CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK = "cluster.routing.allocation.disk.watermark.low";
    public static final String CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK = "cluster.routing.allocation.disk.watermark.high";
    public static final String CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS = "cluster.routing.allocation.disk.include_relocations";
    public static final String CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL = "cluster.routing.allocation.disk.reroute_interval";

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String newLowWatermark = settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, null);
            String newHighWatermark = settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, null);
            Boolean newRelocationsSetting = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS, null);
            Boolean newEnableSetting =  settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, null);
            TimeValue newRerouteInterval = settings.getAsTime(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL, null);

            if (newEnableSetting != null) {
                logger.info("updating [{}] from [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED,
                        DiskThresholdDecider.this.enabled, newEnableSetting);
                DiskThresholdDecider.this.enabled = newEnableSetting;
            }
            if (newRelocationsSetting != null) {
                logger.info("updating [{}] from [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS,
                        DiskThresholdDecider.this.includeRelocations, newRelocationsSetting);
                DiskThresholdDecider.this.includeRelocations = newRelocationsSetting;
            }
            if (newLowWatermark != null) {
                if (!validWatermarkSetting(newLowWatermark)) {
                    throw new ElasticsearchParseException("Unable to parse low watermark: [" + newLowWatermark + "]");
                }
                logger.info("updating [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, newLowWatermark);
                DiskThresholdDecider.this.freeDiskThresholdLow = 100.0 - thresholdPercentageFromWatermark(newLowWatermark);
                DiskThresholdDecider.this.freeBytesThresholdLow = thresholdBytesFromWatermark(newLowWatermark);
            }
            if (newHighWatermark != null) {
                if (!validWatermarkSetting(newHighWatermark)) {
                    throw new ElasticsearchParseException("Unable to parse high watermark: [" + newHighWatermark + "]");
                }
                logger.info("updating [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, newHighWatermark);
                DiskThresholdDecider.this.freeDiskThresholdHigh = 100.0 - thresholdPercentageFromWatermark(newHighWatermark);
                DiskThresholdDecider.this.freeBytesThresholdHigh = thresholdBytesFromWatermark(newHighWatermark);
            }
            if (newRerouteInterval != null) {
                logger.info("updating [{}] to [{}]", CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL, newRerouteInterval);
                DiskThresholdDecider.this.rerouteInterval = newRerouteInterval;
            }
        }
    }

    /**
     * Listens for a node to go over the high watermark and kicks off an empty
     * reroute if it does. Also responsible for logging about nodes that have
     * passed the disk watermarks
     */
    class DiskListener implements ClusterInfoService.Listener {
        private final Client client;
        private long lastRun;

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
                        Strings.format1Decimals(DiskThresholdDecider.this.freeDiskThresholdHigh, "%"), usage);
            } else if (usage.getFreeDiskAsPercentage() < DiskThresholdDecider.this.freeDiskThresholdLow) {
                logger.info("low disk watermark [{}] exceeded on {}, replicas will not be assigned to this node",
                        Strings.format1Decimals(DiskThresholdDecider.this.freeDiskThresholdLow, "%"), usage);
            }
        }

        @Override
        public void onNewInfo(ClusterInfo info) {
            Map<String, DiskUsage> usages = info.getNodeDiskUsages();
            if (usages != null) {
                boolean reroute = false;
                for (DiskUsage entry : usages.values()) {
                    warnAboutDiskIfNeeded(entry);
                    if (entry.getFreeBytes() < DiskThresholdDecider.this.freeBytesThresholdHigh.bytes() ||
                            entry.getFreeDiskAsPercentage() < DiskThresholdDecider.this.freeDiskThresholdHigh) {
                        if ((System.currentTimeMillis() - lastRun) > DiskThresholdDecider.this.rerouteInterval.millis()) {
                            lastRun = System.currentTimeMillis();
                            reroute = true;
                        } else {
                            logger.debug("high disk watermark exceeded on {} but an automatic reroute has occurred in the last [{}], skipping reroute",
                                    entry, DiskThresholdDecider.this.rerouteInterval);
                        }
                    }
                }
                if (reroute) {
                    logger.info("high disk watermark exceeded on one or more nodes, rerouting shards");
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
        this(settings, new NodeSettingsService(settings), ClusterInfoService.EMPTY, null);
    }

    @Inject
    public DiskThresholdDecider(Settings settings, NodeSettingsService nodeSettingsService, ClusterInfoService infoService, Client client) {
        super(settings);
        String lowWatermark = settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "85%");
        String highWatermark = settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "90%");

        if (!validWatermarkSetting(lowWatermark)) {
            throw new ElasticsearchParseException("Unable to parse low watermark: [" + lowWatermark + "]");
        }
        if (!validWatermarkSetting(highWatermark)) {
            throw new ElasticsearchParseException("Unable to parse high watermark: [" + highWatermark + "]");
        }
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeDiskThresholdLow = 100.0 - thresholdPercentageFromWatermark(lowWatermark);
        this.freeDiskThresholdHigh = 100.0 - thresholdPercentageFromWatermark(highWatermark);

        this.freeBytesThresholdLow = thresholdBytesFromWatermark(lowWatermark);
        this.freeBytesThresholdHigh = thresholdBytesFromWatermark(highWatermark);
        this.includeRelocations = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS, true);
        this.rerouteInterval = settings.getAsTime(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL, TimeValue.timeValueSeconds(60));

        this.enabled = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true);
        nodeSettingsService.addListener(new ApplySettings());
        infoService.addListener(new DiskListener(client));
    }

    // For Testing
    ApplySettings newApplySettings() {
        return new ApplySettings();
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
     * the node, but may not be finished transfering yet.
     *
     * If subtractShardsMovingAway is set then the size of shards moving away is subtracted from the total size
     * of all shards
     */
    public long sizeOfRelocatingShards(RoutingNode node, Map<String, Long> shardSizes, boolean subtractShardsMovingAway) {
        long totalSize = 0;
        for (ShardRouting routing : node.shardsWithState(ShardRoutingState.RELOCATING, ShardRoutingState.INITIALIZING)) {
            if (routing.initializing() && routing.relocatingNodeId() != null) {
                totalSize += getShardSize(routing, shardSizes);
            } else if (subtractShardsMovingAway && routing.relocating()) {
                totalSize -= getShardSize(routing, shardSizes);
            }
        }
        return totalSize;
    }

    private long getShardSize(ShardRouting routing, Map<String, Long> shardSizes) {
        Long shardSize = shardSizes.get(shardIdentifierFromRouting(routing));
        return shardSize == null ? 0 : shardSize;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {

        // Always allow allocation if the decider is disabled
        if (!enabled) {
            return allocation.decision(Decision.YES, NAME, "disk threshold decider disabled");
        }

        // Allow allocation regardless if only a single node is available
        if (allocation.nodes().size() <= 1) {
            if (logger.isTraceEnabled()) {
                logger.trace("Only a single node is present, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "only a single node is present");
        }

        // Fail open there is no info available
        ClusterInfo clusterInfo = allocation.clusterInfo();
        if (clusterInfo == null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Cluster info unavailable for disk threshold decider, allowing allocation.");
            }
            return allocation.decision(Decision.YES, NAME, "cluster info unavailable");
        }

        // Fail open if there are no disk usages available
        Map<String, DiskUsage> usages = clusterInfo.getNodeDiskUsages();
        Map<String, Long> shardSizes = clusterInfo.getShardSizes();
        if (usages.isEmpty()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Unable to determine disk usages for disk-aware allocation, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "disk usages unavailable");
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

        if (includeRelocations) {
            long relocatingShardsSize = sizeOfRelocatingShards(node, shardSizes, false);
            DiskUsage usageIncludingRelocations = new DiskUsage(node.nodeId(), node.node().name(),
                    usage.getTotalBytes(), usage.getFreeBytes() - relocatingShardsSize);
            if (logger.isTraceEnabled()) {
                logger.trace("usage without relocations: {}", usage);
                logger.trace("usage with relocations: [{} bytes] {}", relocatingShardsSize, usageIncludingRelocations);
            }
            usage = usageIncludingRelocations;
        }

        // First, check that the node currently over the low watermark
        double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        long freeBytes = usage.getFreeBytes();
        if (logger.isTraceEnabled()) {
            logger.trace("Node [{}] has {}% free disk", node.nodeId(), freeDiskPercentage);
        }

        // a flag for whether the primary shard has been previously allocated
        boolean primaryHasBeenAllocated = allocation.routingTable().index(shardRouting.index()).shard(shardRouting.id()).primaryAllocatedPostApi();

        // checks for exact byte comparisons
        if (freeBytes < freeBytesThresholdLow.bytes()) {
            // If the shard is a replica or has a primary that has already been allocated before, check the low threshold
            if (!shardRouting.primary() || (shardRouting.primary() && primaryHasBeenAllocated)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Less than the required {} free bytes threshold ({} bytes free) on node {}, preventing allocation",
                            freeBytesThresholdLow, freeBytes, node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME, "less than required [%s] free on node, free: [%s]",
                        freeBytesThresholdLow, new ByteSizeValue(freeBytes));
            } else if (freeBytes > freeBytesThresholdHigh.bytes()) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (logger.isDebugEnabled()) {
                    logger.debug("Less than the required {} free bytes threshold ({} bytes free) on node {}, " +
                                    "but allowing allocation because primary has never been allocated",
                            freeBytesThresholdLow, freeBytes, node.nodeId());
                }
                return allocation.decision(Decision.YES, NAME, "primary has never been allocated before");
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (logger.isDebugEnabled()) {
                    logger.debug("Less than the required {} free bytes threshold ({} bytes free) on node {}, " +
                                    "preventing allocation even though primary has never been allocated",
                            freeBytesThresholdHigh, freeBytes, node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME, "less than required [%s] free on node, free: [%s]",
                        freeBytesThresholdHigh, new ByteSizeValue(freeBytes));
            }
        }

        // checks for percentage comparisons
        if (freeDiskPercentage < freeDiskThresholdLow) {
            // If the shard is a replica or has a primary that has already been allocated before, check the low threshold
            if (!shardRouting.primary() || (shardRouting.primary() && primaryHasBeenAllocated)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Less than the required {} free disk threshold ({} free) on node [{}], preventing allocation",
                            Strings.format1Decimals(freeDiskThresholdLow, "%"),
                            Strings.format1Decimals(freeDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME, "less than required [%s%%] free disk on node, free: [%s%%]",
                        freeDiskThresholdLow, freeDiskPercentage);
            } else if (freeDiskPercentage > freeDiskThresholdHigh) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (logger.isDebugEnabled()) {
                    logger.debug("Less than the required {} free disk threshold ({} free) on node [{}], " +
                                    "but allowing allocation because primary has never been allocated",
                            Strings.format1Decimals(freeDiskThresholdLow, "%"),
                            Strings.format1Decimals(freeDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.YES, NAME, "primary has never been allocated before");
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (logger.isDebugEnabled()) {
                    logger.debug("Less than the required {} free bytes threshold ({} bytes free) on node {}, " +
                                    "preventing allocation even though primary has never been allocated",
                            Strings.format1Decimals(freeDiskThresholdHigh, "%"),
                            Strings.format1Decimals(freeDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME, "less than required [%s%%] free disk on node, free: [%s%%]",
                        freeDiskThresholdLow, freeDiskPercentage);
            }
        }

        // Secondly, check that allocating the shard to this node doesn't put it above the high watermark
        Long shardSize = shardSizes.get(shardIdentifierFromRouting(shardRouting));
        shardSize = shardSize == null ? 0 : shardSize;
        double freeSpaceAfterShard = this.freeDiskPercentageAfterShardAssigned(usage, shardSize);
        long freeBytesAfterShard = freeBytes - shardSize;
        if (freeBytesAfterShard < freeBytesThresholdHigh.bytes()) {
            logger.warn("After allocating, node [{}] would have less than the required {} free bytes threshold ({} bytes free), preventing allocation",
                    node.nodeId(), freeBytesThresholdHigh, freeBytesAfterShard);
            return allocation.decision(Decision.NO, NAME, "after allocation less than required [%s] free on node, free: [%s]",
                    freeBytesThresholdLow, new ByteSizeValue(freeBytesAfterShard));
        }
        if (freeSpaceAfterShard < freeDiskThresholdHigh) {
            logger.warn("After allocating, node [{}] would have less than the required {} free disk threshold ({} free), preventing allocation",
                    node.nodeId(), Strings.format1Decimals(freeDiskThresholdHigh, "%"), Strings.format1Decimals(freeSpaceAfterShard, "%"));
            return allocation.decision(Decision.NO, NAME, "after allocation less than required [%s%%] free disk on node, free: [%s%%]",
                    freeDiskThresholdLow, freeSpaceAfterShard);
        }

        return allocation.decision(Decision.YES, NAME, "enough disk for shard on node, free: [%s]", new ByteSizeValue(freeBytes));
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (!enabled) {
            return allocation.decision(Decision.YES, NAME, "disk threshold decider disabled");
        }
        // Allow allocation regardless if only a single node is available
        if (allocation.nodes().size() <= 1) {
            return allocation.decision(Decision.YES, NAME, "only a single node is present");
        }

        ClusterInfo clusterInfo = allocation.clusterInfo();
        if (clusterInfo == null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Cluster info unavailable for disk threshold decider, allowing allocation.");
            }
            return allocation.decision(Decision.YES, NAME, "cluster info unavailable");
        }

        Map<String, DiskUsage> usages = clusterInfo.getNodeDiskUsages();
        if (usages.isEmpty()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Unable to determine disk usages for disk-aware allocation, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "disk usages unavailable");
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

        if (includeRelocations) {
            Map<String, Long> shardSizes = clusterInfo.getShardSizes();
            long relocatingShardsSize = sizeOfRelocatingShards(node, shardSizes, true);
            DiskUsage usageIncludingRelocations = new DiskUsage(node.nodeId(), node.node().name(),
                    usage.getTotalBytes(), usage.getFreeBytes() - relocatingShardsSize);
            if (logger.isTraceEnabled()) {
                logger.trace("usage without relocations: {}", usage);
                logger.trace("usage with relocations: [{} bytes] {}", relocatingShardsSize, usageIncludingRelocations);
            }
            usage = usageIncludingRelocations;
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
            return allocation.decision(Decision.NO, NAME, "after allocation less than required [%s] free on node, free: [%s]",
                    freeBytesThresholdHigh, new ByteSizeValue(freeBytes));
        }
        if (freeDiskPercentage < freeDiskThresholdHigh) {
            if (logger.isDebugEnabled()) {
                logger.debug("Less than the required {}% free disk threshold ({}% free) on node {}, shard cannot remain",
                        freeDiskThresholdHigh, freeDiskPercentage, node.nodeId());
            }
            return allocation.decision(Decision.NO, NAME, "after allocation less than required [%s%%] free disk on node, free: [%s%%]",
                    freeDiskThresholdHigh, freeDiskPercentage);
        }

        return allocation.decision(Decision.YES, NAME, "enough disk for shard to remain on node, free: [%s]", new ByteSizeValue(freeBytes));
    }

    /**
     * Returns a {@link DiskUsage} for the {@link RoutingNode} using the
     * average usage of other nodes in the disk usage map.
     * @param node Node to return an averaged DiskUsage object for
     * @param usages Map of nodeId to DiskUsage for all known nodes
     * @return DiskUsage representing given node using the average disk usage
     */
    public DiskUsage averageUsage(RoutingNode node, Map<String, DiskUsage> usages) {
        if (usages.size() == 0) {
            return new DiskUsage(node.nodeId(), node.node().name(), 0, 0);
        }
        long totalBytes = 0;
        long freeBytes = 0;
        for (DiskUsage du : usages.values()) {
            totalBytes += du.getTotalBytes();
            freeBytes += du.getFreeBytes();
        }
        return new DiskUsage(node.nodeId(), node.node().name(), totalBytes / usages.size(), freeBytes / usages.size());
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
        DiskUsage newUsage = new DiskUsage(usage.getNodeId(), usage.getNodeName(),
                usage.getTotalBytes(), usage.getFreeBytes() - shardSize);
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
        } catch (ElasticsearchParseException ex) {
            return ByteSizeValue.parseBytesSizeValue("0b");
        }
    }

    /**
     * Checks if a watermark string is a valid percentage or byte size value,
     * returning true if valid, false if invalid.
     */
    public boolean validWatermarkSetting(String watermark) {
        try {
            RatioValue.parseRatioValue(watermark);
            return true;
        } catch (ElasticsearchParseException e) {
            try {
                ByteSizeValue.parseBytesSizeValue(watermark);
                return true;
            } catch (ElasticsearchParseException ex) {
                return false;
            }
        }
    }
}
