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

package org.elasticsearch.cluster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * InternalClusterInfoService provides the ClusterInfoService interface,
 * routinely updated on a timer. The timer can be dynamically changed by
 * setting the <code>cluster.info.update.interval</code> setting (defaulting
 * to 30 seconds). The InternalClusterInfoService only runs on the master node.
 * Listens for changes in the number of data nodes and immediately submits a
 * ClusterInfoUpdateJob if a node has been added.
 *
 * Every time the timer runs, gathers information about the disk usage and
 * shard sizes across the cluster.
 */
public class InternalClusterInfoService implements ClusterInfoService, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(InternalClusterInfoService.class);

    private static final String REFRESH_EXECUTOR = ThreadPool.Names.MANAGEMENT;

    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING =
        Setting.timeSetting("cluster.info.update.interval", TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(10),
            Property.Dynamic, Property.NodeScope);
    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("cluster.info.update.timeout", TimeValue.timeValueSeconds(15),
            Property.Dynamic, Property.NodeScope);

    private volatile TimeValue updateFrequency;

    private volatile ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsages;
    private volatile ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsages;
    private volatile IndicesStatsSummary indicesStatsSummary;
    // null if this node is not currently the master
    private final AtomicReference<RefreshAndRescheduleRunnable> refreshAndRescheduleRunnable = new AtomicReference<>();
    private volatile boolean enabled;
    private volatile TimeValue fetchTimeout;
    private final ThreadPool threadPool;
    private final Client client;
    private final List<Consumer<ClusterInfo>> listeners = new CopyOnWriteArrayList<>();

    public InternalClusterInfoService(Settings settings, ClusterService clusterService, ThreadPool threadPool, Client client) {
        this.leastAvailableSpaceUsages = ImmutableOpenMap.of();
        this.mostAvailableSpaceUsages = ImmutableOpenMap.of();
        this.indicesStatsSummary = IndicesStatsSummary.EMPTY;
        this.threadPool = threadPool;
        this.client = client;
        this.updateFrequency = INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings);
        this.fetchTimeout = INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.get(settings);
        this.enabled = DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING, this::setFetchTimeout);
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING, this::setUpdateFrequency);
        clusterSettings.addSettingsUpdateConsumer(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING,
                                                  this::setEnabled);
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setFetchTimeout(TimeValue fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }

    void setUpdateFrequency(TimeValue updateFrequency) {
        this.updateFrequency = updateFrequency;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() && refreshAndRescheduleRunnable.get() == null) {
            logger.trace("elected as master, scheduling cluster info update tasks");
            executeRefresh(event.state(), "became master");

            final RefreshAndRescheduleRunnable newRunnable = new RefreshAndRescheduleRunnable();
            refreshAndRescheduleRunnable.set(newRunnable);
            threadPool.scheduleUnlessShuttingDown(updateFrequency, REFRESH_EXECUTOR, newRunnable);
        } else if (event.localNodeMaster() == false) {
            refreshAndRescheduleRunnable.set(null);
            return;
        }

        if (enabled == false) {
            return;
        }

        // Refresh if a data node was added
        for (DiscoveryNode addedNode : event.nodesDelta().addedNodes()) {
            if (addedNode.isDataNode()) {
                executeRefresh(event.state(), "data node added");
                break;
            }
        }

        // Clean up info for any removed nodes
        for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
            if (removedNode.isDataNode()) {
                logger.trace("Removing node from cluster info: {}", removedNode.getId());
                if (leastAvailableSpaceUsages.containsKey(removedNode.getId())) {
                    ImmutableOpenMap.Builder<String, DiskUsage> newMaxUsages = ImmutableOpenMap.builder(leastAvailableSpaceUsages);
                    newMaxUsages.remove(removedNode.getId());
                    leastAvailableSpaceUsages = newMaxUsages.build();
                }
                if (mostAvailableSpaceUsages.containsKey(removedNode.getId())) {
                    ImmutableOpenMap.Builder<String, DiskUsage> newMinUsages = ImmutableOpenMap.builder(mostAvailableSpaceUsages);
                    newMinUsages.remove(removedNode.getId());
                    mostAvailableSpaceUsages = newMinUsages.build();
                }
            }
        }
    }

    private void executeRefresh(ClusterState clusterState, String reason) {
        if (clusterState.nodes().getDataNodes().size() > 1) {
            logger.trace("refreshing cluster info in background [{}]", reason);
            threadPool.executor(REFRESH_EXECUTOR).execute(new RefreshRunnable(reason));
        }
    }

    @Override
    public ClusterInfo getClusterInfo() {
        final IndicesStatsSummary indicesStatsSummary = this.indicesStatsSummary; // single volatile read
        return new ClusterInfo(leastAvailableSpaceUsages, mostAvailableSpaceUsages,
            indicesStatsSummary.shardSizes, indicesStatsSummary.shardRoutingToDataPath, indicesStatsSummary.reservedSpace);
    }

    /**
     * Retrieve the latest nodes stats, calling the listener when complete
     * @return a latch that can be used to wait for the nodes stats to complete if desired
     */
    protected CountDownLatch updateNodeStats(final ActionListener<NodesStatsResponse> listener) {
        final CountDownLatch latch = new CountDownLatch(1);
        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
        nodesStatsRequest.clear();
        nodesStatsRequest.addMetric(NodesStatsRequest.Metric.FS.metricName());
        nodesStatsRequest.timeout(fetchTimeout);
        client.admin().cluster().nodesStats(nodesStatsRequest, new LatchedActionListener<>(listener, latch));
        return latch;
    }

    /**
     * Retrieve the latest indices stats, calling the listener when complete
     * @return a latch that can be used to wait for the indices stats to complete if desired
     */
    protected CountDownLatch updateIndicesStats(final ActionListener<IndicesStatsResponse> listener) {
        final CountDownLatch latch = new CountDownLatch(1);
        final IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.clear();
        indicesStatsRequest.store(true);
        indicesStatsRequest.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_CLOSED);

        client.admin().indices().stats(indicesStatsRequest, new LatchedActionListener<>(listener, latch));
        return latch;
    }

    // allow tests to adjust the node stats on receipt
    List<NodeStats> adjustNodesStats(List<NodeStats> nodeStats) {
        return nodeStats;
    }

    /**
     * Refreshes the ClusterInfo in a blocking fashion
     */
    public final ClusterInfo refresh() {
        logger.trace("refreshing cluster info");
        final CountDownLatch nodeLatch = updateNodeStats(new ActionListener<NodesStatsResponse>() {
            @Override
            public void onResponse(NodesStatsResponse nodesStatsResponse) {
                ImmutableOpenMap.Builder<String, DiskUsage> leastAvailableUsagesBuilder = ImmutableOpenMap.builder();
                ImmutableOpenMap.Builder<String, DiskUsage> mostAvailableUsagesBuilder = ImmutableOpenMap.builder();
                fillDiskUsagePerNode(logger, adjustNodesStats(nodesStatsResponse.getNodes()),
                    leastAvailableUsagesBuilder, mostAvailableUsagesBuilder);
                leastAvailableSpaceUsages = leastAvailableUsagesBuilder.build();
                mostAvailableSpaceUsages = mostAvailableUsagesBuilder.build();
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ReceiveTimeoutTransportException) {
                    logger.error("NodeStatsAction timed out for ClusterInfoUpdateJob", e);
                } else {
                    if (e instanceof ClusterBlockException) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Failed to execute NodeStatsAction for ClusterInfoUpdateJob", e);
                        }
                    } else {
                        logger.warn("Failed to execute NodeStatsAction for ClusterInfoUpdateJob", e);
                    }
                    // we empty the usages list, to be safe - we don't know what's going on.
                    leastAvailableSpaceUsages = ImmutableOpenMap.of();
                    mostAvailableSpaceUsages = ImmutableOpenMap.of();
                }
            }
        });

        final CountDownLatch indicesLatch = updateIndicesStats(new ActionListener<IndicesStatsResponse>() {
            @Override
            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                final ShardStats[] stats = indicesStatsResponse.getShards();
                final ImmutableOpenMap.Builder<String, Long> shardSizeByIdentifierBuilder = ImmutableOpenMap.builder();
                final ImmutableOpenMap.Builder<ShardRouting, String> dataPathByShardRoutingBuilder = ImmutableOpenMap.builder();
                final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace.Builder> reservedSpaceBuilders = new HashMap<>();
                buildShardLevelInfo(logger, stats, shardSizeByIdentifierBuilder, dataPathByShardRoutingBuilder, reservedSpaceBuilders);

                final ImmutableOpenMap.Builder<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> rsrvdSpace = ImmutableOpenMap.builder();
                reservedSpaceBuilders.forEach((nodeAndPath, builder) -> rsrvdSpace.put(nodeAndPath, builder.build()));

                indicesStatsSummary = new IndicesStatsSummary(
                    shardSizeByIdentifierBuilder.build(),
                    dataPathByShardRoutingBuilder.build(),
                    rsrvdSpace.build());
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ReceiveTimeoutTransportException) {
                    logger.error("IndicesStatsAction timed out for ClusterInfoUpdateJob", e);
                } else {
                    if (e instanceof ClusterBlockException) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Failed to execute IndicesStatsAction for ClusterInfoUpdateJob", e);
                        }
                    } else {
                        logger.warn("Failed to execute IndicesStatsAction for ClusterInfoUpdateJob", e);
                    }
                    // we empty the usages list, to be safe - we don't know what's going on.
                    indicesStatsSummary = IndicesStatsSummary.EMPTY;
                }
            }
        });

        try {
            if (nodeLatch.await(fetchTimeout.getMillis(), TimeUnit.MILLISECONDS) == false) {
                logger.warn("Failed to update node information for ClusterInfoUpdateJob within {} timeout", fetchTimeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt status
        }

        try {
            if (indicesLatch.await(fetchTimeout.getMillis(), TimeUnit.MILLISECONDS) == false) {
                logger.warn("Failed to update shard information for ClusterInfoUpdateJob within {} timeout", fetchTimeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt status
        }
        ClusterInfo clusterInfo = getClusterInfo();
        boolean anyListeners = false;
        for (final Consumer<ClusterInfo> listener : listeners) {
            anyListeners = true;
            try {
                logger.trace("notifying [{}] of new cluster info", listener);
                listener.accept(clusterInfo);
            } catch (Exception e) {
                logger.info(new ParameterizedMessage("failed to notify [{}] of new cluster info", listener), e);
            }
        }
        assert anyListeners : "expected to notify at least one listener";
        return clusterInfo;
    }

    @Override
    public void addListener(Consumer<ClusterInfo> clusterInfoConsumer) {
        listeners.add(clusterInfoConsumer);
    }

    static void buildShardLevelInfo(Logger logger, ShardStats[] stats, ImmutableOpenMap.Builder<String, Long> shardSizes,
                                    ImmutableOpenMap.Builder<ShardRouting, String> newShardRoutingToDataPath,
                                    Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace.Builder> reservedSpaceByShard) {
        for (ShardStats s : stats) {
            final ShardRouting shardRouting = s.getShardRouting();
            newShardRoutingToDataPath.put(shardRouting, s.getDataPath());

            final StoreStats storeStats = s.getStats().getStore();
            final long size = storeStats.sizeInBytes();
            final long reserved = storeStats.getReservedSize().getBytes();

            final String shardIdentifier = ClusterInfo.shardIdentifierFromRouting(shardRouting);
            logger.trace("shard: {} size: {} reserved: {}", shardIdentifier, size, reserved);
            shardSizes.put(shardIdentifier, size);

            if (reserved != StoreStats.UNKNOWN_RESERVED_BYTES) {
                final ClusterInfo.ReservedSpace.Builder reservedSpaceBuilder = reservedSpaceByShard.computeIfAbsent(
                    new ClusterInfo.NodeAndPath(shardRouting.currentNodeId(), s.getDataPath()),
                    t -> new ClusterInfo.ReservedSpace.Builder());
                reservedSpaceBuilder.add(shardRouting.shardId(), reserved);
            }
        }
    }

    static void fillDiskUsagePerNode(Logger logger, List<NodeStats> nodeStatsArray,
            ImmutableOpenMap.Builder<String, DiskUsage> newLeastAvaiableUsages,
            ImmutableOpenMap.Builder<String, DiskUsage> newMostAvaiableUsages) {
        for (NodeStats nodeStats : nodeStatsArray) {
            if (nodeStats.getFs() == null) {
                logger.warn("Unable to retrieve node FS stats for {}", nodeStats.getNode().getName());
            } else {
                FsInfo.Path leastAvailablePath = null;
                FsInfo.Path mostAvailablePath = null;
                for (FsInfo.Path info : nodeStats.getFs()) {
                    if (leastAvailablePath == null) {
                        assert mostAvailablePath == null;
                        mostAvailablePath = leastAvailablePath = info;
                    } else if (leastAvailablePath.getAvailable().getBytes() > info.getAvailable().getBytes()) {
                        leastAvailablePath = info;
                    } else if (mostAvailablePath.getAvailable().getBytes() < info.getAvailable().getBytes()) {
                        mostAvailablePath = info;
                    }
                }
                String nodeId = nodeStats.getNode().getId();
                String nodeName = nodeStats.getNode().getName();
                if (logger.isTraceEnabled()) {
                    logger.trace("node: [{}], most available: total disk: {}," +
                            " available disk: {} / least available: total disk: {}, available disk: {}",
                            nodeId, mostAvailablePath.getTotal(), leastAvailablePath.getAvailable(),
                            leastAvailablePath.getTotal(), leastAvailablePath.getAvailable());
                }
                if (leastAvailablePath.getTotal().getBytes() < 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("node: [{}] least available path has less than 0 total bytes of disk [{}], skipping",
                                nodeId, leastAvailablePath.getTotal().getBytes());
                    }
                } else {
                    newLeastAvaiableUsages.put(nodeId, new DiskUsage(nodeId, nodeName, leastAvailablePath.getPath(),
                        leastAvailablePath.getTotal().getBytes(), leastAvailablePath.getAvailable().getBytes()));
                }
                if (mostAvailablePath.getTotal().getBytes() < 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("node: [{}] most available path has less than 0 total bytes of disk [{}], skipping",
                                nodeId, mostAvailablePath.getTotal().getBytes());
                    }
                } else {
                    newMostAvaiableUsages.put(nodeId, new DiskUsage(nodeId, nodeName, mostAvailablePath.getPath(),
                        mostAvailablePath.getTotal().getBytes(), mostAvailablePath.getAvailable().getBytes()));
                }

            }
        }
    }

    private static class IndicesStatsSummary {
        static final IndicesStatsSummary EMPTY
            = new IndicesStatsSummary(ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of());

        final ImmutableOpenMap<String, Long> shardSizes;
        final ImmutableOpenMap<ShardRouting, String> shardRoutingToDataPath;
        final ImmutableOpenMap<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace;

        IndicesStatsSummary(ImmutableOpenMap<String, Long> shardSizes,
                            ImmutableOpenMap<ShardRouting, String> shardRoutingToDataPath,
                            ImmutableOpenMap<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace) {
            this.shardSizes = shardSizes;
            this.shardRoutingToDataPath = shardRoutingToDataPath;
            this.reservedSpace = reservedSpace;
        }
    }

    /**
     * Runs {@link InternalClusterInfoService#refresh()}, logging failures/rejections appropriately.
     */
    private class RefreshRunnable extends AbstractRunnable {
        private final String reason;

        RefreshRunnable(String reason) {
            this.reason = reason;
        }

        @Override
        protected void doRun() {
            if (enabled) {
                logger.trace("refreshing cluster info [{}]", reason);
                refresh();
            } else {
                logger.trace("skipping cluster info refresh [{}] since it is disabled", reason);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(new ParameterizedMessage("refreshing cluster info failed [{}]", reason), e);
        }


        @Override
        public void onRejection(Exception e) {
            final boolean shutDown = e instanceof EsRejectedExecutionException && ((EsRejectedExecutionException) e).isExecutorShutdown();
            logger.log(shutDown ? Level.DEBUG : Level.WARN, "refreshing cluster info rejected [{}]", reason, e);
        }
    }


    /**
     * Runs {@link InternalClusterInfoService#refresh()}, logging failures/rejections appropriately, and reschedules itself on completion.
     */
    private class RefreshAndRescheduleRunnable extends RefreshRunnable {
        RefreshAndRescheduleRunnable() {
            super("scheduled");
        }

        @Override
        protected void doRun() {
            if (this == refreshAndRescheduleRunnable.get()) {
                super.doRun();
            } else {
                logger.trace("master changed, scheduled refresh job is stale");
            }
        }

        @Override
        public void onAfter() {
            if (this == refreshAndRescheduleRunnable.get()) {
                logger.trace("scheduling next cluster info refresh in [{}]", updateFrequency);
                threadPool.scheduleUnlessShuttingDown(updateFrequency, REFRESH_EXECUTOR, this);
            }
        }
    }
}
