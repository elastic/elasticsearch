/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
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

    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING =
        Setting.timeSetting("cluster.info.update.interval", TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(10),
            Property.Dynamic, Property.NodeScope);
    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("cluster.info.update.timeout", TimeValue.timeValueSeconds(15),
            Property.Dynamic, Property.NodeScope);

    private volatile boolean enabled;
    private volatile TimeValue updateFrequency;
    private volatile TimeValue fetchTimeout;

    private volatile ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsages;
    private volatile ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsages;
    private volatile IndicesStatsSummary indicesStatsSummary;

    private final ThreadPool threadPool;
    private final Client client;
    private final List<Consumer<ClusterInfo>> listeners = new CopyOnWriteArrayList<>();

    private final Object mutex = new Object();
    private final List<ActionListener<ClusterInfo>> nextRefreshListeners = new ArrayList<>();
    private AsyncRefresh currentRefresh;
    private RefreshScheduler refreshScheduler;

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
        final Runnable newRefresh;
        synchronized (mutex) {
            if (event.localNodeMaster() == false) {
                refreshScheduler = null;
                return;
            }

            if (refreshScheduler == null) {
                logger.trace("elected as master, scheduling cluster info update tasks");
                refreshScheduler = new RefreshScheduler();
                nextRefreshListeners.add(refreshScheduler.getListener());
            }
            newRefresh = getNewRefresh();
            assert assertRefreshInvariant();
        }
        newRefresh.run();

        // Refresh if a data node was added
        for (DiscoveryNode addedNode : event.nodesDelta().addedNodes()) {
            if (addedNode.canContainData()) {
                refreshAsync(new PlainActionFuture<>());
                break;
            }
        }
    }

    private class AsyncRefresh {

        private final List<ActionListener<ClusterInfo>> thisRefreshListeners;
        private final CountDown countDown = new CountDown(2);

        AsyncRefresh(List<ActionListener<ClusterInfo>> thisRefreshListeners) {
            this.thisRefreshListeners = thisRefreshListeners;
        }

        void execute() {
            assert countDown.isCountedDown() == false;

            logger.trace("starting async refresh");

            final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
            nodesStatsRequest.clear();
            nodesStatsRequest.addMetric(NodesStatsRequest.Metric.FS.metricName());
            nodesStatsRequest.timeout(fetchTimeout);
            client.admin().cluster().nodesStats(nodesStatsRequest, ActionListener.runAfter(new ActionListener<>() {
                @Override
                public void onResponse(NodesStatsResponse nodesStatsResponse) {
                    logger.trace("received node stats response");

                    for (final FailedNodeException failure : nodesStatsResponse.failures()) {
                        final Throwable cause = failure.getCause();
                        if (logger.isDebugEnabled()) {
                            logger.warn(new ParameterizedMessage("failed to retrieve stats for node [{}]", failure.nodeId()), cause);
                        } else {
                            logger.warn("failed to retrieve stats for node [{}]: {}", failure.nodeId(), cause.getMessage());
                        }
                    }

                    ImmutableOpenMap.Builder<String, DiskUsage> leastAvailableUsagesBuilder = ImmutableOpenMap.builder();
                    ImmutableOpenMap.Builder<String, DiskUsage> mostAvailableUsagesBuilder = ImmutableOpenMap.builder();
                    fillDiskUsagePerNode(adjustNodesStats(nodesStatsResponse.getNodes()),
                            leastAvailableUsagesBuilder, mostAvailableUsagesBuilder);
                    leastAvailableSpaceUsages = leastAvailableUsagesBuilder.build();
                    mostAvailableSpaceUsages = mostAvailableUsagesBuilder.build();
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ClusterBlockException) {
                        logger.trace("failed to retrieve node stats", e);
                    } else {
                        logger.warn("failed to retrieve node stats", e);
                    }
                    leastAvailableSpaceUsages = ImmutableOpenMap.of();
                    mostAvailableSpaceUsages = ImmutableOpenMap.of();
                }
            }, this::onStatsProcessed));

            final IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.clear();
            indicesStatsRequest.store(true);
            indicesStatsRequest.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_CLOSED_HIDDEN);
            indicesStatsRequest.timeout(fetchTimeout);
            client.admin().indices().stats(indicesStatsRequest, ActionListener.runAfter(new ActionListener<>() {
                @Override
                public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                    logger.trace("received indices stats response");

                    if (indicesStatsResponse.getShardFailures().length > 0) {
                        final Set<String> failedNodeIds = new HashSet<>();
                        for (final DefaultShardOperationFailedException shardFailure : indicesStatsResponse.getShardFailures()) {
                            if (shardFailure.getCause() instanceof FailedNodeException) {
                                final FailedNodeException failedNodeException = (FailedNodeException) shardFailure.getCause();
                                if (failedNodeIds.add(failedNodeException.nodeId())) {
                                    if (logger.isDebugEnabled()) {
                                        logger.warn(new ParameterizedMessage("failed to retrieve shard stats from node [{}]",
                                                failedNodeException.nodeId()), failedNodeException);
                                    } else {
                                        logger.warn("failed to retrieve shard stats from node [{}]: {}",
                                                failedNodeException.nodeId(), failedNodeException.getCause().getMessage());
                                    }
                                }
                                logger.trace(new ParameterizedMessage("failed to retrieve stats for shard [{}][{}]",
                                        shardFailure.index(), shardFailure.shardId()), shardFailure.getCause());
                            } else {
                                logger.warn(new ParameterizedMessage("failed to retrieve stats for shard [{}][{}]",
                                        shardFailure.index(), shardFailure.shardId()), shardFailure.getCause());
                            }
                        }
                    }

                    final ShardStats[] stats = indicesStatsResponse.getShards();
                    final ImmutableOpenMap.Builder<String, Long> shardSizeByIdentifierBuilder = ImmutableOpenMap.builder();
                    final ImmutableOpenMap.Builder<ShardId, Long> shardDataSetSizeBuilder = ImmutableOpenMap.builder();
                    final ImmutableOpenMap.Builder<ShardRouting, String> dataPathByShardRoutingBuilder = ImmutableOpenMap.builder();
                    final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace.Builder> reservedSpaceBuilders = new HashMap<>();
                    buildShardLevelInfo(stats, shardSizeByIdentifierBuilder, shardDataSetSizeBuilder,
                        dataPathByShardRoutingBuilder, reservedSpaceBuilders);

                    final ImmutableOpenMap.Builder<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> rsrvdSpace
                            = ImmutableOpenMap.builder();
                    reservedSpaceBuilders.forEach((nodeAndPath, builder) -> rsrvdSpace.put(nodeAndPath, builder.build()));

                    indicesStatsSummary = new IndicesStatsSummary(
                        shardSizeByIdentifierBuilder.build(), shardDataSetSizeBuilder.build(),
                        dataPathByShardRoutingBuilder.build(),
                        rsrvdSpace.build());
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ClusterBlockException) {
                        logger.trace("failed to retrieve indices stats", e);
                    } else {
                        logger.warn("failed to retrieve indices stats", e);
                    }
                    indicesStatsSummary = IndicesStatsSummary.EMPTY;
                }
            }, this::onStatsProcessed));
        }

        private void onStatsProcessed() {
            if (countDown.countDown()) {
                logger.trace("stats all received, computing cluster info and notifying listeners");
                try {
                    final ClusterInfo clusterInfo = getClusterInfo();
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

                    for (final ActionListener<ClusterInfo> listener : thisRefreshListeners) {
                        listener.onResponse(clusterInfo);
                    }
                } finally {
                    onRefreshComplete(this);
                }
            }
        }
    }

    private void onRefreshComplete(AsyncRefresh completedRefresh) {
        final Runnable newRefresh;
        synchronized (mutex) {
            assert currentRefresh == completedRefresh;
            currentRefresh = null;

            // We only ever run one refresh at once; if another refresh was requested while this one was running then we must start another
            // to ensure that the stats it sees are up-to-date.
            newRefresh = getNewRefresh();
            assert assertRefreshInvariant();
        }
        newRefresh.run();
    }

    private Runnable getNewRefresh() {
        assert Thread.holdsLock(mutex) : "mutex not held";

        if (currentRefresh != null) {
            return () -> {};
        }

        if (nextRefreshListeners.isEmpty()) {
            return () -> {};
        }

        final ArrayList<ActionListener<ClusterInfo>> thisRefreshListeners = new ArrayList<>(nextRefreshListeners);
        nextRefreshListeners.clear();

        if (enabled) {
            currentRefresh = new AsyncRefresh(thisRefreshListeners);
            return currentRefresh::execute;
        } else {
            return () -> {
                leastAvailableSpaceUsages = ImmutableOpenMap.of();
                mostAvailableSpaceUsages = ImmutableOpenMap.of();
                indicesStatsSummary = IndicesStatsSummary.EMPTY;
                thisRefreshListeners.forEach(l -> l.onResponse(ClusterInfo.EMPTY));
            };
        }
    }

    private boolean assertRefreshInvariant() {
        assert Thread.holdsLock(mutex) : "mutex not held";
        // We never leave a refresh listener waiting unless we're already refreshing (which will pick up the waiting listener on completion)
        assert nextRefreshListeners.isEmpty() || currentRefresh != null;
        return true;
    }

    private class RefreshScheduler {

        ActionListener<ClusterInfo> getListener() {
            return ActionListener.wrap(() -> {
                if (shouldRefresh()) {
                    threadPool.scheduleUnlessShuttingDown(updateFrequency, ThreadPool.Names.SAME, () -> {
                        if (shouldRefresh()) {
                            refreshAsync(getListener());
                        }
                    });
                }
            });
        }

        private boolean shouldRefresh() {
            synchronized (mutex) {
                return refreshScheduler == this;
            }
        }
    }

    @Override
    public ClusterInfo getClusterInfo() {
        final IndicesStatsSummary indicesStatsSummary = this.indicesStatsSummary; // single volatile read
        return new ClusterInfo(leastAvailableSpaceUsages, mostAvailableSpaceUsages,
            indicesStatsSummary.shardSizes, indicesStatsSummary.shardDataSetSizes,
            indicesStatsSummary.shardRoutingToDataPath, indicesStatsSummary.reservedSpace);
    }

    // allow tests to adjust the node stats on receipt
    List<NodeStats> adjustNodesStats(List<NodeStats> nodeStats) {
        return nodeStats;
    }

    void refreshAsync(ActionListener<ClusterInfo> future) {
        final Runnable newRefresh;
        synchronized (mutex) {
            nextRefreshListeners.add(future);
            newRefresh = getNewRefresh();
            assert assertRefreshInvariant();
        }
        newRefresh.run();
    }

    @Override
    public void addListener(Consumer<ClusterInfo> clusterInfoConsumer) {
        listeners.add(clusterInfoConsumer);
    }

    static void buildShardLevelInfo(ShardStats[] stats, ImmutableOpenMap.Builder<String, Long> shardSizes,
                                    ImmutableOpenMap.Builder<ShardId, Long> shardDataSetSizeBuilder,
                                    ImmutableOpenMap.Builder<ShardRouting, String> newShardRoutingToDataPath,
                                    Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace.Builder> reservedSpaceByShard) {
        for (ShardStats s : stats) {
            final ShardRouting shardRouting = s.getShardRouting();
            newShardRoutingToDataPath.put(shardRouting, s.getDataPath());

            final StoreStats storeStats = s.getStats().getStore();
            if (storeStats == null) {
                continue;
            }
            final long size = storeStats.sizeInBytes();
            final long dataSetSize = storeStats.totalDataSetSizeInBytes();
            final long reserved = storeStats.getReservedSize().getBytes();

            final String shardIdentifier = ClusterInfo.shardIdentifierFromRouting(shardRouting);
            logger.trace("shard: {} size: {} reserved: {}", shardIdentifier, size, reserved);
            shardSizes.put(shardIdentifier, size);
            if (dataSetSize > shardDataSetSizeBuilder.getOrDefault(shardRouting.shardId(), -1L)) {
                shardDataSetSizeBuilder.put(shardRouting.shardId(), dataSetSize);
            }
            if (reserved != StoreStats.UNKNOWN_RESERVED_BYTES) {
                final ClusterInfo.ReservedSpace.Builder reservedSpaceBuilder = reservedSpaceByShard.computeIfAbsent(
                    new ClusterInfo.NodeAndPath(shardRouting.currentNodeId(), s.getDataPath()),
                    t -> new ClusterInfo.ReservedSpace.Builder());
                reservedSpaceBuilder.add(shardRouting.shardId(), reserved);
            }
        }
    }

    static void fillDiskUsagePerNode(List<NodeStats> nodeStatsArray,
            ImmutableOpenMap.Builder<String, DiskUsage> newLeastAvailableUsages,
            ImmutableOpenMap.Builder<String, DiskUsage> newMostAvailableUsages) {
        for (NodeStats nodeStats : nodeStatsArray) {
            if (nodeStats.getFs() == null) {
                logger.warn("node [{}/{}] did not return any filesystem stats", nodeStats.getNode().getName(), nodeStats.getNode().getId());
                continue;
            }

            FsInfo.Path leastAvailablePath = null;
            FsInfo.Path mostAvailablePath = null;
            for (FsInfo.Path info : nodeStats.getFs()) {
                if (leastAvailablePath == null) {
                    //noinspection ConstantConditions this assertion is for the benefit of readers, it's always true
                    assert mostAvailablePath == null;
                    mostAvailablePath = leastAvailablePath = info;
                } else if (leastAvailablePath.getAvailable().getBytes() > info.getAvailable().getBytes()) {
                    leastAvailablePath = info;
                } else if (mostAvailablePath.getAvailable().getBytes() < info.getAvailable().getBytes()) {
                    mostAvailablePath = info;
                }
            }
            if (leastAvailablePath == null) {
                //noinspection ConstantConditions this assertion is for the benefit of readers, it's always true
                assert mostAvailablePath == null;
                logger.warn("node [{}/{}] did not return any filesystem stats", nodeStats.getNode().getName(), nodeStats.getNode().getId());
                continue;
            }

            final String nodeId = nodeStats.getNode().getId();
            final String nodeName = nodeStats.getNode().getName();
            if (logger.isTraceEnabled()) {
                logger.trace("node [{}]: most available: total: {}, available: {} / least available: total: {}, available: {}",
                        nodeId, mostAvailablePath.getTotal(), mostAvailablePath.getAvailable(),
                        leastAvailablePath.getTotal(), leastAvailablePath.getAvailable());
            }
            if (leastAvailablePath.getTotal().getBytes() < 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("node: [{}] least available path has less than 0 total bytes of disk [{}], skipping",
                            nodeId, leastAvailablePath.getTotal().getBytes());
                }
            } else {
                newLeastAvailableUsages.put(nodeId, new DiskUsage(nodeId, nodeName, leastAvailablePath.getPath(),
                    leastAvailablePath.getTotal().getBytes(), leastAvailablePath.getAvailable().getBytes()));
            }
            if (mostAvailablePath.getTotal().getBytes() < 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("node: [{}] most available path has less than 0 total bytes of disk [{}], skipping",
                            nodeId, mostAvailablePath.getTotal().getBytes());
                }
            } else {
                newMostAvailableUsages.put(nodeId, new DiskUsage(nodeId, nodeName, mostAvailablePath.getPath(),
                    mostAvailablePath.getTotal().getBytes(), mostAvailablePath.getAvailable().getBytes()));
            }

        }
    }

    private static class IndicesStatsSummary {
        static final IndicesStatsSummary EMPTY
            = new IndicesStatsSummary(ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of());

        final ImmutableOpenMap<String, Long> shardSizes;
        final ImmutableOpenMap<ShardId, Long> shardDataSetSizes;
        final ImmutableOpenMap<ShardRouting, String> shardRoutingToDataPath;
        final ImmutableOpenMap<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace;

        IndicesStatsSummary(ImmutableOpenMap<String, Long> shardSizes,
                            ImmutableOpenMap<ShardId, Long> shardDataSetSizes,
                            ImmutableOpenMap<ShardRouting, String> shardRoutingToDataPath,
                            ImmutableOpenMap<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace) {
            this.shardSizes = shardSizes;
            this.shardDataSetSizes = shardDataSetSizes;
            this.shardRoutingToDataPath = shardRoutingToDataPath;
            this.reservedSpace = reservedSpace;
        }
    }

}
