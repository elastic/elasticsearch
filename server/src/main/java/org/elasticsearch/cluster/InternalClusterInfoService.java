/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings.WriteLoadDeciderStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING;
import static org.elasticsearch.core.Strings.format;

/**
 * InternalClusterInfoService provides the ClusterInfoService interface,
 * routinely updated on a timer. The timer can be dynamically changed by
 * setting the <code>cluster.info.update.interval</code> setting (defaulting
 * to 30 seconds). The InternalClusterInfoService only runs on the master node.
 * Listens for changes in the number of data nodes and immediately submits a
 * ClusterInfoUpdateJob if a node has been added.
 *
 * Every time the timer runs, if <code>cluster.routing.allocation.disk.threshold_enabled</code>
 * is enabled, gathers information about the disk usage and shard sizes across the cluster,
 * computes a new cluster info and notifies the registered listeners. If disk threshold
 * monitoring is disabled, listeners are called with an empty cluster info.
 */
public class InternalClusterInfoService implements ClusterInfoService, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(InternalClusterInfoService.class);

    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.info.update.interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(10),
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "cluster.info.update.timeout",
        TimeValue.timeValueSeconds(15),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED = Setting.boolSetting(
        "cluster.routing.allocation.estimated_heap.threshold_enabled",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile boolean diskThresholdEnabled;
    private volatile boolean estimatedHeapThresholdEnabled;
    private volatile WriteLoadDeciderStatus writeLoadConstraintEnabled;
    private volatile TimeValue updateFrequency;
    private volatile TimeValue fetchTimeout;

    private volatile Map<String, DiskUsage> leastAvailableSpaceUsages;
    private volatile Map<String, DiskUsage> mostAvailableSpaceUsages;
    private volatile Map<String, ByteSizeValue> maxHeapPerNode;
    private volatile Map<String, Long> estimatedHeapUsagePerNode;
    private volatile Map<String, NodeUsageStatsForThreadPools> nodeThreadPoolUsageStatsPerNode;
    private volatile IndicesStatsSummary indicesStatsSummary;

    private final ThreadPool threadPool;
    private final Client client;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final List<Consumer<ClusterInfo>> listeners = new CopyOnWriteArrayList<>();

    private final Object mutex = new Object();
    private final List<ActionListener<ClusterInfo>> nextRefreshListeners = new ArrayList<>();
    private final EstimatedHeapUsageCollector estimatedHeapUsageCollector;
    private final NodeUsageStatsForThreadPoolsCollector nodeUsageStatsForThreadPoolsCollector;

    private AsyncRefresh currentRefresh;
    private RefreshScheduler refreshScheduler;

    @SuppressWarnings("this-escape")
    public InternalClusterInfoService(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        EstimatedHeapUsageCollector estimatedHeapUsageCollector,
        NodeUsageStatsForThreadPoolsCollector nodeUsageStatsForThreadPoolsCollector
    ) {
        this.leastAvailableSpaceUsages = Map.of();
        this.mostAvailableSpaceUsages = Map.of();
        this.maxHeapPerNode = Map.of();
        this.estimatedHeapUsagePerNode = Map.of();
        this.nodeThreadPoolUsageStatsPerNode = Map.of();
        this.indicesStatsSummary = IndicesStatsSummary.EMPTY;
        this.threadPool = threadPool;
        this.client = client;
        this.estimatedHeapUsageCollector = estimatedHeapUsageCollector;
        this.nodeUsageStatsForThreadPoolsCollector = nodeUsageStatsForThreadPoolsCollector;
        this.updateFrequency = INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings);
        this.fetchTimeout = INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.get(settings);
        this.diskThresholdEnabled = DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        this.clusterStateSupplier = clusterService::state;
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING, this::setFetchTimeout);
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING, this::setUpdateFrequency);
        clusterSettings.addSettingsUpdateConsumer(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING,
            this::setDiskThresholdEnabled
        );
        clusterSettings.initializeAndWatch(
            CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
            this::setEstimatedHeapThresholdEnabled
        );

        clusterSettings.initializeAndWatch(WRITE_LOAD_DECIDER_ENABLED_SETTING, this::setWriteLoadConstraintEnabled);
    }

    private void setDiskThresholdEnabled(boolean diskThresholdEnabled) {
        this.diskThresholdEnabled = diskThresholdEnabled;
    }

    private void setEstimatedHeapThresholdEnabled(boolean estimatedHeapThresholdEnabled) {
        this.estimatedHeapThresholdEnabled = estimatedHeapThresholdEnabled;
    }

    private void setWriteLoadConstraintEnabled(WriteLoadDeciderStatus writeLoadConstraintEnabled) {
        this.writeLoadConstraintEnabled = writeLoadConstraintEnabled;
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
        private final RefCountingRunnable fetchRefs = new RefCountingRunnable(this::callListeners);

        AsyncRefresh(List<ActionListener<ClusterInfo>> thisRefreshListeners) {
            this.thisRefreshListeners = thisRefreshListeners;
        }

        void execute() {
            logger.trace("starting async refresh");

            try (var ignoredRefs = fetchRefs) {
                maybeFetchIndicesStats(diskThresholdEnabled || writeLoadConstraintEnabled == WriteLoadDeciderStatus.ENABLED);
                maybeFetchNodeStats(diskThresholdEnabled || estimatedHeapThresholdEnabled);
                maybeFetchNodesEstimatedHeapUsage(estimatedHeapThresholdEnabled);
                maybeFetchNodesUsageStatsForThreadPools(writeLoadConstraintEnabled);
            }
        }

        private void maybeFetchIndicesStats(boolean shouldFetch) {
            if (shouldFetch) {
                try (var ignored = threadPool.getThreadContext().clearTraceContext()) {
                    fetchIndicesStats();
                }
            } else {
                logger.trace("skipping collecting disk usage info from cluster, notifying listeners with empty indices stats");
                indicesStatsSummary = IndicesStatsSummary.EMPTY;
            }
        }

        private void maybeFetchNodeStats(boolean shouldFetch) {
            if (shouldFetch) {
                try (var ignored = threadPool.getThreadContext().clearTraceContext()) {
                    fetchNodeStats();
                }
            } else {
                logger.trace("skipping collecting node stats from cluster, notifying listeners with empty node stats");
                leastAvailableSpaceUsages = Map.of();
                mostAvailableSpaceUsages = Map.of();
                maxHeapPerNode = Map.of();
            }
        }

        private void maybeFetchNodesEstimatedHeapUsage(boolean shouldFetch) {
            if (shouldFetch) {
                try (var ignored = threadPool.getThreadContext().clearTraceContext()) {
                    fetchNodesEstimatedHeapUsage();
                }
            } else {
                logger.trace("skipping collecting estimated heap usage from cluster, notifying listeners with empty estimated heap usage");
                estimatedHeapUsagePerNode = Map.of();
            }
        }

        private void maybeFetchNodesUsageStatsForThreadPools(WriteLoadDeciderStatus writeLoadConstraintEnabled) {
            if (writeLoadConstraintEnabled != WriteLoadDeciderStatus.DISABLED) {
                try (var ignored = threadPool.getThreadContext().clearTraceContext()) {
                    fetchNodesUsageStatsForThreadPools();
                }
            } else {
                logger.trace("skipping collecting shard/node write load estimates from cluster, feature currently disabled");
                nodeThreadPoolUsageStatsPerNode = Map.of();
            }
        }

        private void fetchNodesUsageStatsForThreadPools() {
            nodeUsageStatsForThreadPoolsCollector.collectUsageStats(
                client,
                clusterStateSupplier.get(),
                ActionListener.releaseAfter(new ActionListener<>() {
                    @Override
                    public void onResponse(Map<String, NodeUsageStatsForThreadPools> threadPoolStats) {
                        nodeThreadPoolUsageStatsPerNode = threadPoolStats;
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("failed to fetch thread pool usage estimates for nodes", e);
                        nodeThreadPoolUsageStatsPerNode = Map.of();
                    }
                }, fetchRefs.acquire())
            );
        }

        private void fetchNodesEstimatedHeapUsage() {
            estimatedHeapUsageCollector.collectClusterHeapUsage(ActionListener.releaseAfter(new ActionListener<>() {
                @Override
                public void onResponse(Map<String, Long> currentEstimatedHeapUsages) {
                    estimatedHeapUsagePerNode = currentEstimatedHeapUsages;
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to fetch heap usage for nodes", e);
                    estimatedHeapUsagePerNode = Map.of();
                }
            }, fetchRefs.acquire()));
        }

        private void fetchIndicesStats() {
            final IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.clear();
            if (diskThresholdEnabled) {
                // This returns the shard sizes on disk
                indicesStatsRequest.store(true);
            }
            if (writeLoadConstraintEnabled == WriteLoadDeciderStatus.ENABLED) {
                // This returns the shard write-loads
                indicesStatsRequest.indexing(true);
            }
            indicesStatsRequest.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_CLOSED_HIDDEN);
            indicesStatsRequest.timeout(fetchTimeout);
            client.admin()
                .indices()
                .stats(
                    indicesStatsRequest,
                    new ThreadedActionListener<>(
                        threadPool.executor(ThreadPool.Names.MANAGEMENT),
                        ActionListener.releaseAfter(new ActionListener<>() {
                            @Override
                            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                                logger.trace("received indices stats response");

                                if (indicesStatsResponse.getShardFailures().length > 0) {
                                    final Set<String> failedNodeIds = new HashSet<>();
                                    for (final var shardFailure : indicesStatsResponse.getShardFailures()) {
                                        if (shardFailure.getCause() instanceof final FailedNodeException failedNodeException) {
                                            if (failedNodeIds.add(failedNodeException.nodeId())) {
                                                logger.warn(
                                                    () -> format(
                                                        "failed to retrieve shard stats from node [%s]",
                                                        failedNodeException.nodeId()
                                                    ),
                                                    failedNodeException.getCause()
                                                );
                                            }
                                            logger.trace(
                                                () -> format(
                                                    "failed to retrieve stats for shard [%s][%s]",
                                                    shardFailure.index(),
                                                    shardFailure.shardId()
                                                ),
                                                shardFailure.getCause()
                                            );
                                        } else {
                                            logger.warn(
                                                () -> format(
                                                    "failed to retrieve stats for shard [%s][%s]",
                                                    shardFailure.index(),
                                                    shardFailure.shardId()
                                                ),
                                                shardFailure.getCause()
                                            );
                                        }
                                    }
                                }

                                final ShardStats[] stats = indicesStatsResponse.getShards();
                                final Map<ShardId, Double> shardWriteLoadByIdentifierBuilder = new HashMap<>();
                                final Map<String, Long> shardSizeByIdentifierBuilder = new HashMap<>();
                                final Map<ShardId, Long> shardDataSetSizeBuilder = new HashMap<>();
                                final Map<ClusterInfo.NodeAndShard, String> dataPath = new HashMap<>();
                                final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace.Builder> reservedSpaceBuilders =
                                    new HashMap<>();
                                buildShardLevelInfo(
                                    adjustShardStats(stats),
                                    shardWriteLoadByIdentifierBuilder,
                                    shardSizeByIdentifierBuilder,
                                    shardDataSetSizeBuilder,
                                    dataPath,
                                    reservedSpaceBuilders
                                );

                                final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace = new HashMap<>();
                                reservedSpaceBuilders.forEach((nodeAndPath, builder) -> reservedSpace.put(nodeAndPath, builder.build()));

                                indicesStatsSummary = new IndicesStatsSummary(
                                    Map.copyOf(shardSizeByIdentifierBuilder),
                                    Map.copyOf(shardDataSetSizeBuilder),
                                    Map.copyOf(dataPath),
                                    Map.copyOf(reservedSpace),
                                    Map.copyOf(shardWriteLoadByIdentifierBuilder)
                                );
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
                        }, fetchRefs.acquire())
                    )
                );
        }

        private void fetchNodeStats() {
            final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
            nodesStatsRequest.setIncludeShardsStats(false);
            nodesStatsRequest.clear();
            nodesStatsRequest.addMetric(NodesStatsRequestParameters.Metric.FS);
            nodesStatsRequest.addMetric(NodesStatsRequestParameters.Metric.JVM);
            nodesStatsRequest.setTimeout(fetchTimeout);
            client.admin().cluster().nodesStats(nodesStatsRequest, ActionListener.releaseAfter(new ActionListener<>() {
                @Override
                public void onResponse(NodesStatsResponse nodesStatsResponse) {
                    logger.trace("received node stats response");

                    for (final FailedNodeException failure : nodesStatsResponse.failures()) {
                        logger.warn(() -> "failed to retrieve stats for node [" + failure.nodeId() + "]", failure.getCause());
                    }

                    Map<String, DiskUsage> leastAvailableUsagesBuilder = new HashMap<>();
                    Map<String, DiskUsage> mostAvailableUsagesBuilder = new HashMap<>();
                    Map<String, ByteSizeValue> maxHeapPerNodeBuilder = new HashMap<>();
                    processNodeStatsArray(
                        adjustNodesStats(nodesStatsResponse.getNodes()),
                        leastAvailableUsagesBuilder,
                        mostAvailableUsagesBuilder,
                        maxHeapPerNodeBuilder
                    );
                    leastAvailableSpaceUsages = Map.copyOf(leastAvailableUsagesBuilder);
                    mostAvailableSpaceUsages = Map.copyOf(mostAvailableUsagesBuilder);
                    maxHeapPerNode = Map.copyOf(maxHeapPerNodeBuilder);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ClusterBlockException) {
                        logger.trace("failed to retrieve node stats", e);
                    } else {
                        logger.warn("failed to retrieve node stats", e);
                    }
                    leastAvailableSpaceUsages = Map.of();
                    mostAvailableSpaceUsages = Map.of();
                    maxHeapPerNode = Map.of();
                }
            }, fetchRefs.acquire()));
        }

        private void callListeners() {
            try {
                logger.trace("stats all received, computing cluster info and notifying listeners");
                final ClusterInfo clusterInfo = getClusterInfo();
                boolean anyListeners = false;
                for (final Consumer<ClusterInfo> listener : listeners) {
                    anyListeners = true;
                    try {
                        logger.trace("notifying [{}] of new cluster info", listener);
                        listener.accept(clusterInfo);
                    } catch (Exception e) {
                        logger.info(() -> "failed to notify [" + listener + "] of new cluster info", e);
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

        currentRefresh = new AsyncRefresh(thisRefreshListeners);
        return currentRefresh::execute;
    }

    private boolean assertRefreshInvariant() {
        assert Thread.holdsLock(mutex) : "mutex not held";
        // We never leave a refresh listener waiting unless we're already refreshing (which will pick up the waiting listener on completion)
        assert nextRefreshListeners.isEmpty() || currentRefresh != null;
        return true;
    }

    private class RefreshScheduler {

        ActionListener<ClusterInfo> getListener() {
            return ActionListener.running(() -> {
                if (shouldRefresh()) {
                    threadPool.scheduleUnlessShuttingDown(updateFrequency, EsExecutors.DIRECT_EXECUTOR_SERVICE, () -> {
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
        final Map<String, EstimatedHeapUsage> estimatedHeapUsages = new HashMap<>();
        maxHeapPerNode.forEach((nodeId, maxHeapSize) -> {
            final Long estimatedHeapUsage = estimatedHeapUsagePerNode.get(nodeId);
            if (estimatedHeapUsage != null) {
                estimatedHeapUsages.put(nodeId, new EstimatedHeapUsage(nodeId, maxHeapSize.getBytes(), estimatedHeapUsage));
            }
        });
        return new ClusterInfo(
            leastAvailableSpaceUsages,
            mostAvailableSpaceUsages,
            indicesStatsSummary.shardSizes,
            indicesStatsSummary.shardDataSetSizes,
            indicesStatsSummary.dataPath,
            indicesStatsSummary.reservedSpace,
            estimatedHeapUsages,
            nodeThreadPoolUsageStatsPerNode,
            indicesStatsSummary.shardWriteLoads()
        );
    }

    // allow tests to adjust the node stats on receipt
    List<NodeStats> adjustNodesStats(List<NodeStats> nodeStats) {
        return nodeStats;
    }

    ShardStats[] adjustShardStats(ShardStats[] shardStats) {
        return shardStats;
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

    static void buildShardLevelInfo(
        ShardStats[] stats,
        Map<ShardId, Double> shardWriteLoads,
        Map<String, Long> shardSizes,
        Map<ShardId, Long> shardDataSetSizeBuilder,
        Map<ClusterInfo.NodeAndShard, String> dataPathByShard,
        Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace.Builder> reservedSpaceByShard
    ) {
        for (ShardStats s : stats) {
            final ShardRouting shardRouting = s.getShardRouting();
            dataPathByShard.put(ClusterInfo.NodeAndShard.from(shardRouting), s.getDataPath());

            final StoreStats storeStats = s.getStats().getStore();
            if (storeStats != null) {
                final long size = storeStats.sizeInBytes();
                final long dataSetSize = storeStats.totalDataSetSizeInBytes();
                final long reserved = storeStats.reservedSizeInBytes();

                final String shardIdentifier = ClusterInfo.shardIdentifierFromRouting(shardRouting);
                logger.trace("shard: {} size: {} reserved: {}", shardIdentifier, size, reserved);
                shardSizes.put(shardIdentifier, size);
                if (dataSetSize > shardDataSetSizeBuilder.getOrDefault(shardRouting.shardId(), -1L)) {
                    shardDataSetSizeBuilder.put(shardRouting.shardId(), dataSetSize);
                }
                if (reserved != StoreStats.UNKNOWN_RESERVED_BYTES) {
                    final ClusterInfo.ReservedSpace.Builder reservedSpaceBuilder = reservedSpaceByShard.computeIfAbsent(
                        new ClusterInfo.NodeAndPath(shardRouting.currentNodeId(), s.getDataPath()),
                        t -> new ClusterInfo.ReservedSpace.Builder()
                    );
                    reservedSpaceBuilder.add(shardRouting.shardId(), reserved);
                }
            }
            final IndexingStats indexingStats = s.getStats().getIndexing();
            if (indexingStats != null) {
                final double shardWriteLoad = indexingStats.getTotal().getPeakWriteLoad();
                if (shardWriteLoad > shardWriteLoads.getOrDefault(shardRouting.shardId(), -1.0)) {
                    shardWriteLoads.put(shardRouting.shardId(), shardWriteLoad);
                }
            }
        }
    }

    private static void processNodeStatsArray(
        List<NodeStats> nodeStatsArray,
        Map<String, DiskUsage> newLeastAvailableUsages,
        Map<String, DiskUsage> newMostAvailableUsages,
        Map<String, ByteSizeValue> maxHeapPerNodeBuilder
    ) {
        for (NodeStats nodeStats : nodeStatsArray) {
            DiskUsage leastAvailableUsage = DiskUsage.findLeastAvailablePath(nodeStats);
            if (leastAvailableUsage != null) {
                newLeastAvailableUsages.put(nodeStats.getNode().getId(), leastAvailableUsage);
            }
            DiskUsage mostAvailableUsage = DiskUsage.findMostAvailable(nodeStats);
            if (mostAvailableUsage != null) {
                newMostAvailableUsages.put(nodeStats.getNode().getId(), mostAvailableUsage);
            }
            maxHeapPerNodeBuilder.put(nodeStats.getNode().getId(), nodeStats.getJvm().getMem().getHeapMax());
        }
    }

    private record IndicesStatsSummary(
        Map<String, Long> shardSizes,
        Map<ShardId, Long> shardDataSetSizes,
        Map<ClusterInfo.NodeAndShard, String> dataPath,
        Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace,
        Map<ShardId, Double> shardWriteLoads
    ) {
        static final IndicesStatsSummary EMPTY = new IndicesStatsSummary(Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

}
