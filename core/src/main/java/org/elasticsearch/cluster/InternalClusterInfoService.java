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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
public class InternalClusterInfoService extends AbstractComponent implements ClusterInfoService, LocalNodeMasterListener, ClusterStateListener {

    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING =
        Setting.timeSetting("cluster.info.update.interval", TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(10),
            Property.Dynamic, Property.NodeScope);
    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("cluster.info.update.timeout", TimeValue.timeValueSeconds(15),
            Property.Dynamic, Property.NodeScope);

    private volatile TimeValue updateFrequency;

    private volatile ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsages;
    private volatile ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsages;
    private volatile ImmutableOpenMap<ShardRouting, String> shardRoutingToDataPath;
    private volatile ImmutableOpenMap<String, Long> shardSizes;
    private volatile boolean isMaster = false;
    private volatile boolean enabled;
    private volatile TimeValue fetchTimeout;
    private final TransportNodesStatsAction transportNodesStatsAction;
    private final TransportIndicesStatsAction transportIndicesStatsAction;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    @Inject
    public InternalClusterInfoService(Settings settings, ClusterSettings clusterSettings,
                                      TransportNodesStatsAction transportNodesStatsAction,
                                      TransportIndicesStatsAction transportIndicesStatsAction, ClusterService clusterService,
                                      ThreadPool threadPool) {
        super(settings);
        this.leastAvailableSpaceUsages = ImmutableOpenMap.of();
        this.mostAvailableSpaceUsages = ImmutableOpenMap.of();
        this.shardRoutingToDataPath = ImmutableOpenMap.of();
        this.shardSizes = ImmutableOpenMap.of();
        this.transportNodesStatsAction = transportNodesStatsAction;
        this.transportIndicesStatsAction = transportIndicesStatsAction;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.updateFrequency = INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings);
        this.fetchTimeout = INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.get(settings);
        this.enabled = DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING, this::setFetchTimeout);
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING, this::setUpdateFrequency);
        clusterSettings.addSettingsUpdateConsumer(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING, this::setEnabled);

        // Add InternalClusterInfoService to listen for Master changes
        this.clusterService.add((LocalNodeMasterListener)this);
        // Add to listen for state changes (when nodes are added)
        this.clusterService.add((ClusterStateListener)this);
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
    public void onMaster() {
        this.isMaster = true;
        if (logger.isTraceEnabled()) {
            logger.trace("I have been elected master, scheduling a ClusterInfoUpdateJob");
        }
        try {
            // Submit a job that will start after DEFAULT_STARTING_INTERVAL, and reschedule itself after running
            threadPool.schedule(updateFrequency, executorName(), new SubmitReschedulingClusterInfoUpdatedJob());
            if (clusterService.state().getNodes().getDataNodes().size() > 1) {
                // Submit an info update job to be run immediately
                threadPool.executor(executorName()).execute(() -> maybeRefresh());
            }
        } catch (EsRejectedExecutionException ex) {
            if (logger.isDebugEnabled()) {
                logger.debug("Couldn't schedule cluster info update task - node might be shutting down", ex);
            }
        }
    }

    @Override
    public void offMaster() {
        this.isMaster = false;
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!this.enabled) {
            return;
        }

        // Check whether it was a data node that was added
        boolean dataNodeAdded = false;
        for (DiscoveryNode addedNode : event.nodesDelta().addedNodes()) {
            if (addedNode.isDataNode()) {
                dataNodeAdded = true;
                break;
            }
        }

        if (this.isMaster && dataNodeAdded && clusterService.state().getNodes().getDataNodes().size() > 1) {
            if (logger.isDebugEnabled()) {
                logger.debug("data node was added, retrieving new cluster info");
            }
            threadPool.executor(executorName()).execute(() -> maybeRefresh());
        }

        if (this.isMaster && event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                if (removedNode.isDataNode()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Removing node from cluster info: {}", removedNode.getId());
                    }
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
    }

    @Override
    public ClusterInfo getClusterInfo() {
        return new ClusterInfo(leastAvailableSpaceUsages, mostAvailableSpaceUsages, shardSizes, shardRoutingToDataPath);
    }

    @Override
    public void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Class used to submit {@link #maybeRefresh()} on the
     * {@link InternalClusterInfoService} threadpool, these jobs will
     * reschedule themselves by placing a new instance of this class onto the
     * scheduled threadpool.
     */
    public class SubmitReschedulingClusterInfoUpdatedJob implements Runnable {
        @Override
        public void run() {
            if (logger.isTraceEnabled()) {
                logger.trace("Submitting new rescheduling cluster info update job");
            }
            try {
                threadPool.executor(executorName()).execute(() -> {
                    try {
                        maybeRefresh();
                    } finally { //schedule again after we refreshed
                        if (isMaster) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Scheduling next run for updating cluster info in: {}", updateFrequency.toString());
                            }
                            try {
                                threadPool.schedule(updateFrequency, executorName(), this);
                            } catch (EsRejectedExecutionException ex) {
                                logger.debug("Reschedule cluster info service was rejected", ex);
                            }
                        }
                    }
                });
            } catch (EsRejectedExecutionException ex) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Couldn't re-schedule cluster info update task - node might be shutting down", ex);
                }
            }
        }
    }

    /**
     * Retrieve the latest nodes stats, calling the listener when complete
     * @return a latch that can be used to wait for the nodes stats to complete if desired
     */
    protected CountDownLatch updateNodeStats(final ActionListener<NodesStatsResponse> listener) {
        final CountDownLatch latch = new CountDownLatch(1);
        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
        nodesStatsRequest.clear();
        nodesStatsRequest.fs(true);
        nodesStatsRequest.timeout(fetchTimeout);

        transportNodesStatsAction.execute(nodesStatsRequest, new LatchedActionListener<>(listener, latch));
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

        transportIndicesStatsAction.execute(indicesStatsRequest, new LatchedActionListener<>(listener, latch));
        return latch;
    }

    private final void maybeRefresh() {
        // Short-circuit if not enabled
        if (enabled) {
            refresh();
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Skipping ClusterInfoUpdatedJob since it is disabled");
            }
        }
    }

    /**
     * Refreshes the ClusterInfo in a blocking fashion
     */
    public final ClusterInfo refresh() {
        if (logger.isTraceEnabled()) {
            logger.trace("Performing ClusterInfoUpdateJob");
        }
        final CountDownLatch nodeLatch = updateNodeStats(new ActionListener<NodesStatsResponse>() {
            @Override
            public void onResponse(NodesStatsResponse nodeStatses) {
                ImmutableOpenMap.Builder<String, DiskUsage> newLeastAvaiableUsages = ImmutableOpenMap.builder();
                ImmutableOpenMap.Builder<String, DiskUsage> newMostAvaiableUsages = ImmutableOpenMap.builder();
                fillDiskUsagePerNode(logger, nodeStatses.getNodes(), newLeastAvaiableUsages, newMostAvaiableUsages);
                leastAvailableSpaceUsages = newLeastAvaiableUsages.build();
                mostAvailableSpaceUsages = newMostAvaiableUsages.build();
            }

            @Override
            public void onFailure(Throwable e) {
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
                ShardStats[] stats = indicesStatsResponse.getShards();
                ImmutableOpenMap.Builder<String, Long> newShardSizes = ImmutableOpenMap.builder();
                ImmutableOpenMap.Builder<ShardRouting, String> newShardRoutingToDataPath = ImmutableOpenMap.builder();
                buildShardLevelInfo(logger, stats, newShardSizes, newShardRoutingToDataPath, clusterService.state());
                shardSizes = newShardSizes.build();
                shardRoutingToDataPath = newShardRoutingToDataPath.build();
            }

            @Override
            public void onFailure(Throwable e) {
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
                    shardSizes = ImmutableOpenMap.of();
                    shardRoutingToDataPath = ImmutableOpenMap.of();
                }
            }
        });

        try {
            nodeLatch.await(fetchTimeout.getMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt status
            logger.warn("Failed to update node information for ClusterInfoUpdateJob within {} timeout", fetchTimeout);
        }

        try {
            indicesLatch.await(fetchTimeout.getMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt status
            logger.warn("Failed to update shard information for ClusterInfoUpdateJob within {} timeout", fetchTimeout);
        }
        ClusterInfo clusterInfo = getClusterInfo();
        for (Listener l : listeners) {
            try {
                l.onNewInfo(clusterInfo);
            } catch (Exception e) {
                logger.info("Failed executing ClusterInfoService listener", e);
            }
        }
        return clusterInfo;
    }

    static void buildShardLevelInfo(ESLogger logger, ShardStats[] stats, ImmutableOpenMap.Builder<String, Long> newShardSizes,
                                    ImmutableOpenMap.Builder<ShardRouting, String> newShardRoutingToDataPath, ClusterState state) {
        MetaData meta = state.getMetaData();
        for (ShardStats s : stats) {
            IndexMetaData indexMeta = meta.index(s.getShardRouting().index());
            Settings indexSettings = indexMeta == null ? null : indexMeta.getSettings();
            newShardRoutingToDataPath.put(s.getShardRouting(), s.getDataPath());
            long size = s.getStats().getStore().sizeInBytes();
            String sid = ClusterInfo.shardIdentifierFromRouting(s.getShardRouting());
            if (logger.isTraceEnabled()) {
                logger.trace("shard: {} size: {}", sid, size);
            }
            if (indexSettings != null && IndexMetaData.isIndexUsingShadowReplicas(indexSettings)) {
                // Shards on a shared filesystem should be considered of size 0
                if (logger.isTraceEnabled()) {
                    logger.trace("shard: {} is using shadow replicas and will be treated as size 0", sid);
                }
                size = 0;
            }
            newShardSizes.put(sid, size);
        }
    }

    static void fillDiskUsagePerNode(ESLogger logger, List<NodeStats> nodeStatsArray,
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
                    } else if (leastAvailablePath.getAvailable().bytes() > info.getAvailable().bytes()){
                        leastAvailablePath = info;
                    } else if (mostAvailablePath.getAvailable().bytes() < info.getAvailable().bytes()) {
                        mostAvailablePath = info;
                    }
                }
                String nodeId = nodeStats.getNode().getId();
                String nodeName = nodeStats.getNode().getName();
                if (logger.isTraceEnabled()) {
                    logger.trace("node: [{}], most available: total disk: {}, available disk: {} / least available: total disk: {}, available disk: {}",
                            nodeId, mostAvailablePath.getTotal(), leastAvailablePath.getAvailable(),
                            leastAvailablePath.getTotal(), leastAvailablePath.getAvailable());
                }
                if (leastAvailablePath.getTotal().bytes() < 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("node: [{}] least available path has less than 0 total bytes of disk [{}], skipping",
                                nodeId, leastAvailablePath.getTotal().bytes());
                    }
                } else {
                    newLeastAvaiableUsages.put(nodeId, new DiskUsage(nodeId, nodeName, leastAvailablePath.getPath(), leastAvailablePath.getTotal().bytes(), leastAvailablePath.getAvailable().bytes()));
                }
                if (mostAvailablePath.getTotal().bytes() < 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("node: [{}] most available path has less than 0 total bytes of disk [{}], skipping",
                                nodeId, mostAvailablePath.getTotal().bytes());
                    }
                } else {
                    newMostAvaiableUsages.put(nodeId, new DiskUsage(nodeId, nodeName, mostAvailablePath.getPath(), mostAvailablePath.getTotal().bytes(), mostAvailablePath.getAvailable().bytes()));
                }

            }
        }
    }


}
