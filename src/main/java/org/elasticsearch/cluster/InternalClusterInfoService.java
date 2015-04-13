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

import com.google.common.collect.ImmutableMap;
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
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.util.*;
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

    public static final String INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL = "cluster.info.update.interval";
    public static final String INTERNAL_CLUSTER_INFO_TIMEOUT = "cluster.info.update.timeout";

    private volatile TimeValue updateFrequency;

    private volatile ImmutableMap<String, DiskUsage> usages;
    private volatile ImmutableMap<String, Long> shardSizes;
    private volatile boolean isMaster = false;
    private volatile boolean enabled;
    private volatile TimeValue fetchTimeout;
    private final TransportNodesStatsAction transportNodesStatsAction;
    private final TransportIndicesStatsAction transportIndicesStatsAction;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Set<Listener> listeners = Collections.synchronizedSet(new HashSet<Listener>());

    @Inject
    public InternalClusterInfoService(Settings settings, NodeSettingsService nodeSettingsService,
                                      TransportNodesStatsAction transportNodesStatsAction,
                                      TransportIndicesStatsAction transportIndicesStatsAction, ClusterService clusterService,
                                      ThreadPool threadPool) {
        super(settings);
        this.usages = ImmutableMap.of();
        this.shardSizes = ImmutableMap.of();
        this.transportNodesStatsAction = transportNodesStatsAction;
        this.transportIndicesStatsAction = transportIndicesStatsAction;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.updateFrequency = settings.getAsTime(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, TimeValue.timeValueSeconds(30));
        this.fetchTimeout = settings.getAsTime(INTERNAL_CLUSTER_INFO_TIMEOUT, TimeValue.timeValueSeconds(15));
        this.enabled = settings.getAsBoolean(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, true);
        nodeSettingsService.addListener(new ApplySettings());

        // Add InternalClusterInfoService to listen for Master changes
        this.clusterService.add((LocalNodeMasterListener)this);
        // Add to listen for state changes (when nodes are added)
        this.clusterService.add((ClusterStateListener)this);
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            TimeValue newUpdateFrequency = settings.getAsTime(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, null);
            // ClusterInfoService is only enabled if the DiskThresholdDecider is enabled
            Boolean newEnabled = settings.getAsBoolean(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, null);

            if (newUpdateFrequency != null) {
                if (newUpdateFrequency.getMillis() < TimeValue.timeValueSeconds(10).getMillis()) {
                    logger.warn("[{}] set too low [{}] (< 10s)", INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, newUpdateFrequency);
                    throw new IllegalStateException("Unable to set " + INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL + " less than 10 seconds");
                } else {
                    logger.info("updating [{}] from [{}] to [{}]", INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, updateFrequency, newUpdateFrequency);
                    InternalClusterInfoService.this.updateFrequency = newUpdateFrequency;
                }
            }

            TimeValue newFetchTimeout = settings.getAsTime(INTERNAL_CLUSTER_INFO_TIMEOUT, null);
            if (newFetchTimeout != null) {
                logger.info("updating fetch timeout [{}] from [{}] to [{}]", INTERNAL_CLUSTER_INFO_TIMEOUT, fetchTimeout, newFetchTimeout);
                InternalClusterInfoService.this.fetchTimeout = newFetchTimeout;
            }


            // We don't log about enabling it here, because the DiskThresholdDecider will already be logging about enable/disable
            if (newEnabled != null) {
                InternalClusterInfoService.this.enabled = newEnabled;
            }
        }
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
                updateOnce();
            }
        } catch (EsRejectedExecutionException ex) {
            if (logger.isDebugEnabled()) {
                logger.debug("Couldn't schedule cluster info update task - node might be shutting down", ex);
            }
        }
    }


    // called from tests as well

    /**
     * will collect a fresh {@link ClusterInfo} from the nodes, without scheduling a future collection
     */
    void updateOnce() {
        threadPool.executor(executorName()).execute(new ClusterInfoUpdateJob(false));
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
            if (addedNode.dataNode()) {
                dataNodeAdded = true;
                break;
            }
        }

        if (this.isMaster && dataNodeAdded && clusterService.state().getNodes().getDataNodes().size() > 1) {
            if (logger.isDebugEnabled()) {
                logger.debug("data node was added, retrieving new cluster info");
            }
            updateOnce();
        }

        if (this.isMaster && event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                if (removedNode.dataNode()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Removing node from cluster info: {}", removedNode.getId());
                    }
                    Map<String, DiskUsage> newUsages = new HashMap<>(usages);
                    newUsages.remove(removedNode.getId());
                    usages = ImmutableMap.copyOf(newUsages);
                }
            }
        }
    }

    @Override
    public ClusterInfo getClusterInfo() {
        return new ClusterInfo(usages, shardSizes);
    }

    @Override
    public void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Class used to submit {@link ClusterInfoUpdateJob}s on the
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
                threadPool.executor(executorName()).execute(new ClusterInfoUpdateJob(true));
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

    /**
     * Runnable class that performs a {@Link NodesStatsRequest} to retrieve
     * disk usages for nodes in the cluster and an {@link IndicesStatsRequest}
     * to retrieve the sizes of all shards to ensure they can fit on nodes
     * during shard balancing.
     */
    public class ClusterInfoUpdateJob implements Runnable {

        // This boolean is used to signal to the ClusterInfoUpdateJob that it
        // needs to reschedule itself to run again at a later time. It can be
        // set to false to only run once
        private final boolean reschedule;

        public ClusterInfoUpdateJob(boolean reschedule) {
            this.reschedule = reschedule;
        }

        @Override
        public void run() {
            if (logger.isTraceEnabled()) {
                logger.trace("Performing ClusterInfoUpdateJob");
            }

            if (isMaster && this.reschedule) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Scheduling next run for updating cluster info in: {}", updateFrequency.toString());
                }
                try {
                    threadPool.schedule(updateFrequency, executorName(), new SubmitReschedulingClusterInfoUpdatedJob());
                } catch (EsRejectedExecutionException ex) {
                    logger.debug("Reschedule cluster info service was rejected", ex);
                }
            }
            if (!enabled) {
                // Short-circuit if not enabled
                if (logger.isTraceEnabled()) {
                    logger.trace("Skipping ClusterInfoUpdatedJob since it is disabled");
                }
                return;
            }

            CountDownLatch nodeLatch = updateNodeStats(new ActionListener<NodesStatsResponse>() {
                @Override
                public void onResponse(NodesStatsResponse nodeStatses) {
                    Map<String, DiskUsage> newUsages = new HashMap<>();
                    for (NodeStats nodeStats : nodeStatses.getNodes()) {
                        if (nodeStats.getFs() == null) {
                            logger.warn("Unable to retrieve node FS stats for {}", nodeStats.getNode().name());
                        } else {
                            long available = 0;
                            long total = 0;

                            for (FsStats.Info info : nodeStats.getFs()) {
                                available += info.getAvailable().bytes();
                                total += info.getTotal().bytes();
                            }
                            String nodeId = nodeStats.getNode().id();
                            String nodeName = nodeStats.getNode().getName();
                            if (logger.isTraceEnabled()) {
                                logger.trace("node: [{}], total disk: {}, available disk: {}", nodeId, total, available);
                            }
                            newUsages.put(nodeId, new DiskUsage(nodeId, nodeName, total, available));
                        }
                    }
                    usages = ImmutableMap.copyOf(newUsages);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (e instanceof ReceiveTimeoutTransportException) {
                        logger.error("NodeStatsAction timed out for ClusterInfoUpdateJob (reason [{}])", e.getMessage());
                    } else {
                        if (e instanceof ClusterBlockException) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Failed to execute NodeStatsAction for ClusterInfoUpdateJob", e);
                            }
                        } else {
                            logger.warn("Failed to execute NodeStatsAction for ClusterInfoUpdateJob", e);
                        }
                        // we empty the usages list, to be safe - we don't know what's going on.
                        usages = ImmutableMap.of();
                    }
                }
            });

            CountDownLatch indicesLatch = updateIndicesStats(new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                    ShardStats[] stats = indicesStatsResponse.getShards();
                    HashMap<String, Long> newShardSizes = new HashMap<>();
                    for (ShardStats s : stats) {
                        long size = s.getStats().getStore().sizeInBytes();
                        String sid = shardIdentifierFromRouting(s.getShardRouting());
                        if (logger.isTraceEnabled()) {
                            logger.trace("shard: {} size: {}", sid, size);
                        }
                        newShardSizes.put(sid, size);
                    }
                    shardSizes = ImmutableMap.copyOf(newShardSizes);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (e instanceof ReceiveTimeoutTransportException) {
                        logger.error("IndicesStatsAction timed out for ClusterInfoUpdateJob (reason [{}])", e.getMessage());
                    } else {
                        if (e instanceof ClusterBlockException) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Failed to execute IndicesStatsAction for ClusterInfoUpdateJob", e);
                            }
                        } else {
                            logger.warn("Failed to execute IndicesStatsAction for ClusterInfoUpdateJob", e);
                        }
                        // we empty the usages list, to be safe - we don't know what's going on.
                        shardSizes = ImmutableMap.of();
                    }
                }
            });

            try {
                nodeLatch.await(fetchTimeout.getMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("Failed to update node information for ClusterInfoUpdateJob within 15s timeout");
            }

            try {
                indicesLatch.await(fetchTimeout.getMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("Failed to update shard information for ClusterInfoUpdateJob within 15s timeout");
            }

            for (Listener l : listeners) {
                try {
                    l.onNewInfo(getClusterInfo());
                } catch (Exception e) {
                    logger.info("Failed executing ClusterInfoService listener", e);
                }
            }
        }
    }

    /**
     * Method that incorporates the ShardId for the shard into a string that
     * includes a 'p' or 'r' depending on whether the shard is a primary.
     */
    public static String shardIdentifierFromRouting(ShardRouting shardRouting) {
        return shardRouting.shardId().toString() + "[" + (shardRouting.primary() ? "p" : "r") + "]";
    }
}
