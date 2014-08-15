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
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.ClusterInfo.ShardsBucketedBySize;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
public final class InternalClusterInfoService extends AbstractComponent implements ClusterInfoService, LocalNodeMasterListener, ClusterStateListener {
    // Cluster settings
    public static final String INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL = "cluster.info.update.interval";
    public static final String SMALL_SHARD_LIMIT = "cluster.info.small_shard_size";

    // Index settings
    public static final String INDEX_SETTING_MINIMUM_SHARD_SIZE = "index.minimum_shard_size";

    // Cluster settings defaults
    public static final ByteSizeValue DEFAULT_SMALL_SHARD_LIMIT = ByteSizeValue.parseBytesSizeValue("10mb");

    private volatile TimeValue updateFrequency;

    private volatile ImmutableMap<String, DiskUsage> usages;
    private volatile ImmutableMap<String, Long> shardSizes;
    private volatile ImmutableMap<String, Long> indexToAverageShardSize;
    private volatile ShardsBucketedBySize shardsBucketedBySize;
    private volatile MetaData metaData;

    private volatile boolean isMaster = false;
    private final TransportNodesStatsAction transportNodesStatsAction;
    private final TransportIndicesStatsAction transportIndicesStatsAction;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    /**
     * Max size in bytes that a shard can be and still be considered "small".
     * All "small" shards are binned together logShardSize and thus by the shard
     * size allocation function.
     */
    private ByteSizeValue smallShardLimit;

    @Inject
    public InternalClusterInfoService(Settings settings, NodeSettingsService nodeSettingsService,
                                      TransportNodesStatsAction transportNodesStatsAction,
                                      TransportIndicesStatsAction transportIndicesStatsAction, ClusterService clusterService,
                                      ThreadPool threadPool) {
        super(settings);
        this.transportNodesStatsAction = transportNodesStatsAction;
        this.transportIndicesStatsAction = transportIndicesStatsAction;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // The next two values get their configuration in the ApplySettings
        // class on startup
        this.updateFrequency = settings.getAsTime(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, TimeValue.timeValueSeconds(30));
        smallShardLimit = settings.getAsBytesSize(SMALL_SHARD_LIMIT, DEFAULT_SMALL_SHARD_LIMIT);
        this.usages = ImmutableMap.of();
        this.shardSizes = ImmutableMap.of();
        this.indexToAverageShardSize = ImmutableMap.of();
        this.shardsBucketedBySize = ShardsBucketedBySize.builder(smallShardLimit.bytes()).build();
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
            ByteSizeValue newSmallShardLimit = settings.getAsBytesSize(SMALL_SHARD_LIMIT, null);

            if (newUpdateFrequency != null) {
                if (newUpdateFrequency.getMillis() < TimeValue.timeValueSeconds(10).getMillis()) {
                    logger.warn("[{}] set too low [{}] (< 10s)", INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, newUpdateFrequency);
                    throw new IllegalStateException("Unable to set " + INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL + " less than 10 seconds");
                } else {
                    logger.info("updating [{}] from [{}] to [{}]", INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, updateFrequency, newUpdateFrequency);
                    InternalClusterInfoService.this.updateFrequency = newUpdateFrequency;
                }
            }
            if (newSmallShardLimit != null && !newSmallShardLimit.equals(smallShardLimit)) {
                logger.info("updating [{}] from [{}] to [{}]", SMALL_SHARD_LIMIT, smallShardLimit, newSmallShardLimit);
                smallShardLimit = newSmallShardLimit;
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
            // Submit a job that will start after DEFAULT_STARTING_INTERVAL, and
            // reschedule itself after running
            threadPool.schedule(updateFrequency, executorName(), new SubmitReschedulingClusterInfoUpdatedJob());
            if (clusterService.state().getNodes().getDataNodes().size() > 1) {
                // Submit an info update job to be run immediately
                threadPool.executor(executorName()).execute(new ClusterInfoUpdateJob(false));
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
        metaData = event.state().metaData();

        // Check whether it was a data node that was added
        boolean dataNodeAdded = false;
        for (DiscoveryNode addedNode : event.nodesDelta().addedNodes()) {
            if (addedNode.dataNode()) {
                dataNodeAdded = true;
                break;
            }
        }

        if (isMaster && dataNodeAdded && clusterService.state().getNodes().getDataNodes().size() > 1) {
            if (logger.isDebugEnabled()) {
                logger.debug("data node was added, retrieving new cluster info");
            }
            threadPool.executor(executorName()).execute(new ClusterInfoUpdateJob(false));
        }

        if (isMaster && event.nodesRemoved()) {
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
        return new ClusterInfo(usages, shardSizes, indexToAverageShardSize, shardsBucketedBySize);
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

            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
            nodesStatsRequest.clear();
            nodesStatsRequest.fs(true);
            nodesStatsRequest.timeout(TimeValue.timeValueSeconds(15));

            transportNodesStatsAction.execute(nodesStatsRequest, new ActionListener<NodesStatsResponse>() {
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
                            if (logger.isTraceEnabled()) {
                                logger.trace("node: [{}], total disk: {}, available disk: {}", nodeId, total, available);
                            }
                            newUsages.put(nodeId, new DiskUsage(nodeId, total, available));
                        }
                    }
                    usages = ImmutableMap.copyOf(newUsages);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (e instanceof ClusterBlockException) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Failed to execute NodeStatsAction for ClusterInfoUpdateJob", e);
                        }
                    } else {
                        logger.warn("Failed to execute NodeStatsAction for ClusterInfoUpdateJob", e);
                    }
                }
            });

            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.clear();
            indicesStatsRequest.store(true);
            transportIndicesStatsAction.execute(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                    updateIndicesStats(indicesStatsResponse);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (e instanceof ClusterBlockException) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Failed to execute IndicesStatsAction for ClusterInfoUpdateJob", e);
                        }
                    } else {
                        logger.warn("Failed to execute IndicesStatsAction for ClusterInfoUpdateJob", e);
                    }
                }
            });

            if (logger.isTraceEnabled()) {
                logger.trace("Finished ClusterInfoUpdateJob");
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

    void updateIndicesStats(IndicesStatsResponse indicesStatsResponse) {
        Map<String, AverageWork> averageShardSizesWork = new TreeMap<>();
        Map<String, Long> newShardSizes = new HashMap<>();
        ShardsBucketedBySize.Builder newShardsBucketedBySize = ShardsBucketedBySize.builder(smallShardLimit.bytes());
        for (ShardStats s : indicesStatsResponse.getShards()) {
            // Default to the configured "default" size if there is
            // one that isn't expired and is larger then the actual
            // size.
            long size = minimumShardSizeFromMetaData(s.getIndex());
            if (size < s.getStats().getStore().sizeInBytes()) {
                size = s.getStats().getStore().sizeInBytes();
            }
            String sid = shardIdentifierFromRouting(s.getShardRouting());
            if (logger.isTraceEnabled()) {
                logger.trace("shard: {} size: {}", sid, size);
            }
            newShardSizes.put(sid, size);
            newShardsBucketedBySize.add(size, sid);
            AverageWork avgWork = averageShardSizesWork.get(s.getIndex());
            if (avgWork == null) {
                avgWork = new AverageWork(size);
                averageShardSizesWork.put(s.getIndex(), avgWork);
            } else {
                avgWork.add(size);
            }
        }
        shardSizes = ImmutableMap.copyOf(newShardSizes);
        shardsBucketedBySize = newShardsBucketedBySize.build();

        ImmutableMap.Builder<String, Long> newIndexToAverageShardSize = ImmutableMap.builder();
        for (Map.Entry<String, AverageWork> entry : averageShardSizesWork.entrySet()) {
            // TODO support for newly created indexes returning the average shard size for a while?
            newIndexToAverageShardSize.put(entry.getKey(), entry.getValue().average());
        }
        indexToAverageShardSize = newIndexToAverageShardSize.build();
    }

    /**
     * Load the shard size from its metadata if it has any configured.
     * @param index index name
     * @return -1 if there isn't any shard size in the metadata.  0 or more if there was a size in the metadata.
     */
    private long minimumShardSizeFromMetaData(String index) {
        IndexMetaData meta = metaData.index(index);
        if (meta == null) {
            return -1;
        }
        ByteSizeValue minimumSize = meta.getSettings().getAsBytesSize(INDEX_SETTING_MINIMUM_SHARD_SIZE, null);
        if (minimumSize == null) {
            return -1;
        }
        return minimumSize.bytes();
    }

    /**
     * Holds work items for average calculations.
     */
    private class AverageWork {
        private long total;
        private int count;

        public AverageWork(long size) {
            total = size;
            count = 1;
        }

        public void add(long size) {
            total += size;
            count++;
        }

        public long average() {
            return total / count;
        }
    }
}
