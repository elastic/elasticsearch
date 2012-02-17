/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indices.store;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/**
 *
 */
public class IndicesStore extends AbstractComponent implements ClusterStateListener {

    private final NodeEnvironment nodeEnv;

    private final IndicesService indicesService;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final TimeValue danglingTimeout;

    private final Map<String, DanglingIndex> danglingIndices = ConcurrentCollections.newConcurrentMap();

    private final Object danglingMutex = new Object();

    static class DanglingIndex {
        public final String index;
        public final ScheduledFuture future;

        DanglingIndex(String index, ScheduledFuture future) {
            this.index = index;
            this.future = future;
        }
    }

    @Inject
    public IndicesStore(Settings settings, NodeEnvironment nodeEnv, IndicesService indicesService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        this.danglingTimeout = componentSettings.getAsTime("dangling_timeout", TimeValue.timeValueMinutes(2));

        clusterService.addLast(this);
    }

    public void close() {
        clusterService.remove(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.routingTableChanged()) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            return;
        }

        // when all shards are started within a shard replication group, delete an unallocated shard on this node
        RoutingTable routingTable = event.state().routingTable();
        for (IndexRoutingTable indexRoutingTable : routingTable) {
            IndexService indexService = indicesService.indexService(indexRoutingTable.index());
            if (indexService == null) {
                // we handle this later...
                continue;
            }
            // if the store is not persistent, don't bother trying to check if it can be deleted
            if (!indexService.store().persistent()) {
                continue;
            }
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                // if it has been created on this node, we don't want to delete it
                if (indexService.hasShard(indexShardRoutingTable.shardId().id())) {
                    continue;
                }
                if (!indexService.store().canDeleteUnallocated(indexShardRoutingTable.shardId())) {
                    continue;
                }
                // only delete an unallocated shard if all (other shards) are started
                if (indexShardRoutingTable.countWithState(ShardRoutingState.STARTED) == indexShardRoutingTable.size()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}] deleting unallocated shard", indexShardRoutingTable.shardId().index().name(), indexShardRoutingTable.shardId().id());
                    }
                    try {
                        indexService.store().deleteUnallocated(indexShardRoutingTable.shardId());
                    } catch (Exception e) {
                        logger.debug("[{}][{}] failed to delete unallocated shard, ignoring", e, indexShardRoutingTable.shardId().index().name(), indexShardRoutingTable.shardId().id());
                    }
                }
            }
        }

        // do the reverse, and delete dangling indices / shards that might remain on that node
        // this can happen when deleting a closed index, or when a node joins and it has deleted indices / shards
        if (nodeEnv.hasNodeFile()) {
            // delete unused shards for existing indices
            for (IndexRoutingTable indexRoutingTable : routingTable) {
                IndexService indexService = indicesService.indexService(indexRoutingTable.index());
                if (indexService != null) { // allocated, ignore this
                    continue;
                }
                for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                    boolean shardCanBeDeleted = true;
                    for (ShardRouting shardRouting : indexShardRoutingTable) {
                        // don't delete a shard that not all instances are active
                        if (!shardRouting.active()) {
                            shardCanBeDeleted = false;
                            break;
                        }
                        String localNodeId = clusterService.localNode().id();
                        // check if shard is active on the current node or is getting relocated to the our node
                        if (localNodeId.equals(shardRouting.currentNodeId()) || localNodeId.equals(shardRouting.relocatingNodeId())) {
                            // shard will be used locally - keep it
                            shardCanBeDeleted = false;
                            break;
                        }
                    }
                    if (shardCanBeDeleted) {
                        ShardId shardId = indexShardRoutingTable.shardId();
                        for (File shardLocation : nodeEnv.shardLocations(shardId)) {
                            if (shardLocation.exists()) {
                                logger.debug("[{}][{}] deleting shard that is no longer used", shardId.index().name(), shardId.id());
                                FileSystemUtils.deleteRecursively(shardLocation);
                            }
                        }
                    }
                }
            }

            if (danglingTimeout.millis() >= 0) {
                synchronized (danglingMutex) {
                    for (String danglingIndex : danglingIndices.keySet()) {
                        if (event.state().metaData().hasIndex(danglingIndex)) {
                            logger.debug("[{}] no longer dangling (created), removing", danglingIndex);
                            DanglingIndex removed = danglingIndices.remove(danglingIndex);
                            removed.future.cancel(false);
                        }
                    }
                    // delete indices that are no longer part of the metadata
                    try {
                        for (String indexName : nodeEnv.findAllIndices()) {
                            // if we have the index on the metadata, don't delete it
                            if (event.state().metaData().hasIndex(indexName)) {
                                continue;
                            }
                            if (danglingIndices.containsKey(indexName)) {
                                // already dangling, continue
                                continue;
                            }
                            if (danglingTimeout.millis() == 0) {
                                logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, timeout set to 0, deleting now", indexName);
                                FileSystemUtils.deleteRecursively(nodeEnv.indexLocations(new Index(indexName)));
                            } else {
                                logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, scheduling to delete in [{}]", indexName, danglingTimeout);
                                danglingIndices.put(indexName, new DanglingIndex(indexName, threadPool.schedule(danglingTimeout, ThreadPool.Names.SAME, new RemoveDanglingIndex(indexName))));
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("failed to find dangling indices", e);
                    }
                }
            }
        }
    }

    class RemoveDanglingIndex implements Runnable {

        private final String index;

        RemoveDanglingIndex(String index) {
            this.index = index;
        }

        @Override
        public void run() {
            synchronized (danglingMutex) {
                DanglingIndex remove = danglingIndices.remove(index);
                // no longer there...
                if (remove == null) {
                    return;
                }
                logger.info("[{}] deleting dangling index", index);
                FileSystemUtils.deleteRecursively(nodeEnv.indexLocations(new Index(index)));
            }
        }
    }
}
