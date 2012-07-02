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

import org.apache.lucene.store.StoreRateLimiting;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;

/**
 *
 */
public class IndicesStore extends AbstractComponent implements ClusterStateListener {

    static {
        MetaData.addDynamicSettings(
                "indices.store.throttle.type",
                "indices.store.throttle.max_bytes_per_sec"
        );
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String rateLimitingType = settings.get("indices.store.throttle.type", IndicesStore.this.rateLimitingType);
            // try and parse the type
            StoreRateLimiting.Type.fromString(rateLimitingType);
            if (!rateLimitingType.equals(IndicesStore.this.rateLimitingType)) {
                logger.info("updating indices.store.throttle.type from [{}] to [{}]", IndicesStore.this.rateLimitingType, rateLimitingType);
                IndicesStore.this.rateLimitingType = rateLimitingType;
                IndicesStore.this.rateLimiting.setType(rateLimitingType);
            }

            ByteSizeValue rateLimitingThrottle = settings.getAsBytesSize("indices.store.throttle.max_bytes_per_sec", IndicesStore.this.rateLimitingThrottle);
            if (!rateLimitingThrottle.equals(IndicesStore.this.rateLimitingThrottle)) {
                logger.info("updating indices.store.throttle.max_bytes_per_sec from [{}] to [{}], note, type is [{}]", IndicesStore.this.rateLimitingThrottle, rateLimitingThrottle, IndicesStore.this.rateLimitingType);
                IndicesStore.this.rateLimitingThrottle = rateLimitingThrottle;
                IndicesStore.this.rateLimiting.setMaxRate(rateLimitingThrottle);
            }
        }
    }


    private final NodeEnvironment nodeEnv;

    private final NodeSettingsService nodeSettingsService;

    private final IndicesService indicesService;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private volatile String rateLimitingType;
    private volatile ByteSizeValue rateLimitingThrottle;
    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();

    private final ApplySettings applySettings = new ApplySettings();

    @Inject
    public IndicesStore(Settings settings, NodeEnvironment nodeEnv, NodeSettingsService nodeSettingsService, IndicesService indicesService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.nodeSettingsService = nodeSettingsService;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        this.rateLimitingType = componentSettings.get("throttle.type", "none");
        rateLimiting.setType(rateLimitingType);
        this.rateLimitingThrottle = componentSettings.getAsBytesSize("throttle.max_bytes_per_sec", new ByteSizeValue(0));
        rateLimiting.setMaxRate(rateLimitingThrottle);

        logger.debug("using indices.store.throttle.type [{}], with index.store.throttle.max_bytes_per_sec [{}]", rateLimitingType, rateLimitingThrottle);

        nodeSettingsService.addListener(applySettings);
        clusterService.addLast(this);
    }

    public StoreRateLimiting rateLimiting() {
        return this.rateLimiting;
    }

    public void close() {
        nodeSettingsService.removeListener(applySettings);
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
                int startedShardsCount = indexShardRoutingTable.countWithState(ShardRoutingState.STARTED);
                if (startedShardsCount > 0 && startedShardsCount == indexShardRoutingTable.size()) {
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

        // do the reverse, and delete dangling shards that might remain on that node
        // but are allocated on other nodes
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
        }
    }
}
