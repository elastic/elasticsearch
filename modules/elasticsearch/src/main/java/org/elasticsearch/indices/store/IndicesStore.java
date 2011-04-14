/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;

import java.io.File;

/**
 * @author kimchy (shay.banon)
 */
public class IndicesStore extends AbstractComponent implements ClusterStateListener {

    private final NodeEnvironment nodeEnv;

    private final IndicesService indicesService;

    private final ClusterService clusterService;

    @Inject public IndicesStore(Settings settings, NodeEnvironment nodeEnv, IndicesService indicesService, ClusterService clusterService) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        clusterService.add(this);
    }

    public void close() {
        clusterService.remove(this);
    }

    @Override public void clusterChanged(ClusterChangedEvent event) {
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
                // not allocated on this node yet...
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

        // do the reverse, and delete dangling indices that might remain on that node
        // this can happen when deleting a closed index, or when a node joins and it has deleted indices
        if (nodeEnv.hasNodeFile()) {
            File[] files = nodeEnv.indicesLocation().listFiles();
            if (files != null) {
                for (File file : files) {
                    // if we have the index on the metadata, don't delete it
                    if (event.state().metaData().hasIndex(file.getName())) {
                        continue;
                    }
                    logger.debug("[{}] deleting index that is no longer in the cluster meta_date", file.getName());
                    FileSystemUtils.deleteRecursively(file);
                }
            }
        }
    }
}
