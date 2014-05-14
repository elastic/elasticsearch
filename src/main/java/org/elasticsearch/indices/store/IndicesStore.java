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

package org.elasticsearch.indices.store;

import org.apache.lucene.store.StoreRateLimiting;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.transport.TransportService;

import java.io.File;

/**
 *
 */
public class IndicesStore extends AbstractComponent implements ClusterStateListener {

    public static final String INDICES_STORE_THROTTLE_TYPE = "indices.store.throttle.type";
    public static final String INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC = "indices.store.throttle.max_bytes_per_sec";

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String rateLimitingType = settings.get(INDICES_STORE_THROTTLE_TYPE, IndicesStore.this.rateLimitingType);
            // try and parse the type
            StoreRateLimiting.Type.fromString(rateLimitingType);
            if (!rateLimitingType.equals(IndicesStore.this.rateLimitingType)) {
                logger.info("updating indices.store.throttle.type from [{}] to [{}]", IndicesStore.this.rateLimitingType, rateLimitingType);
                IndicesStore.this.rateLimitingType = rateLimitingType;
                IndicesStore.this.rateLimiting.setType(rateLimitingType);
            }

            ByteSizeValue rateLimitingThrottle = settings.getAsBytesSize(INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC, IndicesStore.this.rateLimitingThrottle);
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
    private final TransportService transportService;

    private volatile String rateLimitingType;
    private volatile ByteSizeValue rateLimitingThrottle;
    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();

    private final ApplySettings applySettings = new ApplySettings();

    @Inject
    public IndicesStore(Settings settings, NodeEnvironment nodeEnv, NodeSettingsService nodeSettingsService, IndicesService indicesService,
                        ClusterService clusterService, TransportService transportService) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.nodeSettingsService = nodeSettingsService;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;

        // we limit with 20MB / sec by default with a default type set to merge sice 0.90.1
        this.rateLimitingType = componentSettings.get("throttle.type", StoreRateLimiting.Type.MERGE.name());
        rateLimiting.setType(rateLimitingType);
        this.rateLimitingThrottle = componentSettings.getAsBytesSize("throttle.max_bytes_per_sec", new ByteSizeValue(20, ByteSizeUnit.MB));
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

        for (IndexRoutingTable indexRoutingTable : event.state().routingTable()) {
            // Note, closed indices will not have any routing information, so won't be deleted
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                ShardId shardId = indexShardRoutingTable.shardId();
                // a shard can be deleted if all its copies are active, and its not allocated on this node
                boolean shardCanBeDeleted = true;
                if (indexShardRoutingTable.size() == 0) {
                    // should not really happen, there should always be at least 1 (primary) shard in a
                    // shard replication group, in any case, protected from deleting something by mistake
                    shardCanBeDeleted = false;
                } else {
                    for (ShardRouting shardRouting : indexShardRoutingTable) {
                        // be conservative here, check on started, not even active
                        if (!shardRouting.started()) {
                            shardCanBeDeleted = false;
                            break;
                        }

                        // if the allocated or relocation node id doesn't exists in the cluster state or we're not connected to it
                        // it may be a stale node, make sure we don't do anything with this until the routing table has properly been
                        // rerouted to reflect the fact that the node does not exists
                        DiscoveryNode node = event.state().nodes().get(shardRouting.currentNodeId());
                        if (node == null || !transportService.nodeConnected(node)) {
                            shardCanBeDeleted = false;
                            break;
                        }
                        if (shardRouting.relocatingNodeId() != null) {
                            node = event.state().nodes().get(shardRouting.relocatingNodeId());
                            if (node == null || !transportService.nodeConnected(node)) {
                                shardCanBeDeleted = false;
                                break;
                            }
                        }

                        // check if shard is active on the current node or is getting relocated to the our node
                        String localNodeId = clusterService.localNode().id();
                        if (localNodeId.equals(shardRouting.currentNodeId()) || localNodeId.equals(shardRouting.relocatingNodeId())) {
                            shardCanBeDeleted = false;
                            break;
                        }
                    }
                }
                if (shardCanBeDeleted) {
                    IndexService indexService = indicesService.indexService(indexRoutingTable.index());
                    if (indexService == null) {
                        // not physical allocation of the index, delete it from the file system if applicable
                        if (nodeEnv.hasNodeFile()) {
                            File[] shardLocations = nodeEnv.shardLocations(shardId);
                            if (FileSystemUtils.exists(shardLocations)) {
                                logger.debug("[{}][{}] deleting shard that is no longer used", shardId.index().name(), shardId.id());
                                FileSystemUtils.deleteRecursively(shardLocations);
                            }
                        }
                    } else {
                        if (!indexService.hasShard(shardId.id())) {
                            if (indexService.store().canDeleteUnallocated(shardId)) {
                                logger.debug("[{}][{}] deleting shard that is no longer used", shardId.index().name(), shardId.id());
                                try {
                                    indexService.store().deleteUnallocated(indexShardRoutingTable.shardId());
                                } catch (Exception e) {
                                    logger.debug("[{}][{}] failed to delete unallocated shard, ignoring", e, indexShardRoutingTable.shardId().index().name(), indexShardRoutingTable.shardId().id());
                                }
                            }
                        } else {
                            // this state is weird, should we log?
                            // basically, it means that the shard is not allocated on this node using the routing
                            // but its still physically exists on an IndexService
                            // Note, this listener should run after IndicesClusterStateService...
                        }
                    }
                }
            }
        }
    }
}
