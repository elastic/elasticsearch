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

package org.elasticsearch.cluster.routing.strategy;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.gateway.blobstore.BlobStoreIndexGateway;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;

import java.util.Iterator;

/**
 * @author kimchy (shay.banon)
 */
public class PreferUnallocatedShardUnassignedStrategy extends AbstractComponent {

    private final IndicesService indicesService;

    private final TransportNodesListShardStoreMetaData transportNodesListShardStoreMetaData;

    @Inject public PreferUnallocatedShardUnassignedStrategy(Settings settings, IndicesService indicesService,
                                                            TransportNodesListShardStoreMetaData transportNodesListShardStoreMetaData) {
        super(settings);
        this.indicesService = indicesService;
        this.transportNodesListShardStoreMetaData = transportNodesListShardStoreMetaData;
    }

    public boolean allocateUnassigned(RoutingNodes routingNodes) {
        boolean changed = false;

        Iterator<MutableShardRouting> unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            MutableShardRouting shard = unassignedIterator.next();
            InternalIndexService indexService = (InternalIndexService) indicesService.indexService(shard.index());
            if (indexService == null) {
                continue;
            }
            // if the store is not persistent, it makes no sense to test for special allocation
            if (!indexService.store().persistent()) {
                continue;
            }

            TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData nodesStoreFilesMetaData = transportNodesListShardStoreMetaData.list(shard.shardId(), false).actionGet();

            long lastSizeMatched = 0;
            DiscoveryNode lastDiscoNodeMatched = null;
            RoutingNode lastNodeMatched = null;

            for (TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData nodeStoreFilesMetaData : nodesStoreFilesMetaData) {
                DiscoveryNode discoNode = nodeStoreFilesMetaData.node();
                IndexStore.StoreFilesMetaData storeFilesMetaData = nodeStoreFilesMetaData.storeFilesMetaData();

                if (storeFilesMetaData == null) {
                    // already allocated on that node...
                    continue;
                }

                RoutingNode node = routingNodes.node(discoNode.id());
                if (node == null) {
                    continue;
                }

                // check if we can allocate on that node...
                if (!(node.canAllocate(routingNodes.metaData(), routingNodes.routingTable()) && node.canAllocate(shard))) {
                    continue;
                }
                // if it is already allocated, we can't assign to it...
                if (storeFilesMetaData.allocated()) {
                    continue;
                }


                // if its a primary, it will be recovered from the gateway, find one that is closet to it
                if (shard.primary() && indexService.gateway() instanceof BlobStoreIndexGateway) {
                    BlobStoreIndexGateway indexGateway = (BlobStoreIndexGateway) indexService.gateway();
                    try {
                        ImmutableMap<String, BlobMetaData> indexBlobsMetaData = indexGateway.listIndexBlobs(shard.id());

                        long sizeMatched = 0;
                        for (StoreFileMetaData storeFileMetaData : storeFilesMetaData) {
                            if (indexBlobsMetaData.containsKey(storeFileMetaData.name()) && indexBlobsMetaData.get(storeFileMetaData.name()).md5().equals(storeFileMetaData.md5())) {
                                sizeMatched += storeFileMetaData.sizeInBytes();
                            }
                        }
                        if (sizeMatched > lastSizeMatched) {
                            lastSizeMatched = sizeMatched;
                            lastDiscoNodeMatched = discoNode;
                            lastNodeMatched = node;
                        }

                        continue;
                    } catch (Exception e) {
                        // failed, log and try and allocate based on size
                        logger.debug("Failed to guess allocation of primary based on gateway for " + shard, e);
                    }
                }

                // if its backup, see if there is a primary that *is* allocated, and try and assign a location that is closest to it
                // note, since we replicate operations, this might not be the same (different flush intervals)
                if (!shard.primary()) {
                    MutableShardRouting primaryShard = routingNodes.findPrimaryForBackup(shard);
                    if (primaryShard != null && primaryShard.active()) {
                        TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData primaryNodeStoreFileMetaData = nodesStoreFilesMetaData.nodesMap().get(primaryShard.currentNodeId());
                        if (primaryNodeStoreFileMetaData != null && primaryNodeStoreFileMetaData.storeFilesMetaData() != null && primaryNodeStoreFileMetaData.storeFilesMetaData().allocated()) {
                            long sizeMatched = 0;

                            IndexStore.StoreFilesMetaData primaryStoreFilesMetaData = primaryNodeStoreFileMetaData.storeFilesMetaData();
                            for (StoreFileMetaData storeFileMetaData : storeFilesMetaData) {
                                if (primaryStoreFilesMetaData.fileExists(storeFileMetaData.name()) && primaryStoreFilesMetaData.file(storeFileMetaData.name()).sizeInBytes() == storeFileMetaData.sizeInBytes()) {
                                    sizeMatched += storeFileMetaData.sizeInBytes();
                                }
                            }
                            if (sizeMatched > lastSizeMatched) {
                                lastSizeMatched = sizeMatched;
                                lastDiscoNodeMatched = discoNode;
                                lastNodeMatched = node;
                            }

                            continue;
                        }
                    }
                }
            }

            if (lastNodeMatched != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}] allocating to [{}] in order to reuse its unallocated persistent store", shard.index(), shard.id(), lastDiscoNodeMatched);
                }
                // we found a match
                changed = true;
                lastNodeMatched.add(shard);
                unassignedIterator.remove();
            }
        }

        return changed;
    }
}
