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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.gateway.blobstore.BlobStoreIndexGateway;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

/**
 * @author kimchy (shay.banon)
 */
public class PreferUnallocatedShardUnassignedStrategy extends AbstractComponent implements PreferUnallocatedStrategy {

    private final ThreadPool threadPool;

    private final IndicesService indicesService;

    private final TransportNodesListShardStoreMetaData transportNodesListShardStoreMetaData;

    @Inject public PreferUnallocatedShardUnassignedStrategy(Settings settings, ThreadPool threadPool, IndicesService indicesService,
                                                            TransportNodesListShardStoreMetaData transportNodesListShardStoreMetaData) {
        super(settings);
        this.threadPool = threadPool;
        this.indicesService = indicesService;
        this.transportNodesListShardStoreMetaData = transportNodesListShardStoreMetaData;
    }

    @Override public void prefetch(IndexMetaData index, DiscoveryNodes nodes) {
        final CountDownLatch latch = new CountDownLatch(index.numberOfShards());
        for (int shardId = 0; shardId < index.numberOfShards(); shardId++) {
            transportNodesListShardStoreMetaData.list(new ShardId(index.index(), shardId), false, nodes.dataNodes().keySet(), new ActionListener<TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData>() {
                @Override public void onResponse(TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData nodesStoreFilesMetaData) {
                    latch.countDown();
                }

                @Override public void onFailure(Throwable e) {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public boolean allocateUnassigned(RoutingNodes routingNodes, DiscoveryNodes nodes) {
        boolean changed = false;

        if (nodes.dataNodes().isEmpty()) {
            return changed;
        }

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

            if (!shard.primary()) {
                // if its a backup, only allocate it if the primary is active
                MutableShardRouting primary = routingNodes.findPrimaryForReplica(shard);
                if (primary == null || !primary.active()) {
                    continue;
                }
            }

            TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData nodesStoreFilesMetaData = transportNodesListShardStoreMetaData.list(shard.shardId(), false, nodes.dataNodes().keySet()).actionGet();

            if (logger.isDebugEnabled()) {
                if (nodesStoreFilesMetaData.failures().length > 0) {
                    StringBuilder sb = new StringBuilder(shard + ": failures when trying to list stores on nodes:");
                    for (int i = 0; i < nodesStoreFilesMetaData.failures().length; i++) {
                        Throwable cause = ExceptionsHelper.unwrapCause(nodesStoreFilesMetaData.failures()[i]);
                        if (cause instanceof ConnectTransportException) {
                            continue;
                        }
                        sb.append("\n    -> ").append(nodesStoreFilesMetaData.failures()[i].getDetailedMessage());
                    }
                    logger.debug(sb.toString());
                }
            }

            long lastSizeMatched = 0;
            DiscoveryNode lastDiscoNodeMatched = null;
            RoutingNode lastNodeMatched = null;

            for (TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData nodeStoreFilesMetaData : nodesStoreFilesMetaData) {
                DiscoveryNode discoNode = nodeStoreFilesMetaData.node();
                logger.trace("{}: checking node [{}]", shard, discoNode);
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

                        if (logger.isDebugEnabled()) {
                            StringBuilder sb = new StringBuilder(shard + ": checking for pre_allocation (gateway) on node " + discoNode + "\n");
                            sb.append("    gateway_files:\n");
                            for (BlobMetaData md : indexBlobsMetaData.values()) {
                                sb.append("        [").append(md.name()).append("], size [").append(new ByteSizeValue(md.sizeInBytes())).append("], md5 [").append(md.md5()).append("]\n");
                            }
                            sb.append("    node_files:\n");
                            for (StoreFileMetaData md : storeFilesMetaData) {
                                sb.append("        [").append(md.name()).append("], size [").append(new ByteSizeValue(md.sizeInBytes())).append("], md5 [").append(md.md5()).append("]\n");
                            }
                            logger.debug(sb.toString());
                        }
                        logger.trace("{}: checking for pre_allocation (gateway) on node [{}]\n   gateway files", shard, discoNode, indexBlobsMetaData.keySet());
                        long sizeMatched = 0;
                        for (StoreFileMetaData storeFileMetaData : storeFilesMetaData) {
                            if (indexBlobsMetaData.containsKey(storeFileMetaData.name())) {
                                if (indexBlobsMetaData.get(storeFileMetaData.name()).md5().equals(storeFileMetaData.md5())) {
                                    logger.trace("{}: [{}] reusing file since it exists on remote node and on gateway (same md5) with size [{}]", shard, storeFileMetaData.name(), new ByteSizeValue(storeFileMetaData.sizeInBytes()));
                                    sizeMatched += storeFileMetaData.sizeInBytes();
                                } else {
                                    logger.trace("{}: [{}] ignore file since it exists on remote node and on gateway but has different md5, remote node [{}], gateway [{}]", shard, storeFileMetaData.name(), storeFileMetaData.md5(), indexBlobsMetaData.get(storeFileMetaData.name()).md5());
                                }
                            } else {
                                logger.trace("{}: [{}] exists on remote node, does not exists on gateway", shard, storeFileMetaData.name());
                            }
                        }
                        if (sizeMatched > lastSizeMatched) {
                            lastSizeMatched = sizeMatched;
                            lastDiscoNodeMatched = discoNode;
                            lastNodeMatched = node;
                            logger.trace("{}: node elected for pre_allocation [{}], total_size_matched [{}]", shard, discoNode, new ByteSizeValue(sizeMatched));
                        } else {
                            logger.trace("{}: node ignored for pre_allocation [{}], total_size_matched [{}] smaller than last_size_matched [{}]", shard, discoNode, new ByteSizeValue(sizeMatched), new ByteSizeValue(lastSizeMatched));
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
                    MutableShardRouting primaryShard = routingNodes.findPrimaryForReplica(shard);
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
                    logger.debug("[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
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
