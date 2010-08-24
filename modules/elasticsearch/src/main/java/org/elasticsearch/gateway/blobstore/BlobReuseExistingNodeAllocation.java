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

package org.elasticsearch.gateway.blobstore;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.NodeAllocation;
import org.elasticsearch.cluster.routing.allocation.NodeAllocations;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.gateway.CommitPoint;
import org.elasticsearch.index.gateway.blobstore.BlobStoreIndexGateway;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.Iterator;

/**
 * @author kimchy (shay.banon)
 */
public class BlobReuseExistingNodeAllocation extends NodeAllocation {

    private final IndicesService indicesService;

    private final TransportNodesListShardStoreMetaData transportNodesListShardStoreMetaData;

    private final TimeValue listTimeout;

    @Inject public BlobReuseExistingNodeAllocation(Settings settings, IndicesService indicesService,
                                                   TransportNodesListShardStoreMetaData transportNodesListShardStoreMetaData) {
        super(settings);
        this.indicesService = indicesService;
        this.transportNodesListShardStoreMetaData = transportNodesListShardStoreMetaData;

        this.listTimeout = componentSettings.getAsTime("list_timeout", TimeValue.timeValueMillis(500));
    }

    @Override public boolean allocate(NodeAllocations nodeAllocations, RoutingNodes routingNodes, DiscoveryNodes nodes) {
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

            TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData nodesStoreFilesMetaData = transportNodesListShardStoreMetaData.list(shard.shardId(), false, nodes.dataNodes().keySet(), listTimeout).actionGet();

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
                // we only check for NO, since if this node is THROTTLING and it has enough "same data"
                // then we will try and assign it next time
                if (nodeAllocations.canAllocate(shard, node, routingNodes) == Decision.NO) {
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
                        CommitPoint commitPoint = indexGateway.findCommitPoint(shard.id());

                        if (logger.isTraceEnabled()) {
                            StringBuilder sb = new StringBuilder(shard + ": checking for pre_allocation (gateway) on node " + discoNode + "\n");
                            sb.append("    gateway_files:\n");
                            for (CommitPoint.FileInfo fileInfo : commitPoint.indexFiles()) {
                                sb.append("        [").append(fileInfo.name()).append("]/[").append(fileInfo.physicalName()).append("], size [").append(new ByteSizeValue(fileInfo.length())).append("]\n");
                            }
                            sb.append("    node_files:\n");
                            for (StoreFileMetaData md : storeFilesMetaData) {
                                sb.append("        [").append(md.name()).append("], size [").append(new ByteSizeValue(md.length())).append("]\n");
                            }
                            logger.trace(sb.toString());
                        }
                        long sizeMatched = 0;
                        for (StoreFileMetaData storeFileMetaData : storeFilesMetaData) {
                            CommitPoint.FileInfo fileInfo = commitPoint.findPhysicalIndexFile(storeFileMetaData.name());
                            if (fileInfo != null) {
                                if (fileInfo.length() == storeFileMetaData.length()) {
                                    logger.trace("{}: [{}] reusing file since it exists on remote node and on gateway with size [{}]", shard, storeFileMetaData.name(), new ByteSizeValue(storeFileMetaData.length()));
                                    sizeMatched += storeFileMetaData.length();
                                } else {
                                    logger.trace("{}: [{}] ignore file since it exists on remote node and on gateway but has different size, remote node [{}], gateway [{}]", shard, storeFileMetaData.name(), storeFileMetaData.length(), fileInfo.length());
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
                                if (primaryStoreFilesMetaData.fileExists(storeFileMetaData.name()) && primaryStoreFilesMetaData.file(storeFileMetaData.name()).length() == storeFileMetaData.length()) {
                                    sizeMatched += storeFileMetaData.length();
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
                if (nodeAllocations.canAllocate(shard, lastNodeMatched, routingNodes) == NodeAllocation.Decision.THROTTLE) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}][{}]: throttling allocation [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
                    }
                    // we are throttling this, but we have enough to allocate to this node, ignore it for now
                    unassignedIterator.remove();
                    routingNodes.ignoredUnassigned().add(shard);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
                    }
                    // we found a match
                    changed = true;
                    lastNodeMatched.add(shard);
                    unassignedIterator.remove();
                }
            }
        }

        return changed;
    }

    @Override public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingNodes routingNodes) {
        return Decision.YES;
    }
}
