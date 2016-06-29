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

package org.elasticsearch.action.admin.cluster.allocation;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;

/**
 * The {@code TransportClusterAllocationExplainAction} is responsible for actually executing the explanation of a shard's allocation on the
 * master node in the cluster.
 */
public class TransportClusterAllocationExplainAction
        extends TransportMasterNodeAction<ClusterAllocationExplainRequest, ClusterAllocationExplainResponse> {

    private final ClusterInfoService clusterInfoService;
    private final AllocationDeciders allocationDeciders;
    private final ShardsAllocator shardAllocator;
    private final TransportIndicesShardStoresAction shardStoresAction;
    private final GatewayAllocator gatewayAllocator;

    @Inject
    public TransportClusterAllocationExplainAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                                   ThreadPool threadPool, ActionFilters actionFilters,
                                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                                   ClusterInfoService clusterInfoService, AllocationDeciders allocationDeciders,
                                                   ShardsAllocator shardAllocator, TransportIndicesShardStoresAction shardStoresAction,
                                                   GatewayAllocator gatewayAllocator) {
        super(settings, ClusterAllocationExplainAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, ClusterAllocationExplainRequest::new);
        this.clusterInfoService = clusterInfoService;
        this.allocationDeciders = allocationDeciders;
        this.shardAllocator = shardAllocator;
        this.shardStoresAction = shardStoresAction;
        this.gatewayAllocator = gatewayAllocator;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterAllocationExplainRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterAllocationExplainResponse newResponse() {
        return new ClusterAllocationExplainResponse();
    }

    /**
     * Return the decisions for the given {@code ShardRouting} on the given {@code RoutingNode}. If {@code includeYesDecisions} is not true,
     * only non-YES (NO and THROTTLE) decisions are returned.
     */
    public static Decision tryShardOnNode(ShardRouting shard, RoutingNode node, RoutingAllocation allocation, boolean includeYesDecisions) {
        Decision d = allocation.deciders().canAllocate(shard, node, allocation);
        if (includeYesDecisions) {
            return d;
        } else {
            Decision.Multi nonYesDecisions = new Decision.Multi();
            List<Decision> decisions = d.getDecisions();
            for (Decision decision : decisions) {
                if (decision.type() != Decision.Type.YES) {
                    nonYesDecisions.add(decision);
                }
            }
            return nonYesDecisions;
        }
    }

    /**
     * Construct a {@code NodeExplanation} object for the given shard given all the metadata. This also attempts to construct the human
     * readable FinalDecision and final explanation as part of the explanation.
     */
    public static NodeExplanation calculateNodeExplanation(ShardRouting shard,
                                                           IndexMetaData indexMetaData,
                                                           DiscoveryNode node,
                                                           Decision nodeDecision,
                                                           Float nodeWeight,
                                                           IndicesShardStoresResponse.StoreStatus storeStatus,
                                                           String assignedNodeId,
                                                           Set<String> activeAllocationIds,
                                                           boolean hasPendingAsyncFetch) {
        final ClusterAllocationExplanation.FinalDecision finalDecision;
        final ClusterAllocationExplanation.StoreCopy storeCopy;
        final String finalExplanation;

        if (storeStatus == null) {
            // No copies of the data
            storeCopy = ClusterAllocationExplanation.StoreCopy.NONE;
        } else {
            final Throwable storeErr = storeStatus.getStoreException();
            if (storeErr != null) {
                if (ExceptionsHelper.unwrapCause(storeErr) instanceof CorruptIndexException) {
                    storeCopy = ClusterAllocationExplanation.StoreCopy.CORRUPT;
                } else {
                    storeCopy = ClusterAllocationExplanation.StoreCopy.IO_ERROR;
                }
            } else if (activeAllocationIds.isEmpty()) {
                // The ids are only empty if dealing with a legacy index
                // TODO: fetch the shard state versions and display here?
                storeCopy = ClusterAllocationExplanation.StoreCopy.UNKNOWN;
            } else if (activeAllocationIds.contains(storeStatus.getAllocationId())) {
                storeCopy = ClusterAllocationExplanation.StoreCopy.AVAILABLE;
            } else {
                // Otherwise, this is a stale copy of the data (allocation ids don't match)
                storeCopy = ClusterAllocationExplanation.StoreCopy.STALE;
            }
        }

        if (node.getId().equals(assignedNodeId)) {
            finalDecision = ClusterAllocationExplanation.FinalDecision.ALREADY_ASSIGNED;
            finalExplanation = "the shard is already assigned to this node";
        } else if (hasPendingAsyncFetch &&
                shard.primary() == false &&
                shard.unassigned() &&
                shard.allocatedPostIndexCreate(indexMetaData) &&
                nodeDecision.type() != Decision.Type.YES) {
            finalExplanation = "the shard cannot be assigned because allocation deciders return a " + nodeDecision.type().name() +
                    " decision and the shard's state is still being fetched";
            finalDecision = ClusterAllocationExplanation.FinalDecision.NO;
        } else if (hasPendingAsyncFetch &&
                shard.unassigned() &&
                shard.allocatedPostIndexCreate(indexMetaData)) {
            finalExplanation = "the shard's state is still being fetched so it cannot be allocated";
            finalDecision = ClusterAllocationExplanation.FinalDecision.NO;
        } else if (shard.primary() && shard.unassigned() && shard.allocatedPostIndexCreate(indexMetaData) &&
                storeCopy == ClusterAllocationExplanation.StoreCopy.STALE) {
            finalExplanation = "the copy of the shard is stale, allocation ids do not match";
            finalDecision = ClusterAllocationExplanation.FinalDecision.NO;
        } else if (shard.primary() && shard.unassigned() && shard.allocatedPostIndexCreate(indexMetaData) &&
                storeCopy == ClusterAllocationExplanation.StoreCopy.NONE) {
            finalExplanation = "there is no copy of the shard available";
            finalDecision = ClusterAllocationExplanation.FinalDecision.NO;
        } else if (shard.primary() && shard.unassigned() && storeCopy == ClusterAllocationExplanation.StoreCopy.CORRUPT) {
            finalExplanation = "the copy of the shard is corrupt";
            finalDecision = ClusterAllocationExplanation.FinalDecision.NO;
        } else if (shard.primary() && shard.unassigned() && storeCopy == ClusterAllocationExplanation.StoreCopy.IO_ERROR) {
            finalExplanation = "the copy of the shard cannot be read";
            finalDecision = ClusterAllocationExplanation.FinalDecision.NO;
        } else {
            if (nodeDecision.type() == Decision.Type.NO) {
                finalDecision = ClusterAllocationExplanation.FinalDecision.NO;
                finalExplanation = "the shard cannot be assigned because one or more allocation decider returns a 'NO' decision";
            } else {
                // TODO: handle throttling decision better here
                finalDecision = ClusterAllocationExplanation.FinalDecision.YES;
                if (storeCopy == ClusterAllocationExplanation.StoreCopy.AVAILABLE) {
                    finalExplanation = "the shard can be assigned and the node contains a valid copy of the shard data";
                } else {
                    finalExplanation = "the shard can be assigned";
                }
            }
        }
        return new NodeExplanation(node, nodeDecision, nodeWeight, storeStatus, finalDecision, finalExplanation, storeCopy);
    }


    /**
     * For the given {@code ShardRouting}, return the explanation of the allocation for that shard on all nodes. If {@code
     * includeYesDecisions} is true, returns all decisions, otherwise returns only 'NO' and 'THROTTLE' decisions.
     */
    public static ClusterAllocationExplanation explainShard(ShardRouting shard, RoutingAllocation allocation, RoutingNodes routingNodes,
                                                            boolean includeYesDecisions, ShardsAllocator shardAllocator,
                                                            List<IndicesShardStoresResponse.StoreStatus> shardStores,
                                                            GatewayAllocator gatewayAllocator) {
        // don't short circuit deciders, we want a full explanation
        allocation.debugDecision(true);
        // get the existing unassigned info if available
        UnassignedInfo ui = shard.unassignedInfo();

        Map<DiscoveryNode, Decision> nodeToDecision = new HashMap<>();
        for (RoutingNode node : routingNodes) {
            DiscoveryNode discoNode = node.node();
            if (discoNode.isDataNode()) {
                Decision d = tryShardOnNode(shard, node, allocation, includeYesDecisions);
                nodeToDecision.put(discoNode, d);
            }
        }
        long remainingDelayMillis = 0;
        final MetaData metadata = allocation.metaData();
        final IndexMetaData indexMetaData = metadata.index(shard.index());
        long allocationDelayMillis = INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexMetaData.getSettings()).getMillis();
        if (ui != null && ui.isDelayed()) {
            long remainingDelayNanos = ui.getRemainingDelay(System.nanoTime(), indexMetaData.getSettings());
            remainingDelayMillis = TimeValue.timeValueNanos(remainingDelayNanos).millis();
        }

        // Calculate weights for each of the nodes
        Map<DiscoveryNode, Float> weights = shardAllocator.weighShard(allocation, shard);

        Map<DiscoveryNode, IndicesShardStoresResponse.StoreStatus> nodeToStatus = new HashMap<>(shardStores.size());
        for (IndicesShardStoresResponse.StoreStatus status : shardStores) {
            nodeToStatus.put(status.getNode(), status);
        }

        Map<DiscoveryNode, NodeExplanation> explanations = new HashMap<>(shardStores.size());
        for (Map.Entry<DiscoveryNode, Decision> entry : nodeToDecision.entrySet()) {
            DiscoveryNode node = entry.getKey();
            Decision decision = entry.getValue();
            Float weight = weights.get(node);
            IndicesShardStoresResponse.StoreStatus storeStatus = nodeToStatus.get(node);
            NodeExplanation nodeExplanation = calculateNodeExplanation(shard, indexMetaData, node, decision, weight,
                    storeStatus, shard.currentNodeId(), indexMetaData.activeAllocationIds(shard.getId()),
                    allocation.hasPendingAsyncFetch());
            explanations.put(node, nodeExplanation);
        }
        return new ClusterAllocationExplanation(shard.shardId(), shard.primary(),
            shard.currentNodeId(), allocationDelayMillis, remainingDelayMillis, ui,
            gatewayAllocator.hasFetchPending(shard.shardId(), shard.primary()), explanations);
    }

    @Override
    protected void masterOperation(final ClusterAllocationExplainRequest request, final ClusterState state,
                                   final ActionListener<ClusterAllocationExplainResponse> listener) {
        final RoutingNodes routingNodes = state.getRoutingNodes();
        final RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, state,
                clusterInfoService.getClusterInfo(), System.nanoTime(), false);

        ShardRouting foundShard = null;
        if (request.useAnyUnassignedShard()) {
            // If we can use any shard, just pick the first unassigned one (if there are any)
            RoutingNodes.UnassignedShards.UnassignedIterator ui = routingNodes.unassigned().iterator();
            if (ui.hasNext()) {
                foundShard = ui.next();
            }
        } else {
            String index = request.getIndex();
            int shard = request.getShard();
            if (request.isPrimary()) {
                // If we're looking for the primary shard, there's only one copy, so pick it directly
                foundShard = allocation.routingTable().shardRoutingTable(index, shard).primaryShard();
            } else {
                // If looking for a replica, go through all the replica shards
                List<ShardRouting> replicaShardRoutings = allocation.routingTable().shardRoutingTable(index, shard).replicaShards();
                if (replicaShardRoutings.size() > 0) {
                    // Pick the first replica at the very least
                    foundShard = replicaShardRoutings.get(0);
                    // In case there are multiple replicas where some are assigned and some aren't,
                    // try to find one that is unassigned at least
                    for (ShardRouting replica : replicaShardRoutings) {
                        if (replica.unassigned()) {
                            foundShard = replica;
                            break;
                        }
                    }
                }
            }
        }

        if (foundShard == null) {
            listener.onFailure(new ElasticsearchException("unable to find any shards to explain [{}] in the routing table", request));
            return;
        }
        final ShardRouting shardRouting = foundShard;
        logger.debug("explaining the allocation for [{}], found shard [{}]", request, shardRouting);

        getShardStores(shardRouting, new ActionListener<IndicesShardStoresResponse>() {
            @Override
            public void onResponse(IndicesShardStoresResponse shardStoreResponse) {
                ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>> shardStatuses =
                        shardStoreResponse.getStoreStatuses().get(shardRouting.getIndexName());
                List<IndicesShardStoresResponse.StoreStatus> shardStoreStatus = shardStatuses.get(shardRouting.id());
                ClusterAllocationExplanation cae = explainShard(shardRouting, allocation, routingNodes,
                        request.includeYesDecisions(), shardAllocator, shardStoreStatus, gatewayAllocator);
                listener.onResponse(new ClusterAllocationExplainResponse(cae));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    private void getShardStores(ShardRouting shard, final ActionListener<IndicesShardStoresResponse> listener) {
        IndicesShardStoresRequest request = new IndicesShardStoresRequest(shard.getIndexName());
        request.shardStatuses("all");
        shardStoresAction.execute(request, listener);
    }
}
