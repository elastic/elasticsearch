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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodes.RoutingNodesIterator;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The {@code TransportClusterAllocationExplainAction} is responsible for actually executing the explanation of a shard's allocation on the
 * master node in the cluster.
 */
public class TransportClusterAllocationExplainAction
        extends TransportMasterNodeAction<ClusterAllocationExplainRequest, ClusterAllocationExplainResponse> {

    private final AllocationService allocationService;
    private final ClusterInfoService clusterInfoService;
    private final AllocationDeciders allocationDeciders;
    private final ShardsAllocator shardAllocator;

    @Inject
    public TransportClusterAllocationExplainAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                                   ThreadPool threadPool, ActionFilters actionFilters,
                                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                                   AllocationService allocationService, ClusterInfoService clusterInfoService,
                                                   AllocationDeciders allocationDeciders, ShardsAllocator shardAllocator) {
        super(settings, ClusterAllocationExplainAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, ClusterAllocationExplainRequest::new);
        this.allocationService = allocationService;
        this.clusterInfoService = clusterInfoService;
        this.allocationDeciders = allocationDeciders;
        this.shardAllocator = shardAllocator;
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
     * For the given {@code ShardRouting}, return the explanation of the allocation for that shard on all nodes. If {@code
     * includeYesDecisions} is true, returns all decisions, otherwise returns only 'NO' and 'THROTTLE' decisions.
     */
    public static ClusterAllocationExplanation explainShard(ShardRouting shard, RoutingAllocation allocation, RoutingNodes routingNodes,
                                                            boolean includeYesDecisions, ShardsAllocator shardAllocator) {
        // don't short circuit deciders, we want a full explanation
        allocation.debugDecision(true);
        // get the existing unassigned info if available
        UnassignedInfo ui = shard.unassignedInfo();

        RoutingNodesIterator iter = routingNodes.nodes();
        Map<DiscoveryNode, Decision> nodeToDecision = new HashMap<>();
        while (iter.hasNext()) {
            RoutingNode node = iter.next();
            DiscoveryNode discoNode = node.node();
            if (discoNode.isDataNode()) {
                Decision d = tryShardOnNode(shard, node, allocation, includeYesDecisions);
                nodeToDecision.put(discoNode, d);
            }
        }
        long remainingDelayNanos = 0;
        if (ui != null) {
            final MetaData metadata = allocation.metaData();
            final Settings indexSettings = metadata.index(shard.index()).getSettings();
            remainingDelayNanos = ui.getRemainingDelay(System.nanoTime(), metadata.settings(), indexSettings);
        }
        return new ClusterAllocationExplanation(shard.shardId(), shard.primary(), shard.currentNodeId(), ui, nodeToDecision,
                shardAllocator.weighShard(allocation, shard), remainingDelayNanos);
    }

    @Override
    protected void masterOperation(final ClusterAllocationExplainRequest request, final ClusterState state,
                                   final ActionListener<ClusterAllocationExplainResponse> listener) {
        final RoutingNodes routingNodes = state.getRoutingNodes();
        final RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, state.nodes(),
                clusterInfoService.getClusterInfo(), System.nanoTime());

        ShardRouting shardRouting = null;
        if (request.useAnyUnassignedShard()) {
            // If we can use any shard, just pick the first unassigned one (if there are any)
            RoutingNodes.UnassignedShards.UnassignedIterator ui = routingNodes.unassigned().iterator();
            if (ui.hasNext()) {
                shardRouting = ui.next();
            }
        } else {
            String index = request.getIndex();
            int shard = request.getShard();
            if (request.isPrimary()) {
                // If we're looking for the primary shard, there's only one copy, so pick it directly
                shardRouting = allocation.routingTable().shardRoutingTable(index, shard).primaryShard();
            } else {
                // If looking for a replica, go through all the replica shards
                List<ShardRouting> replicaShardRoutings = allocation.routingTable().shardRoutingTable(index, shard).replicaShards();
                if (replicaShardRoutings.size() > 0) {
                    // Pick the first replica at the very least
                    shardRouting = replicaShardRoutings.get(0);
                    // In case there are multiple replicas where some are assigned and some aren't,
                    // try to find one that is unassigned at least
                    for (ShardRouting replica : replicaShardRoutings) {
                        if (replica.unassigned()) {
                            shardRouting = replica;
                            break;
                        }
                    }
                }
            }
        }

        if (shardRouting == null) {
            listener.onFailure(new ElasticsearchException("unable to find any shards to explain [{}] in the routing table", request));
            return;
        }
        logger.debug("explaining the allocation for [{}], found shard [{}]", request, shardRouting);

        ClusterAllocationExplanation cae = explainShard(shardRouting, allocation, routingNodes,
                request.includeYesDecisions(), shardAllocator);
        listener.onResponse(new ClusterAllocationExplainResponse(cae));
    }
}
