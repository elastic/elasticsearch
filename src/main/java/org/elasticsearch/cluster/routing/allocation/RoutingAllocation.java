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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@link RoutingAllocation} keep the state of the current allocation
 * of shards and holds the {@link AllocationDeciders} which are responsible
 *  for the current routing state.
 */
public class RoutingAllocation {

    /**
     * this class is used to describe results of a {@link RoutingAllocation}  
     */
    public static class Result {

        private final boolean changed;

        private final RoutingTable routingTable;

        private final AllocationExplanation explanation;

        /**
         * Creates a new {@link RoutingAllocation.Result}
         * 
         * @param changed a flag to determine whether the actual {@link RoutingTable} has been changed
         * @param routingTable the {@link RoutingTable} this Result references
         * @param explanation Explanation of the Result
         */
        public Result(boolean changed, RoutingTable routingTable, AllocationExplanation explanation) {
            this.changed = changed;
            this.routingTable = routingTable;
            this.explanation = explanation;
        }

        /** determine whether the actual {@link RoutingTable} has been changed
         * @return <code>true</code> if the {@link RoutingTable} has been changed by allocation. Otherwise <code>false</code>
         */
        public boolean changed() {
            return this.changed;
        }

        /**
         * Get the {@link RoutingTable} referenced by this result
         * @return referenced {@link RoutingTable}
         */
        public RoutingTable routingTable() {
            return routingTable;
        }

        /**
         * Get the explanation of this result
         * @return explanation
         */
        public AllocationExplanation explanation() {
            return explanation;
        }
    }

    private final AllocationDeciders deciders;

    private final RoutingNodes routingNodes;

    private final DiscoveryNodes nodes;

    private final AllocationExplanation explanation = new AllocationExplanation();

    private final ClusterInfo clusterInfo;

    private Map<ShardId, String> ignoredShardToNodes = null;

    private boolean ignoreDisable = false;

    /**
     * Creates a new {@link RoutingAllocation}
     * 
     * @param deciders {@link AllocationDeciders} to used to make decisions for routing allocations
     * @param routingNodes Routing nodes in the current cluster 
     * @param nodes TODO: Documentation
     */
    public RoutingAllocation(AllocationDeciders deciders, RoutingNodes routingNodes, DiscoveryNodes nodes, ClusterInfo clusterInfo) {
        this.deciders = deciders;
        this.routingNodes = routingNodes;
        this.nodes = nodes;
        this.clusterInfo = clusterInfo;
    }

    /**
     * Get {@link AllocationDeciders} used for allocation
     * @return {@link AllocationDeciders} used for allocation
     */
    public AllocationDeciders deciders() {
        return this.deciders;
    }

    /**
     * Get routing table of current nodes
     * @return current routing table
     */
    public RoutingTable routingTable() {
        return routingNodes.routingTable();
    }

    /**
     * Get current routing nodes
     * @return routing nodes
     */
    public RoutingNodes routingNodes() {
        return routingNodes;
    }

    /**
     * Get metadata of routing nodes
     * @return Metadata of routing nodes
     */
    public MetaData metaData() {
        return routingNodes.metaData();
    }

    /**
     * Get discovery nodes in current routing
     * @return discovery nodes
     */
    public DiscoveryNodes nodes() {
        return nodes;
    }

    public ClusterInfo clusterInfo() {
        return clusterInfo;
    }

    /**
     * Get explanations of current routing
     * @return explanation of routing
     */
    public AllocationExplanation explanation() {
        return explanation;
    }

    public void ignoreDisable(boolean ignoreDisable) {
        this.ignoreDisable = ignoreDisable;
    }

    public boolean ignoreDisable() {
        return this.ignoreDisable;
    }

    public void addIgnoreShardForNode(ShardId shardId, String nodeId) {
        if (ignoredShardToNodes == null) {
            ignoredShardToNodes = new HashMap<ShardId, String>();
        }
        ignoredShardToNodes.put(shardId, nodeId);
    }

    public boolean shouldIgnoreShardForNode(ShardId shardId, String nodeId) {
        return ignoredShardToNodes != null && nodeId.equals(ignoredShardToNodes.get(shardId));
    }
}
