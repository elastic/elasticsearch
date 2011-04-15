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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class RoutingAllocation {

    public static class Result {

        private final boolean changed;

        private final RoutingTable routingTable;

        private final AllocationExplanation explanation;

        public Result(boolean changed, RoutingTable routingTable, AllocationExplanation explanation) {
            this.changed = changed;
            this.routingTable = routingTable;
            this.explanation = explanation;
        }

        public boolean changed() {
            return this.changed;
        }

        public RoutingTable routingTable() {
            return routingTable;
        }

        public AllocationExplanation explanation() {
            return explanation;
        }
    }

    private final RoutingNodes routingNodes;

    private final DiscoveryNodes nodes;

    private final AllocationExplanation explanation = new AllocationExplanation();

    private Map<ShardId, String> ignoredShardToNodes = null;

    public RoutingAllocation(RoutingNodes routingNodes, DiscoveryNodes nodes) {
        this.routingNodes = routingNodes;
        this.nodes = nodes;
    }

    public RoutingTable routingTable() {
        return routingNodes.routingTable();
    }

    public RoutingNodes routingNodes() {
        return routingNodes;
    }

    public DiscoveryNodes nodes() {
        return nodes;
    }

    public AllocationExplanation explanation() {
        return explanation;
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
