/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;

interface ShardInformationValidator {
    boolean isShardIdStillValid(ShardId shardId);

    boolean isShardRoutingStillValid(ShardRouting searchShardTarget);

    class ClusterStateShardInformationValidator implements ShardInformationValidator {
        private final ClusterService clusterService;

        ClusterStateShardInformationValidator(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public boolean isShardIdStillValid(ShardId shardId) {
            RoutingTable routingTable = getRoutingTable();
            IndexRoutingTable indexRoutingTable = routingTable.index(shardId.getIndex());
            if (indexRoutingTable == null) {
                return false;
            }

            return indexRoutingTable.shard(shardId.getId()) != null;
        }

        @Override
        public boolean isShardRoutingStillValid(ShardRouting shardRouting) {
            // To optimize this we might want to save the cluster state version on which the
            // searchShardTarget was based on and do a fast comparison just based on that and
            // the latest known cluster version?
            RoutingTable routingTable = getRoutingTable();

            IndexRoutingTable indexRoutingTable = routingTable.index(shardRouting.index());
            if (indexRoutingTable == null) {
                return false;
            }
            IndexShardRoutingTable shard = indexRoutingTable.shard(shardRouting.shardId().getId());
            ShardRouting latestShardRouting = shard.getByAllocationId(shardRouting.allocationId().getId());
            return latestShardRouting != null &&
                   latestShardRouting.active() &&
                   latestShardRouting.currentNodeId().equalsIgnoreCase(shardRouting.currentNodeId());
        }

        private RoutingTable getRoutingTable() {
            return clusterService.state().getRoutingTable();
        }
    }
}
