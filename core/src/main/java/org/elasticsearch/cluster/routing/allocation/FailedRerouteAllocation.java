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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;

/**
 * This {@link RoutingAllocation} keeps a shard which routing
 * allocation has failed.
 */
public class FailedRerouteAllocation extends RoutingAllocation {

    /**
     * A failed shard with the shard routing itself and an optional
     * details on why it failed.
     */
    public static class FailedShard {
        public final ShardRouting routingEntry;
        public final String message;
        public final Exception failure;

        public FailedShard(ShardRouting routingEntry, String message, Exception failure) {
            assert routingEntry.assignedToNode() : "only assigned shards can be failed " + routingEntry;
            this.routingEntry = routingEntry;
            this.message = message;
            this.failure = failure;
        }

        @Override
        public String toString() {
            return "failed shard, shard " + routingEntry + ", message [" + message + "], failure [" +
                ExceptionsHelper.detailedMessage(failure) + "]";
        }
    }

    private final List<FailedShard> failedShards;

    public FailedRerouteAllocation(AllocationDeciders deciders, RoutingNodes routingNodes, ClusterState clusterState,
                                   List<FailedShard> failedShards, ClusterInfo clusterInfo, long currentNanoTime) {
        super(deciders, routingNodes, clusterState, clusterInfo, currentNanoTime, false);
        this.failedShards = failedShards;
    }

    public List<FailedShard> failedShards() {
        return failedShards;
    }
}
