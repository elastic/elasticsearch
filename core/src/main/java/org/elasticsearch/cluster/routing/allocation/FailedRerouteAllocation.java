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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;

import java.util.List;

/**
 * This {@link RoutingAllocation} keeps a shard which routing
 * allocation has faild
 */
public class FailedRerouteAllocation extends RoutingAllocation {

    /**
     * A failed shard with the shard routing itself and an optional
     * details on why it failed.
     */
    public static class FailedShard {
        public final ShardRouting shard;
        public final String message;
        public final Throwable failure;

        public FailedShard(ShardRouting shard, String message, Throwable failure) {
            this.shard = shard;
            this.message = message;
            this.failure = failure;
        }

        @Override
        public String toString() {
            return "failed shard, shard " + shard + ", message [" + message + "], failure [" + ExceptionsHelper.detailedMessage(failure) + "]";
        }
    }

    private final List<FailedShard> failedShards;

    public FailedRerouteAllocation(AllocationDeciders deciders, RoutingNodes routingNodes, DiscoveryNodes nodes, List<FailedShard> failedShards, ClusterInfo clusterInfo) {
        super(deciders, routingNodes, nodes, clusterInfo);
        this.failedShards = failedShards;
    }

    public List<FailedShard> failedShards() {
        return failedShards;
    }
}
