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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;

/**
 * @author kimchy (Shay Banon)
 */
public interface ShardsRoutingStrategy {

    /**
     * Applies the started shards. Note, shards can be called several times within this method.
     *
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    RoutingTable applyStartedShards(ClusterState clusterState, Iterable<? extends ShardRouting> startedShardEntries);

    /**
     * Applies the failed shards. Note, shards can be called several times within this method.
     *
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    RoutingTable applyFailedShards(ClusterState clusterState, Iterable<? extends ShardRouting> failedShardEntries);

    /**
     * Reroutes the routing table based on the live nodes.
     *
     * <p>If the same instance of the routing table is returned, then no change has been made.
     */
    RoutingTable reroute(ClusterState clusterState);
}
