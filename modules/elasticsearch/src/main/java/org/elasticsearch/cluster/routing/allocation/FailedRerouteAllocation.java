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
import org.elasticsearch.cluster.routing.ShardRouting;

/**
 * @author kimchy (shay.banon)
 */
public class FailedRerouteAllocation extends RoutingAllocation {

    private final ShardRouting failedShard;

    public FailedRerouteAllocation(RoutingNodes routingNodes, DiscoveryNodes nodes, ShardRouting failedShard) {
        super(routingNodes, nodes);
        this.failedShard = failedShard;
    }

    public ShardRouting failedShard() {
        return failedShard;
    }
}
