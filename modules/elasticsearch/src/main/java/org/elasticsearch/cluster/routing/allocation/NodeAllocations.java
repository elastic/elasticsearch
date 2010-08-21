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

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

/**
 * Holds several {@link NodeAllocation}s and combines them into a single allocation decision.
 *
 * @author kimchy (shay.banon)
 */
public class NodeAllocations extends AbstractComponent implements NodeAllocation {

    private final NodeAllocation[] allocations;

    public NodeAllocations(Settings settings) {
        this(settings, ImmutableSet.<NodeAllocation>builder()
                .add(new SameShardNodeAllocation(settings)
                ).build()
        );
    }

    @Inject public NodeAllocations(Settings settings, Set<NodeAllocation> allocations) {
        super(settings);
        this.allocations = allocations.toArray(new NodeAllocation[allocations.size()]);
    }

    @Override public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingNodes routingNodes) {
        for (NodeAllocation allocation : allocations) {
            Decision decision = allocation.canAllocate(shardRouting, node, routingNodes);
            if (decision == Decision.DISALLOWED) {
                return Decision.DISALLOWED;
            }
        }
        return Decision.ALLOWED;
    }
}
