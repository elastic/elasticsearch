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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 * An allocation decider that prevents relocation or allocation from nodes
 * that might not be version compatible. If we relocate from a node that runs
 * a newer version than the node we relocate to this might cause {@link org.apache.lucene.index.IndexFormatTooNewException}
 * on the lowest level since it might have already written segments that use a new postings format or codec that is not
 * available on the target node.
 */
public class NodeVersionAllocationDecider extends AllocationDecider {

    public static final String NAME = "node_version";

    @Inject
    public NodeVersionAllocationDecider(Settings settings) {
        super(settings);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        String sourceNodeId = shardRouting.currentNodeId();
        /* if sourceNodeId is not null we do a relocation and just check the version of the node
         * that we are currently allocate on. If not we are initializing and recover from primary.*/
        if (sourceNodeId == null) { // we allocate - check primary
            if (shardRouting.primary()) {
                // we are the primary we can allocate wherever
                return allocation.decision(Decision.YES, NAME, "primary shard can be allocated anywhere");
            }
            final MutableShardRouting primary = allocation.routingNodes().activePrimary(shardRouting);
            if (primary == null) { // we have a primary - it's a start ;)
                return allocation.decision(Decision.YES, NAME, "no active primary shard yet");
            }
            sourceNodeId = primary.currentNodeId();
        }
        return isVersionCompatible(allocation.routingNodes(), sourceNodeId, node, allocation);

    }

    private Decision isVersionCompatible(final RoutingNodes routingNodes, final String sourceNodeId, final RoutingNode target, RoutingAllocation allocation) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().version().onOrAfter(source.node().version())) {
            /* we can allocate if we can recover from a node that is younger or on the same version
             * if the primary is already running on a newer version that won't work due to possible
             * differences in the lucene index format etc.*/
            return allocation.decision(Decision.YES, NAME, "target node version [%s] is same or newer than source node version [%s]",
                    target.node().version(), source.node().version());
        } else {
            return allocation.decision(Decision.NO, NAME, "target node version [%s] is older than source node version [%s]",
                    target.node().version(), source.node().version());
        }
    }
}
