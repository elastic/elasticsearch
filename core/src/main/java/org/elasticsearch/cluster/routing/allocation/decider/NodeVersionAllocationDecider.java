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

import org.elasticsearch.cluster.routing.RestoreSource;
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
        if (shardRouting.primary()) {
            if (shardRouting.currentNodeId() == null) {
                if (shardRouting.restoreSource() != null) {
                    // restoring from a snapshot - check that the node can handle the version
                    return isVersionCompatible(shardRouting.restoreSource(), node, allocation);
                } else {
                    // fresh primary, we can allocate wherever
                    return allocation.decision(Decision.YES, NAME, "the primary shard is new and can be allocated anywhere");
                }
            } else {
                // relocating primary, only migrate to newer host
                return isVersionCompatible(allocation.routingNodes(), shardRouting.currentNodeId(), node, allocation);
            }
        } else {
            final ShardRouting primary = allocation.routingNodes().activePrimary(shardRouting.shardId());
            // check that active primary has a newer version so that peer recovery works
            if (primary != null) {
                return isVersionCompatible(allocation.routingNodes(), primary.currentNodeId(), node, allocation);
            } else {
                // ReplicaAfterPrimaryActiveAllocationDecider should prevent this case from occurring
                return allocation.decision(Decision.YES, NAME, "no active primary shard yet");
            }
        }
    }

    private Decision isVersionCompatible(final RoutingNodes routingNodes, final String sourceNodeId, final RoutingNode target,
                                         RoutingAllocation allocation) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().getVersion().onOrAfter(source.node().getVersion())) {
            /* we can allocate if we can recover from a node that is younger or on the same version
             * if the primary is already running on a newer version that won't work due to possible
             * differences in the lucene index format etc.*/
            return allocation.decision(Decision.YES, NAME, "target node version [%s] is the same or newer than source node version [%s]",
                    target.node().getVersion(), source.node().getVersion());
        } else {
            return allocation.decision(Decision.NO, NAME, "target node version [%s] is older than the source node version [%s]",
                    target.node().getVersion(), source.node().getVersion());
        }
    }

    private Decision isVersionCompatible(RestoreSource restoreSource, final RoutingNode target, RoutingAllocation allocation) {
        if (target.node().getVersion().onOrAfter(restoreSource.version())) {
            /* we can allocate if we can restore from a snapshot that is older or on the same version */
            return allocation.decision(Decision.YES, NAME, "target node version [%s] is the same or newer than snapshot version [%s]",
                target.node().getVersion(), restoreSource.version());
        } else {
            return allocation.decision(Decision.NO, NAME, "target node version [%s] is older than the snapshot version [%s]",
                target.node().getVersion(), restoreSource.version());
        }
    }
}
