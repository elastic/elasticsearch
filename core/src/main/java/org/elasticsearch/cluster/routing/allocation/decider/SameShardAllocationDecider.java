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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 * An allocation decider that prevents multiple instances of the same shard to
 * be allocated on the same <tt>node</tt>.
 *
 * The {@value #SAME_HOST_SETTING} setting allows to perform a check to prevent
 * allocation of multiple instances of the same shard on a single <tt>host</tt>,
 * based on host name and host address. Defaults to `false`, meaning that no
 * check is performed by default.
 *
 * <p>
 * Note: this setting only applies if multiple nodes are started on the same
 * <tt>host</tt>. Allocations of multiple copies of the same shard on the same
 * <tt>node</tt> are not allowed independently of this setting.
 * </p>
 */
public class SameShardAllocationDecider extends AllocationDecider {

    public static final String NAME = "same_shard";

    public static final String SAME_HOST_SETTING = "cluster.routing.allocation.same_shard.host";

    private final boolean sameHost;

    @Inject
    public SameShardAllocationDecider(Settings settings) {
        super(settings);

        this.sameHost = settings.getAsBoolean(SAME_HOST_SETTING, false);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        Iterable<MutableShardRouting> assignedShards = allocation.routingNodes().assignedShards(shardRouting);
        for (MutableShardRouting assignedShard : assignedShards) {
            if (node.nodeId().equals(assignedShard.currentNodeId())) {
                return allocation.decision(Decision.NO, NAME, "shard cannot be allocated on same node [%s] it already exists on", node.nodeId());
            }
        }
        if (sameHost) {
            if (node.node() != null) {
                for (RoutingNode checkNode : allocation.routingNodes()) {
                    if (checkNode.node() == null) {
                        continue;
                    }
                    // check if its on the same host as the one we want to allocate to
                    boolean checkNodeOnSameHost = false;
                    if (Strings.hasLength(checkNode.node().getHostAddress()) && Strings.hasLength(node.node().getHostAddress())) {
                        if (checkNode.node().getHostAddress().equals(node.node().getHostAddress())) {
                            checkNodeOnSameHost = true;
                        }
                    } else if (Strings.hasLength(checkNode.node().getHostName()) && Strings.hasLength(node.node().getHostName())) {
                        if (checkNode.node().getHostName().equals(node.node().getHostName())) {
                            checkNodeOnSameHost = true;
                        }
                    }
                    if (checkNodeOnSameHost) {
                        for (MutableShardRouting assignedShard : assignedShards) {
                            if (checkNode.nodeId().equals(assignedShard.currentNodeId())) {
                                return allocation.decision(Decision.NO, NAME,
                                        "shard cannot be allocated on same host [%s] it already exists on", node.nodeId());
                            }
                        }
                    }
                }
            }
        }
        return allocation.decision(Decision.YES, NAME, "shard is not allocated to same node or host");
    }
}
