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

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

/**
 * An allocation decider that prevents multiple instances of the same shard to
 * be allocated on the same <tt>node</tt>.
 *
 * The {@link #CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING} setting allows to perform a check to prevent
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

    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING =
        Setting.boolSetting("cluster.routing.allocation.same_shard.host", false, Property.Dynamic, Property.NodeScope);

    private volatile boolean sameHost;

    public SameShardAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        this.sameHost = CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING, this::setSameHost);
    }

    /**
     * Sets the same host setting.  {@code true} if allocating the same shard copy to the same host
     * should not be allowed, even when multiple nodes are being run on the same host.  {@code false}
     * otherwise.
     */
    private void setSameHost(boolean sameHost) {
        this.sameHost = sameHost;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        Iterable<ShardRouting> assignedShards = allocation.routingNodes().assignedShards(shardRouting.shardId());
        Decision decision = decideSameNode(shardRouting, node, allocation, assignedShards);
        if (decision.type() == Decision.Type.NO || sameHost == false) {
            // if its already a NO decision looking at the node, or we aren't configured to look at the host, return the decision
            return decision;
        }
        if (node.node() != null) {
            for (RoutingNode checkNode : allocation.routingNodes()) {
                if (checkNode.node() == null) {
                    continue;
                }
                // check if its on the same host as the one we want to allocate to
                boolean checkNodeOnSameHostName = false;
                boolean checkNodeOnSameHostAddress = false;
                if (Strings.hasLength(checkNode.node().getHostAddress()) && Strings.hasLength(node.node().getHostAddress())) {
                    if (checkNode.node().getHostAddress().equals(node.node().getHostAddress())) {
                        checkNodeOnSameHostAddress = true;
                    }
                } else if (Strings.hasLength(checkNode.node().getHostName()) && Strings.hasLength(node.node().getHostName())) {
                    if (checkNode.node().getHostName().equals(node.node().getHostName())) {
                        checkNodeOnSameHostName = true;
                    }
                }
                if (checkNodeOnSameHostAddress || checkNodeOnSameHostName) {
                    for (ShardRouting assignedShard : assignedShards) {
                        if (checkNode.nodeId().equals(assignedShard.currentNodeId())) {
                            String hostType = checkNodeOnSameHostAddress ? "address" : "name";
                            String host = checkNodeOnSameHostAddress ? node.node().getHostAddress() : node.node().getHostName();
                            return allocation.decision(Decision.NO, NAME,
                                "the shard cannot be allocated on host %s [%s], where it already exists on node [%s]; " +
                                    "set cluster setting [%s] to false to allow multiple nodes on the same host to hold the same " +
                                    "shard copies",
                                hostType, host, node.nodeId(), CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey());
                        }
                    }
                }
            }
        }
        return allocation.decision(Decision.YES, NAME, "the shard does not exist on the same host");
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call force allocate on a non-primary shard";
        Iterable<ShardRouting> assignedShards = allocation.routingNodes().assignedShards(shardRouting.shardId());
        return decideSameNode(shardRouting, node, allocation, assignedShards);
    }

    private Decision decideSameNode(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation,
                                    Iterable<ShardRouting> assignedShards) {
        for (ShardRouting assignedShard : assignedShards) {
            if (node.nodeId().equals(assignedShard.currentNodeId())) {
                if (assignedShard.isSameAllocation(shardRouting)) {
                    return allocation.decision(Decision.NO, NAME,
                        "the shard cannot be allocated to the node on which it already exists [%s]",
                        shardRouting.toString());
                } else {
                    return allocation.decision(Decision.NO, NAME,
                        "the shard cannot be allocated to the same node on which a copy of the shard already exists [%s]",
                        assignedShard.toString());
                }
            }
        }
        return allocation.decision(Decision.YES, NAME, "the shard does not exist on the same node");
    }
}
