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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.allocation.decider.Decision.THROTTLE;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.YES;

/**
 * {@link ThrottlingAllocationDecider} controls the recovery process per node in
 * the cluster. It exposes two settings via the cluster update API that allow
 * changes in real-time:
 * <ul>
 * <li>{@code cluster.routing.allocation.node_initial_primaries_recoveries} -
 * restricts the number of initial primary shard recovery operations on a single
 * node. The default is {@code 4}</li>
 * <li>{@code cluster.routing.allocation.node_concurrent_recoveries} -
 * restricts the number of total concurrent shards initializing on a single node. The
 * default is {@code 2}</li>
 * </ul>
 * <p>
 * If one of the above thresholds is exceeded per node this allocation decider
 * will return {@link Decision#THROTTLE} as a hit to upstream logic to throttle
 * the allocation process to prevent overloading nodes due to too many concurrent recovery
 * processes.
 */
public class ThrottlingAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(ThrottlingAllocationDecider.class);

    public static final int DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES = 2;
    public static final int DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES = 4;
    public static final String NAME = "throttling";
    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING =
        new Setting<>("cluster.routing.allocation.node_concurrent_recoveries",
            Integer.toString(DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES),
            (s) -> Setting.parseInt(s, 0, "cluster.routing.allocation.node_concurrent_recoveries"),
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING =
        Setting.intSetting("cluster.routing.allocation.node_initial_primaries_recoveries",
            DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES, 0,
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING =
        new Setting<>("cluster.routing.allocation.node_concurrent_incoming_recoveries",
            CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING::getRaw,
            (s) -> Setting.parseInt(s, 0, "cluster.routing.allocation.node_concurrent_incoming_recoveries"),
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING =
        new Setting<>("cluster.routing.allocation.node_concurrent_outgoing_recoveries",
            CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING::getRaw,
            (s) -> Setting.parseInt(s, 0, "cluster.routing.allocation.node_concurrent_outgoing_recoveries"),
            Property.Dynamic, Property.NodeScope);


    private volatile int primariesInitialRecoveries;
    private volatile int concurrentIncomingRecoveries;
    private volatile int concurrentOutgoingRecoveries;

    public ThrottlingAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.primariesInitialRecoveries = CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.get(settings);
        concurrentIncomingRecoveries = CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.get(settings);
        concurrentOutgoingRecoveries = CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING,
                this::setPrimariesInitialRecoveries);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
                this::setConcurrentIncomingRecoverries);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING,
                this::setConcurrentOutgoingRecoverries);

        logger.debug("using node_concurrent_outgoing_recoveries [{}], node_concurrent_incoming_recoveries [{}], " +
                        "node_initial_primaries_recoveries [{}]",
                concurrentOutgoingRecoveries, concurrentIncomingRecoveries, primariesInitialRecoveries);
    }

    private void setConcurrentIncomingRecoverries(int concurrentIncomingRecoveries) {
        this.concurrentIncomingRecoveries = concurrentIncomingRecoveries;
    }
    private void setConcurrentOutgoingRecoverries(int concurrentOutgoingRecoveries) {
        this.concurrentOutgoingRecoveries = concurrentOutgoingRecoveries;
    }

    private void setPrimariesInitialRecoveries(int primariesInitialRecoveries) {
        this.primariesInitialRecoveries = primariesInitialRecoveries;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary() && shardRouting.unassigned()) {
            assert initializingShard(shardRouting, node.nodeId()).recoverySource().getType() != RecoverySource.Type.PEER;
            // primary is unassigned, means we are going to do recovery from store, snapshot or local shards
            // count *just the primaries* currently doing recovery on the node and check against primariesInitialRecoveries

            int primariesInRecovery = 0;
            for (ShardRouting shard : node) {
                // when a primary shard is INITIALIZING, it can be because of *initial recovery* or *relocation from another node*
                // we only count initial recoveries here, so we need to make sure that relocating node is null
                if (shard.initializing() && shard.primary() && shard.relocatingNodeId() == null) {
                    primariesInRecovery++;
                }
            }
            if (primariesInRecovery >= primariesInitialRecoveries) {
                // TODO: Should index creation not be throttled for primary shards?
                return allocation.decision(THROTTLE, NAME,
                    "reached the limit of ongoing initial primary recoveries [%d], cluster setting [%s=%d]",
                    primariesInRecovery, CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(),
                    primariesInitialRecoveries);
            } else {
                return allocation.decision(YES, NAME, "below primary recovery limit of [%d]", primariesInitialRecoveries);
            }
        } else {
            // Peer recovery
            assert initializingShard(shardRouting, node.nodeId()).recoverySource().getType() == RecoverySource.Type.PEER;

            // Allocating a shard to this node will increase the incoming recoveries
            int currentInRecoveries = allocation.routingNodes().getIncomingRecoveries(node.nodeId());
            if (currentInRecoveries >= concurrentIncomingRecoveries) {
                return allocation.decision(THROTTLE, NAME,
                    "reached the limit of incoming shard recoveries [%d], cluster setting [%s=%d] (can also be set via [%s])",
                    currentInRecoveries, CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                    concurrentIncomingRecoveries,
                    CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey());
            } else {
                // search for corresponding recovery source (= primary shard) and check number of outgoing recoveries on that node
                ShardRouting primaryShard = allocation.routingNodes().activePrimary(shardRouting.shardId());
                if (primaryShard == null) {
                    return allocation.decision(Decision.NO, NAME, "primary shard for this replica is not yet active");
                }
                int primaryNodeOutRecoveries = allocation.routingNodes().getOutgoingRecoveries(primaryShard.currentNodeId());
                if (primaryNodeOutRecoveries >= concurrentOutgoingRecoveries) {
                    return allocation.decision(THROTTLE, NAME,
                        "reached the limit of outgoing shard recoveries [%d] on the node [%s] which holds the primary, " +
                        "cluster setting [%s=%d] (can also be set via [%s])",
                        primaryNodeOutRecoveries, primaryShard.currentNodeId(),
                        CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                        concurrentOutgoingRecoveries,
                        CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey());
                } else {
                    return allocation.decision(YES, NAME, "below shard recovery limit of outgoing: [%d < %d] incoming: [%d < %d]",
                        primaryNodeOutRecoveries,
                        concurrentOutgoingRecoveries,
                        currentInRecoveries,
                        concurrentIncomingRecoveries);
                }
            }
        }
    }

    /**
     * The shard routing passed to {@link #canAllocate(ShardRouting, RoutingNode, RoutingAllocation)} is not the initializing shard to this
     * node but:
     * - the unassigned shard routing in case if we want to assign an unassigned shard to this node.
     * - the initializing shard routing if we want to assign the initializing shard to this node instead
     * - the started shard routing in case if we want to check if we can relocate to this node.
     * - the relocating shard routing if we want to relocate to this node now instead.
     *
     * This method returns the corresponding initializing shard that would be allocated to this node.
     */
    private ShardRouting initializingShard(ShardRouting shardRouting, String currentNodeId) {
        final ShardRouting initializingShard;
        if (shardRouting.unassigned()) {
            initializingShard = shardRouting.initialize(currentNodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        } else if (shardRouting.initializing()) {
            UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            if (unassignedInfo == null) {
                // unassigned shards must have unassignedInfo (initializing shards might not)
                unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "fake");
            }
            initializingShard = shardRouting.moveToUnassigned(unassignedInfo)
                .initialize(currentNodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        } else if (shardRouting.relocating()) {
            initializingShard = shardRouting.cancelRelocation()
                .relocate(currentNodeId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                .getTargetRelocatingShard();
        } else {
            assert shardRouting.started();
            initializingShard = shardRouting.relocate(currentNodeId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                .getTargetRelocatingShard();
        }
        assert initializingShard.initializing();
        return initializingShard;
    }
}
