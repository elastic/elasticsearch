/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Allocates all shards in a single index to one node.
 * For example, as preparation for shrinking that index.
 */
public class SetSingleNodeAllocateStep extends AsyncActionStep {
    private static final Logger logger = LogManager.getLogger(SetSingleNodeAllocateStep.class);
    public static final String NAME = "set-single-node-allocation";

    // These allocation deciders were chosen because these are the conditions that can prevent
    // allocation long-term, and that we can inspect in advance. Most other allocation deciders
    // will either only delay relocation (e.g. ThrottlingAllocationDecider), or don't work very
    // well when reallocating potentially many shards at once (e.g. DiskThresholdDecider)
    private static final AllocationDeciders ALLOCATION_DECIDERS = new AllocationDeciders(List.of(
        new FilterAllocationDecider(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
        new NodeVersionAllocationDecider()
    ));

    public SetSingleNodeAllocateStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState clusterState, ClusterStateObserver observer, Listener listener) {
        final RoutingNodes routingNodes = clusterState.getRoutingNodes();
        RoutingAllocation allocation = new RoutingAllocation(ALLOCATION_DECIDERS, routingNodes, clusterState, null,
                System.nanoTime());
        List<String> validNodeIds = new ArrayList<>();
        final Map<ShardId, List<ShardRouting>> routingsByShardId = clusterState.getRoutingTable()
            .allShards(indexMetaData.getIndex().getName())
            .stream()
            .collect(Collectors.groupingBy(ShardRouting::shardId));


        if (routingsByShardId.isEmpty() == false) {
            for (RoutingNode node : routingNodes) {
                boolean canAllocateOneCopyOfEachShard = routingsByShardId.values().stream() // For each shard
                    .allMatch(shardRoutings -> shardRoutings.stream() // Can we allocate at least one shard copy to this node?
                        .map(shardRouting -> ALLOCATION_DECIDERS.canAllocate(shardRouting, node, allocation).type())
                        .anyMatch(Decision.Type.YES::equals));
                if (canAllocateOneCopyOfEachShard) {
                    validNodeIds.add(node.node().getId());
                }
            }
            // Shuffle the list of nodes so the one we pick is random
            Randomness.shuffle(validNodeIds);
            Optional<String> nodeId = validNodeIds.stream().findAny();
            if (nodeId.isPresent()) {
                Settings settings = Settings.builder()
                        .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", nodeId.get()).build();
                UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexMetaData.getIndex().getName())
                        .masterNodeTimeout(getMasterTimeout(clusterState))
                        .settings(settings);
                getClient().admin().indices().updateSettings(updateSettingsRequest,
                        ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
            } else {
                // No nodes currently match the allocation rules so just wait until there is one that does
                logger.debug("could not find any nodes to allocate index [{}] onto prior to shrink");
                listener.onResponse(false);
            }
        } else {
            // There are no shards for the index, the index might be gone
            listener.onFailure(new IndexNotFoundException(indexMetaData.getIndex()));
        }
    }

}
