/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Allocates all shards in a single index to one node.
 * For example, as preparation for shrinking that index.
 */
public class SetSingleNodeAllocateStep extends AsyncActionStep {
    public static final String NAME = "set-single-node-allocation";

    private static final AllocationDeciders ALLOCATION_DECIDERS = new AllocationDeciders(Collections.singletonList(
            new FilterAllocationDecider(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))));

    public SetSingleNodeAllocateStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState clusterState, Listener listener) {
        RoutingAllocation allocation = new RoutingAllocation(ALLOCATION_DECIDERS, clusterState.getRoutingNodes(), clusterState, null,
                System.nanoTime());
        List<String> validNodeIds = new ArrayList<>();
        Optional<ShardRouting> anyShard = clusterState.getRoutingTable().allShards(indexMetaData.getIndex().getName()).stream().findAny();
        if (anyShard.isPresent()) {
            // Iterate through the nodes finding ones that are acceptable for the current allocation rules of the shard
            for (RoutingNode node : clusterState.getRoutingNodes()) {
                boolean canRemainOnCurrentNode = ALLOCATION_DECIDERS.canRemain(anyShard.get(), node, allocation)
                        .type() == Decision.Type.YES;
                if (canRemainOnCurrentNode) {
                    DiscoveryNode discoveryNode = node.node();
                    validNodeIds.add(discoveryNode.getId());
                }
            }
            // Shuffle the list of nodes so the one we pick is random
            Randomness.shuffle(validNodeIds);
            Optional<String> nodeId = validNodeIds.stream().findAny();
            if (nodeId.isPresent()) {
                Settings settings = Settings.builder()
                        .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", nodeId.get()).build();
                UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexMetaData.getIndex().getName())
                        .settings(settings);
                getClient().admin().indices().updateSettings(updateSettingsRequest,
                        ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
            } else {
                // No nodes currently match the allocation rules so just wait until there is one that does
                listener.onResponse(false);
            }
        } else {
            // There are no shards for the index, the index might be gone
            listener.onFailure(new IndexNotFoundException(indexMetaData.getIndex()));
        }
    }

}
