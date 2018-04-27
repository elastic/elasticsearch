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

public class SetSingleNodeAllocateStep extends AsyncActionStep {
    public static final String NAME = "set-single-node-allocation";

    private static final AllocationDeciders ALLOCATION_DECIDERS = new AllocationDeciders(Settings.EMPTY, Collections.singletonList(
            new FilterAllocationDecider(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))));

    public SetSingleNodeAllocateStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState clusterState, Listener listener) {
        RoutingAllocation allocation = new RoutingAllocation(ALLOCATION_DECIDERS, clusterState.getRoutingNodes(), clusterState, null,
                System.nanoTime());
        List<String> validNodeNames = new ArrayList<>();
        Optional<ShardRouting> anyShard = clusterState.getRoutingTable().allShards(indexMetaData.getIndex().getName()).stream().findAny();
        if (anyShard.isPresent()) {
            // Iterate through the nodes finding ones that are acceptablee for the current allocation rules of the shard
            for (RoutingNode node : clusterState.getRoutingNodes()) {
                boolean canRemainOnCurrentNode = ALLOCATION_DECIDERS.canRemain(anyShard.get(), node, allocation)
                        .type() == Decision.Type.YES;
                if (canRemainOnCurrentNode) {
                    DiscoveryNode discoveryNode = node.node();
                    validNodeNames.add(discoveryNode.getName());
                }
            }
            // Shuffle the list of nodes so the one we pick is random
            Randomness.shuffle(validNodeNames);
            Optional<String> nodeName = validNodeNames.stream().findAny();
            if (nodeName.isPresent()) {
                Settings settings = Settings.builder()
                        .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", nodeName.get()).build();
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
