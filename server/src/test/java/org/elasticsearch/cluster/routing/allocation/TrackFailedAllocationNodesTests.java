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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class TrackFailedAllocationNodesTests extends ESAllocationTestCase {

    public void testTrackFailedNodes() {
        int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        AllocationService allocationService = createAllocationService();
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 5; i++) {
            discoNodes.add(newNode("node-" + i));
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(discoNodes)
            .metaData(metaData).routingTable(RoutingTable.builder().addAsNew(metaData.index("idx")).build())
            .build();
        clusterState = allocationService.reroute(clusterState, "reroute");
        Set<String> failedNodeIds = new HashSet<>();

        // track the failed nodes if shard is not started
        for (int i = 0; i < maxRetries; i++) {
            failedNodeIds.add(clusterState.routingTable().index("idx").shard(0).shards().get(0).currentNodeId());
            clusterState = allocationService.applyFailedShard(
                clusterState, clusterState.routingTable().index("idx").shard(0).shards().get(0), randomBoolean());
            assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).unassignedInfo().getFailedNodeIds(),
                equalTo(failedNodeIds));
        }

        // reroute with retryFailed=true should discard the failedNodes
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).state(), equalTo(ShardRoutingState.UNASSIGNED));
        clusterState = allocationService.reroute(clusterState, new AllocationCommands(), false, true).getClusterState();
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).unassignedInfo().getFailedNodeIds(), empty());

        // do not track the failed nodes while shard is started
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).state(), equalTo(ShardRoutingState.STARTED));
        clusterState = allocationService.applyFailedShard(
            clusterState, clusterState.routingTable().index("idx").shard(0).shards().get(0), false);
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).unassignedInfo().getFailedNodeIds(), empty());
    }
}
