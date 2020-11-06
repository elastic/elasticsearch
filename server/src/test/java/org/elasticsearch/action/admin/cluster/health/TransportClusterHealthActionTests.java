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

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.core.IsEqual.equalTo;

public class TransportClusterHealthActionTests extends ESTestCase {

    public void testWaitForInitializingShards() throws Exception {
        final String[] indices = {"test"};
        final ClusterHealthRequest request = new ClusterHealthRequest();
        request.waitForNoInitializingShards(true);
        ClusterState clusterState = randomClusterStateWithInitializingShards("test", 0);
        ClusterHealthResponse response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(1));

        request.waitForNoInitializingShards(true);
        clusterState = randomClusterStateWithInitializingShards("test", between(1, 10));
        response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(0));

        request.waitForNoInitializingShards(false);
        clusterState = randomClusterStateWithInitializingShards("test", randomInt(20));
        response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(0));
    }

    public void testWaitForAllShards() {
        final String[] indices = {"test"};
        final ClusterHealthRequest request = new ClusterHealthRequest();
        request.waitForActiveShards(ActiveShardCount.ALL);

        ClusterState clusterState = randomClusterStateWithInitializingShards("test", 1);
        ClusterHealthResponse response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(0));

        clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).build();
        response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(1));
    }

    ClusterState randomClusterStateWithInitializingShards(String index, final int initializingShards) {
        final IndexMetadata indexMetadata = IndexMetadata
            .builder(index)
            .settings(settings(Version.CURRENT))
            .numberOfShards(between(1, 10))
            .numberOfReplicas(randomInt(20))
            .build();

        final List<ShardRoutingState> shardRoutingStates = new ArrayList<>();
        IntStream.range(0, between(1, 30)).forEach(i -> shardRoutingStates.add(randomFrom(
            ShardRoutingState.STARTED, ShardRoutingState.UNASSIGNED, ShardRoutingState.RELOCATING)));
        IntStream.range(0, initializingShards).forEach(i -> shardRoutingStates.add(ShardRoutingState.INITIALIZING));
        Randomness.shuffle(shardRoutingStates);

        final ShardId shardId = new ShardId(new Index("index", "uuid"), 0);
        final IndexRoutingTable.Builder routingTable = new IndexRoutingTable.Builder(indexMetadata.getIndex());

        // Primary
        {
            ShardRoutingState state = shardRoutingStates.remove(0);
            String node = state == ShardRoutingState.UNASSIGNED ? null : "node";
            routingTable.addShard(
                TestShardRouting.newShardRouting(shardId, node, "relocating", true, state)
            );
        }

        // Replicas
        for (int i = 0; i < shardRoutingStates.size(); i++) {
            ShardRoutingState state = shardRoutingStates.get(i);
            String node = state == ShardRoutingState.UNASSIGNED ? null : "node" + i;
            routingTable.addShard(TestShardRouting.newShardRouting(shardId, node, "relocating"+i, randomBoolean(), state));
        }

        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(routingTable.build()).build())
            .build();
    }
}
