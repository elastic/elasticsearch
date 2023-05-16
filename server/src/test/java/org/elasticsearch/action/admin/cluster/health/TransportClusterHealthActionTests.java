/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.core.IsEqual.equalTo;

public class TransportClusterHealthActionTests extends ESTestCase {

    public void testWaitForInitializingShards() throws Exception {
        final String[] indices = { "test" };
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
        final String[] indices = { "test" };
        final ClusterHealthRequest request = new ClusterHealthRequest();
        request.waitForActiveShards(ActiveShardCount.ALL);

        ClusterState clusterState = randomClusterStateWithInitializingShards("test", 1);
        ClusterHealthResponse response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(0));

        clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(1));
    }

    ClusterState randomClusterStateWithInitializingShards(String index, final int initializingShards) {
        final IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(indexSettings(Version.CURRENT, between(1, 10), randomInt(20)))
            .build();

        final List<ShardRoutingState> shardRoutingStates = new ArrayList<>();
        if (initializingShards == 1 && randomBoolean()) {
            shardRoutingStates.add(ShardRoutingState.INITIALIZING);
            IntStream.range(0, between(0, 30)).forEach(i -> shardRoutingStates.add(ShardRoutingState.UNASSIGNED));
        } else {
            IntStream.range(0, between(0, 30))
                .forEach(
                    i -> shardRoutingStates.add(
                        randomFrom(ShardRoutingState.STARTED, ShardRoutingState.UNASSIGNED, ShardRoutingState.RELOCATING)
                    )
                );
            IntStream.range(0, initializingShards).forEach(i -> shardRoutingStates.add(ShardRoutingState.INITIALIZING));
            Randomness.shuffle(shardRoutingStates);

            // primary must be active, otherwise replicas can't in initializing or relocating state.
            shardRoutingStates.add(0, randomFrom(ShardRoutingState.STARTED, ShardRoutingState.RELOCATING));
        }

        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexRoutingTable.Builder routingTable = new IndexRoutingTable.Builder(
            ShardRoutingRoleStrategy.NO_SHARD_CREATION,
            indexMetadata.getIndex()
        );

        // Primary
        {
            ShardRoutingState state = shardRoutingStates.remove(0);
            String node = "node";
            String relocatingNode = state == ShardRoutingState.RELOCATING ? "relocating" : null;
            routingTable.addShard(TestShardRouting.newShardRouting(shardId, node, relocatingNode, true, state));
        }

        // Replicas
        for (int i = 0; i < shardRoutingStates.size(); i++) {
            ShardRoutingState state = shardRoutingStates.get(i);
            String node = state == ShardRoutingState.UNASSIGNED ? null : "node" + i;
            String relocatingNode = state == ShardRoutingState.RELOCATING ? "relocating" + i : null;
            routingTable.addShard(TestShardRouting.newShardRouting(shardId, node, relocatingNode, false, state));
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(routingTable.build()).build())
            .build();
    }
}
