/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Table;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestShardsActionTests extends ESTestCase {

    public void testBuildTable() {
        final int numShards = randomIntBetween(1, 5);
        DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);

        List<ShardRouting> shardRoutings = new ArrayList<>(numShards);
        Map<ShardRouting, ShardStats> shardStatsMap = new HashMap<>();
        String index = "index";
        for (int i = 0; i < numShards; i++) {
            ShardRoutingState shardRoutingState = ShardRoutingState.fromValue((byte) randomIntBetween(2, 3));
            ShardRouting shardRouting = TestShardRouting.newShardRouting(index, i, localNode.getId(), randomBoolean(), shardRoutingState);
            Path path = createTempDir().resolve("indices").resolve(shardRouting.shardId().getIndex().getUUID())
                .resolve(String.valueOf(shardRouting.shardId().id()));
            ShardStats shardStats = new ShardStats(shardRouting, new ShardPath(false, path, path, shardRouting.shardId()),
                null, null, null, null);
            shardStatsMap.put(shardRouting, shardStats);
            shardRoutings.add(shardRouting);
        }

        IndexStats indexStats = mock(IndexStats.class);
        when(indexStats.getPrimaries()).thenReturn(new CommonStats());
        when(indexStats.getTotal()).thenReturn(new CommonStats());

        IndicesStatsResponse stats = mock(IndicesStatsResponse.class);
        when(stats.asMap()).thenReturn(shardStatsMap);

        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.get(localNode.getId())).thenReturn(localNode);

        ClusterStateResponse state = mock(ClusterStateResponse.class);
        RoutingTable routingTable = mock(RoutingTable.class);
        when(routingTable.allShards()).thenReturn(shardRoutings);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(state.getState()).thenReturn(clusterState);

        final RestShardsAction action = new RestShardsAction();
        final Table table = action.buildTable(new FakeRestRequest(), state, stats);

        // now, verify the table is correct
        List<Table.Cell> headers = table.getHeaders();
        assertThat(headers.get(0).value, equalTo("index"));
        assertThat(headers.get(1).value, equalTo("shard"));
        assertThat(headers.get(2).value, equalTo("prirep"));
        assertThat(headers.get(3).value, equalTo("state"));
        assertThat(headers.get(4).value, equalTo("docs"));
        assertThat(headers.get(5).value, equalTo("store"));
        assertThat(headers.get(6).value, equalTo("ip"));
        assertThat(headers.get(7).value, equalTo("id"));
        assertThat(headers.get(8).value, equalTo("node"));

        final List<List<Table.Cell>> rows = table.getRows();
        assertThat(rows.size(), equalTo(numShards));

        Iterator<ShardRouting> shardRoutingsIt = shardRoutings.iterator();
        for (final List<Table.Cell> row : rows) {
            ShardRouting shardRouting = shardRoutingsIt.next();
            ShardStats shardStats = shardStatsMap.get(shardRouting);
            assertThat(row.get(0).value, equalTo(shardRouting.getIndexName()));
            assertThat(row.get(1).value, equalTo(shardRouting.getId()));
            assertThat(row.get(2).value, equalTo(shardRouting.primary() ? "p" : "r"));
            assertThat(row.get(3).value, equalTo(shardRouting.state()));
            assertThat(row.get(6).value, equalTo(localNode.getHostAddress()));
            assertThat(row.get(7).value, equalTo(localNode.getId()));
            assertThat(row.get(69).value, equalTo(shardStats.getDataPath()));
            assertThat(row.get(70).value, equalTo(shardStats.getStatePath()));
        }
    }
}
