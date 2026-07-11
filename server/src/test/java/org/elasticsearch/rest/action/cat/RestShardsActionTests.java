/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectIdResolver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexingStats;
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

    private DiscoveryNode localNode;
    private List<ShardRouting> shardRoutings;
    private ClusterStateResponse clusterStateResponse;
    private IndicesStatsResponse indicesStatsResponse;
    private ProjectIdResolver projectIdResolver;

    public void testBuildTable() {
        mockShardStats(randomBoolean());

        final RestShardsAction action = new RestShardsAction(projectIdResolver);
        final Table table = action.buildTable(new FakeRestRequest(), clusterStateResponse, indicesStatsResponse);

        // now, verify the table is correct
        List<Table.Cell> headers = table.getHeaders();
        assertThat(headers.get(0).value, equalTo("index"));
        assertThat(headers.get(1).value, equalTo("shard"));
        assertThat(headers.get(2).value, equalTo("prirep"));
        assertThat(headers.get(3).value, equalTo("state"));
        assertThat(headers.get(4).value, equalTo("docs"));
        assertThat(headers.get(5).value, equalTo("store"));
        assertThat(headers.get(6).value, equalTo("dataset"));
        assertThat(headers.get(7).value, equalTo("ip"));
        assertThat(headers.get(8).value, equalTo("id"));
        assertThat(headers.get(9).value, equalTo("node"));
        assertThat(headers.get(10).value, equalTo("node.role"));
        assertThat(headers.get(11).value, equalTo("tier"));
        assertThat(headers.get(12).value, equalTo("unassigned.reason"));

        final List<List<Table.Cell>> rows = table.getRows();
        assertThat(rows.size(), equalTo(shardRoutings.size()));

        Iterator<ShardRouting> shardRoutingsIt = shardRoutings.iterator();
        for (final List<Table.Cell> row : rows) {
            ShardRouting shardRouting = shardRoutingsIt.next();
            ShardStats shardStats = indicesStatsResponse.asMap().get(shardRouting);
            assertThat(row.get(0).value, equalTo(shardRouting.getIndexName()));
            assertThat(row.get(1).value, equalTo(shardRouting.getId()));
            assertThat(row.get(2).value, equalTo(shardRouting.primary() ? "p" : "r"));
            assertThat(row.get(3).value, equalTo(shardRouting.state()));
            assertThat(row.get(7).value, equalTo(localNode.getHostAddress()));
            assertThat(row.get(8).value, equalTo(localNode.getId()));
            assertThat(row.get(72).value, equalTo(shardStats.getDataPath()));
            assertThat(row.get(73).value, equalTo(shardStats.getStatePath()));
        }
    }

    public void testShardStatsForIndexing() {
        mockShardStats(true);

        final RestShardsAction action = new RestShardsAction(projectIdResolver);
        final Table table = action.buildTable(new FakeRestRequest(), clusterStateResponse, indicesStatsResponse);

        // now, verify the table is correct
        List<Table.Cell> headers = table.getHeaders();
        // the two new columns (node.role at 10, tier at 11) shift all subsequent headers by 2
        assertThat(headers.get(31).value, equalTo("indexing.delete_current"));
        assertThat(headers.get(32).value, equalTo("indexing.delete_time"));
        assertThat(headers.get(33).value, equalTo("indexing.delete_total"));
        assertThat(headers.get(34).value, equalTo("indexing.index_current"));
        assertThat(headers.get(35).value, equalTo("indexing.index_time"));
        assertThat(headers.get(36).value, equalTo("indexing.index_total"));
        assertThat(headers.get(37).value, equalTo("indexing.index_failed"));
        assertThat(headers.get(38).value, equalTo("indexing.index_failed_due_to_version_conflict"));

        final List<List<Table.Cell>> rows = table.getRows();
        assertThat(rows.size(), equalTo(shardRoutings.size()));

        Iterator<ShardRouting> shardRoutingsIt = shardRoutings.iterator();
        for (final List<Table.Cell> row : rows) {
            ShardRouting shardRouting = shardRoutingsIt.next();
            ShardStats shardStats = indicesStatsResponse.asMap().get(shardRouting);
            assertThat(row.get(31).value, equalTo(shardStats.getStats().getIndexing().getTotal().getDeleteCurrent()));
            assertThat(row.get(32).value, equalTo(shardStats.getStats().getIndexing().getTotal().getDeleteTime()));
            assertThat(row.get(33).value, equalTo(shardStats.getStats().getIndexing().getTotal().getDeleteCount()));
            assertThat(row.get(34).value, equalTo(shardStats.getStats().getIndexing().getTotal().getIndexCurrent()));
            assertThat(row.get(35).value, equalTo(shardStats.getStats().getIndexing().getTotal().getIndexTime()));
            assertThat(row.get(36).value, equalTo(shardStats.getStats().getIndexing().getTotal().getIndexCount()));
            assertThat(row.get(37).value, equalTo(shardStats.getStats().getIndexing().getTotal().getIndexFailedCount()));
            assertThat(
                row.get(38).value,
                equalTo(shardStats.getStats().getIndexing().getTotal().getIndexFailedDueToVersionConflictCount())
            );
        }
    }

    private void mockShardStats(boolean includeCommonStats) {
        final int numShards = randomIntBetween(1, 5);
        this.localNode = DiscoveryNodeUtils.create("local");
        this.shardRoutings = new ArrayList<>(numShards);
        Map<ShardRouting, ShardStats> shardStatsMap = new HashMap<>();
        String indexName = "index";
        for (int i = 0; i < numShards; i++) {
            ShardRoutingState shardRoutingState = ShardRoutingState.fromValue((byte) randomIntBetween(2, 3));
            ShardRouting shardRouting = TestShardRouting.newShardRouting(
                indexName,
                i,
                localNode.getId(),
                randomBoolean(),
                shardRoutingState
            );
            Path path = createTempDir().resolve("indices")
                .resolve(shardRouting.shardId().getIndex().getUUID())
                .resolve(String.valueOf(shardRouting.shardId().id()));
            CommonStats commonStats = null;
            if (includeCommonStats) {
                commonStats = new CommonStats(randomFrom(CommonStatsFlags.ALL, new CommonStatsFlags(CommonStatsFlags.Flag.Indexing)));
                commonStats.indexing.add(
                    new IndexingStats(
                        new IndexingStats.Stats(
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomBoolean(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomDoubleBetween(0.0, 1.0, true),
                            randomDoubleBetween(0.0, 1.0, true)
                        )
                    )
                );
            }
            ShardStats shardStats = new ShardStats(
                shardRouting,
                new ShardPath(false, path, path, shardRouting.shardId()),
                commonStats,
                null,
                null,
                null,
                false,
                0
            );
            shardStatsMap.put(shardRouting, shardStats);
            shardRoutings.add(shardRouting);
        }

        this.indicesStatsResponse = mock(IndicesStatsResponse.class);
        when(this.indicesStatsResponse.asMap()).thenReturn(shardStatsMap);

        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.get(localNode.getId())).thenReturn(localNode);

        // Mock IndexMetadata with a tier preference for the test index
        Index index = shardRoutings.get(0).shardId().getIndex();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.include._tier_preference", "data_hot")
                    .build()
            )
            .build();

        ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
        when(projectMetadata.index(index)).thenReturn(indexMetadata);

        Metadata metadata = mock(Metadata.class);
        when(metadata.getProject(Metadata.DEFAULT_PROJECT_ID)).thenReturn(projectMetadata);

        this.projectIdResolver = () -> Metadata.DEFAULT_PROJECT_ID;

        this.clusterStateResponse = mock(ClusterStateResponse.class);
        RoutingTable routingTable = mock(RoutingTable.class);
        when(routingTable.allShardsIterator()).thenReturn(shardRoutings);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterStateResponse.getState()).thenReturn(clusterState);
    }
}
