/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_WARM_NODE_ROLE;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;

public class ClusterBalanceStatsTests extends ESAllocationTestCase {

    public void testStatsForSingleTierClusterWithNoForecasts() {

        var clusterState = createClusterState(
            List.of(
                newNode("node-1", "node-1", Set.of(DATA_CONTENT_NODE_ROLE)),
                newNode("node-2", "node-2", Set.of(DATA_CONTENT_NODE_ROLE)),
                newNode("node-3", "node-3", Set.of(DATA_CONTENT_NODE_ROLE))
            ),
            List.of(
                startedIndex("index-1", null, null, "node-1", "node-2"),
                startedIndex("index-2", null, null, "node-2", "node-3"),
                startedIndex("index-3", null, null, "node-3", "node-1")
            )
        );

        var stats = ClusterBalanceStats.createFrom(clusterState, TEST_WRITE_LOAD_FORECASTER);

        assertThat(
            stats,
            equalTo(
                new ClusterBalanceStats(
                    Map.of(
                        DATA_CONTENT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new ClusterBalanceStats.MetricStats(6.0, 2.0, 2.0, 2.0, 0.0),
                            new ClusterBalanceStats.MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new ClusterBalanceStats.MetricStats(0.0, 0.0, 0.0, 0.0, 0.0)
                        )
                    ),
                    Map.of(
                        "node-1",
                        new ClusterBalanceStats.NodeBalanceStats(2, 0.0, 0L),
                        "node-2",
                        new ClusterBalanceStats.NodeBalanceStats(2, 0.0, 0L),
                        "node-3",
                        new ClusterBalanceStats.NodeBalanceStats(2, 0.0, 0L)
                    )
                )
            )
        );
    }

    public void testStatsForSingleTierClusterWithForecasts() {

        var clusterState = createClusterState(
            List.of(
                newNode("node-1", "node-1", Set.of(DATA_CONTENT_NODE_ROLE)),
                newNode("node-2", "node-2", Set.of(DATA_CONTENT_NODE_ROLE)),
                newNode("node-3", "node-3", Set.of(DATA_CONTENT_NODE_ROLE))
            ),
            List.of(
                startedIndex("index-1", 1.5, 8L, "node-1", "node-2"),
                startedIndex("index-2", 2.5, 4L, "node-2", "node-3"),
                startedIndex("index-3", 2.0, 6L, "node-3", "node-1")
            )
        );

        var stats = ClusterBalanceStats.createFrom(clusterState, TEST_WRITE_LOAD_FORECASTER);

        assertThat(
            stats,
            equalTo(
                new ClusterBalanceStats(
                    Map.of(
                        DATA_CONTENT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new ClusterBalanceStats.MetricStats(6.0, 2.0, 2.0, 2.0, 0.0),
                            new ClusterBalanceStats.MetricStats(12.0, 3.5, 4.5, 4.0, stdDev(3.5, 4.0, 4.5)),
                            new ClusterBalanceStats.MetricStats(36.0, 10.0, 14.0, 12.0, stdDev(10.0, 12.0, 14.0))
                        )
                    ),
                    Map.of(
                        "node-1",
                        new ClusterBalanceStats.NodeBalanceStats(2, 3.5, 14L),
                        "node-2",
                        new ClusterBalanceStats.NodeBalanceStats(2, 4.0, 12L),
                        "node-3",
                        new ClusterBalanceStats.NodeBalanceStats(2, 4.5, 10L)
                    )
                )
            )
        );
    }

    public void testStatsForHotWarmClusterWithForecasts() {

        var clusterState = createClusterState(
            List.of(
                newNode("node-hot-1", "node-hot-1", Set.of(DATA_CONTENT_NODE_ROLE, DATA_HOT_NODE_ROLE)),
                newNode("node-hot-2", "node-hot-2", Set.of(DATA_CONTENT_NODE_ROLE, DATA_HOT_NODE_ROLE)),
                newNode("node-hot-3", "node-hot-3", Set.of(DATA_CONTENT_NODE_ROLE, DATA_HOT_NODE_ROLE)),
                newNode("node-warm-1", "node-warm-1", Set.of(DATA_WARM_NODE_ROLE)),
                newNode("node-warm-2", "node-warm-2", Set.of(DATA_WARM_NODE_ROLE)),
                newNode("node-warm-3", "node-warm-3", Set.of(DATA_WARM_NODE_ROLE))
            ),
            List.of(
                startedIndex("index-hot-1", 4.0, 4L, "node-hot-1", "node-hot-2", "node-hot-3"),
                startedIndex("index-hot-2", 2.0, 6L, "node-hot-1", "node-hot-2"),
                startedIndex("index-hot-3", 2.5, 6L, "node-hot-1", "node-hot-3"),
                startedIndex("index-warm-1", 0.0, 12L, "node-warm-1", "node-warm-2"),
                startedIndex("index-warm-2", 0.0, 18L, "node-warm-3")
            )
        );

        var stats = ClusterBalanceStats.createFrom(clusterState, TEST_WRITE_LOAD_FORECASTER);

        assertThat(
            stats,
            equalTo(
                new ClusterBalanceStats(
                    Map.of(
                        DATA_CONTENT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new ClusterBalanceStats.MetricStats(7.0, 2.0, 3.0, 7.0 / 3, stdDev(3.0, 2.0, 2.0)),
                            new ClusterBalanceStats.MetricStats(21.0, 6.0, 8.5, 7.0, stdDev(6.0, 8.5, 6.5)),
                            new ClusterBalanceStats.MetricStats(36.0, 10.0, 16.0, 12.0, stdDev(10.0, 10.0, 16.0))
                        ),
                        DATA_HOT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new ClusterBalanceStats.MetricStats(7.0, 2.0, 3.0, 7.0 / 3, stdDev(3.0, 2.0, 2.0)),
                            new ClusterBalanceStats.MetricStats(21.0, 6.0, 8.5, 7.0, stdDev(6.0, 8.5, 6.5)),
                            new ClusterBalanceStats.MetricStats(36.0, 10.0, 16.0, 12.0, stdDev(10.0, 10.0, 16.0))
                        ),
                        DATA_WARM_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new ClusterBalanceStats.MetricStats(3.0, 1.0, 1.0, 1.0, 0.0),
                            new ClusterBalanceStats.MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new ClusterBalanceStats.MetricStats(42.0, 12.0, 18.0, 14.0, stdDev(12.0, 12.0, 18.0))
                        )
                    ),
                    Map.of(
                        "node-hot-1",
                        new ClusterBalanceStats.NodeBalanceStats(3, 8.5, 16L),
                        "node-hot-2",
                        new ClusterBalanceStats.NodeBalanceStats(2, 6.0, 10L),
                        "node-hot-3",
                        new ClusterBalanceStats.NodeBalanceStats(2, 6.5, 10L),
                        "node-warm-1",
                        new ClusterBalanceStats.NodeBalanceStats(1, 0.0, 12L),
                        "node-warm-2",
                        new ClusterBalanceStats.NodeBalanceStats(1, 0.0, 12L),
                        "node-warm-3",
                        new ClusterBalanceStats.NodeBalanceStats(1, 0.0, 18L)
                    )
                )
            )
        );
    }

    public void testStatsForNoIndicesInTier() {

        var clusterState = createClusterState(
            List.of(
                newNode("node-1", "node-1", Set.of(DATA_CONTENT_NODE_ROLE)),
                newNode("node-2", "node-2", Set.of(DATA_CONTENT_NODE_ROLE)),
                newNode("node-3", "node-3", Set.of(DATA_CONTENT_NODE_ROLE))
            ),
            List.of()
        );

        var stats = ClusterBalanceStats.createFrom(clusterState, TEST_WRITE_LOAD_FORECASTER);

        assertThat(
            stats,
            equalTo(
                new ClusterBalanceStats(
                    Map.of(
                        DATA_CONTENT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new ClusterBalanceStats.MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new ClusterBalanceStats.MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new ClusterBalanceStats.MetricStats(0.0, 0.0, 0.0, 0.0, 0.0)
                        )
                    ),
                    Map.of(
                        "node-1",
                        new ClusterBalanceStats.NodeBalanceStats(0, 0.0, 0L),
                        "node-2",
                        new ClusterBalanceStats.NodeBalanceStats(0, 0.0, 0L),
                        "node-3",
                        new ClusterBalanceStats.NodeBalanceStats(0, 0.0, 0L)
                    )
                )
            )
        );
    }

    private static ClusterState createClusterState(List<DiscoveryNode> nodes, List<Tuple<IndexMetadata.Builder, String[]>> indices) {
        var discoveryNodesBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodes) {
            discoveryNodesBuilder.add(node);
        }

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();
        for (var index : indices) {
            var indexMetadata = index.v1()
                .settings(settings(Version.CURRENT))
                .numberOfShards(index.v2().length)
                .numberOfReplicas(0)
                .build();
            metadataBuilder.put(indexMetadata, false);
            var indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int shardId = 0; shardId < index.v2().length; shardId++) {
                indexRoutingTableBuilder.addShard(
                    newShardRouting(new ShardId(indexMetadata.getIndex(), shardId), index.v2()[shardId], true, STARTED)
                );
            }
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();
    }

    private static Tuple<IndexMetadata.Builder, String[]> startedIndex(
        String indexName,
        @Nullable Double indexWriteLoadForecast,
        @Nullable Long shardSizeInBytesForecast,
        String... nodeId
    ) {
        return Tuple.tuple(
            IndexMetadata.builder(indexName)
                .indexWriteLoadForecast(indexWriteLoadForecast)
                .shardSizeInBytesForecast(shardSizeInBytesForecast),
            nodeId
        );
    }

    private static double stdDev(double... data) {
        double total = 0.0;
        double total2 = 0.0;
        int count = data.length;
        for (double d : data) {
            total += d;
            total2 += Math.pow(d, 2);
        }
        return Math.sqrt(total2 / count - Math.pow(total / count, 2));
    }
}
