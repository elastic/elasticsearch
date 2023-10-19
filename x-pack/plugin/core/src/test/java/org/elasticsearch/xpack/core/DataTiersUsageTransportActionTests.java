/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.cluster.routing.allocation.stats.NodeDataTiersUsage;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DataTiersUsageTransportActionTests extends ESTestCase {

    private long byteSize;
    private long docCount;

    @Before
    public void setup() {
        byteSize = randomLongBetween(1024L, 1024L * 1024L * 1024L * 30L); // 1 KB to 30 GB
        docCount = randomLongBetween(100L, 100000000L); // one hundred to one hundred million
    }

    public void testCalculateMAD() {
        assertThat(DataTiersUsageTransportAction.computeMedianAbsoluteDeviation(TDigestState.create(10)), equalTo(0L));

        TDigestState sketch = TDigestState.create(randomDoubleBetween(1, 1000, false));
        sketch.add(1);
        sketch.add(1);
        sketch.add(2);
        sketch.add(2);
        sketch.add(4);
        sketch.add(6);
        sketch.add(9);
        assertThat(DataTiersUsageTransportAction.computeMedianAbsoluteDeviation(sketch), equalTo(1L));
    }

    public void testCalculateStatsNoTiers() {
        // Nodes: 0 Tiered Nodes, 1 Data Node
        DiscoveryNode leader = newNode(0, DiscoveryNodeRole.MASTER_ROLE);
        DiscoveryNode dataNode1 = newNode(1, DiscoveryNodeRole.DATA_ROLE);

        List<NodeDataTiersUsage> nodeDataTiersUsages = List.of(
            new NodeDataTiersUsage(leader, Map.of()),
            new NodeDataTiersUsage(dataNode1, Map.of())
        );
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeDataTiersUsages
        );

        // Verify - No results when no tiers present
        assertThat(tierSpecificStats.size(), is(0));
    }

    public void testCalculateStatsTieredNodesOnly() {
        // Nodes: 1 Data, 1 Hot, 1 Warm, 1 Cold, 1 Frozen
        DiscoveryNode leader = newNode(0, DiscoveryNodeRole.MASTER_ROLE);
        DiscoveryNode dataNode1 = newNode(1, DiscoveryNodeRole.DATA_ROLE);
        DiscoveryNode hotNode1 = newNode(2, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        DiscoveryNode warmNode1 = newNode(3, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        DiscoveryNode coldNode1 = newNode(4, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        DiscoveryNode frozenNode1 = newNode(5, DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);

        List<NodeDataTiersUsage> nodeDataTiersUsages = List.of(
            new NodeDataTiersUsage(leader, Map.of()),
            new NodeDataTiersUsage(dataNode1, Map.of()),
            new NodeDataTiersUsage(hotNode1, Map.of()),
            new NodeDataTiersUsage(warmNode1, Map.of()),
            new NodeDataTiersUsage(coldNode1, Map.of()),
            new NodeDataTiersUsage(frozenNode1, Map.of())
        );

        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeDataTiersUsages
        );

        // Verify - Results are present but they lack index numbers because none are tiered
        assertThat(tierSpecificStats.size(), is(4));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(1));
        assertThat(hotStats.indexCount, is(0));
        assertThat(hotStats.totalShardCount, is(0));
        assertThat(hotStats.docCount, is(0L));
        assertThat(hotStats.totalByteCount, is(0L));
        assertThat(hotStats.primaryShardCount, is(0));
        assertThat(hotStats.primaryByteCount, is(0L));
        assertThat(hotStats.primaryByteCountMedian, is(0L)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(1));
        assertThat(warmStats.indexCount, is(0));
        assertThat(warmStats.totalShardCount, is(0));
        assertThat(warmStats.docCount, is(0L));
        assertThat(warmStats.totalByteCount, is(0L));
        assertThat(warmStats.primaryShardCount, is(0));
        assertThat(warmStats.primaryByteCount, is(0L));
        assertThat(warmStats.primaryByteCountMedian, is(0L)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats coldStats = tierSpecificStats.get(DataTier.DATA_COLD);
        assertThat(coldStats, is(notNullValue()));
        assertThat(coldStats.nodeCount, is(1));
        assertThat(coldStats.indexCount, is(0));
        assertThat(coldStats.totalShardCount, is(0));
        assertThat(coldStats.docCount, is(0L));
        assertThat(coldStats.totalByteCount, is(0L));
        assertThat(coldStats.primaryShardCount, is(0));
        assertThat(coldStats.primaryByteCount, is(0L));
        assertThat(coldStats.primaryByteCountMedian, is(0L)); // All same size
        assertThat(coldStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats frozenStats = tierSpecificStats.get(DataTier.DATA_FROZEN);
        assertThat(frozenStats, is(notNullValue()));
        assertThat(frozenStats.nodeCount, is(1));
        assertThat(frozenStats.indexCount, is(0));
        assertThat(frozenStats.totalShardCount, is(0));
        assertThat(frozenStats.docCount, is(0L));
        assertThat(frozenStats.totalByteCount, is(0L));
        assertThat(frozenStats.primaryShardCount, is(0));
        assertThat(frozenStats.primaryByteCount, is(0L));
        assertThat(frozenStats.primaryByteCountMedian, is(0L)); // All same size
        assertThat(frozenStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    public void testCalculateStatsTieredIndicesOnly() {
        // Nodes: 3 Data, 0 Tiered - Only hosting indices on generic data nodes
        int nodeId = 0;
        DiscoveryNode leader = newNode(nodeId++, DiscoveryNodeRole.MASTER_ROLE);
        DiscoveryNode dataNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_ROLE);
        DiscoveryNode dataNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_ROLE);
        DiscoveryNode dataNode3 = newNode(nodeId, DiscoveryNodeRole.DATA_ROLE);

        String hotIndex = "hot_index_1";
        String warmIndex1 = "warm_index_1";
        String warmIndex2 = "warm_index_2";
        String coldIndex1 = "cold_index_1";
        String coldIndex2 = "cold_index_2";
        String coldIndex3 = "cold_index_3";

        List<NodeDataTiersUsage> nodeDataTiersUsages = List.of(
            new NodeDataTiersUsage(leader, Map.of()),
            new NodeDataTiersUsage(
                dataNode1,
                Map.of(
                    DataTier.DATA_HOT,
                    createStats(Set.of(hotIndex), 1, 2, docCount, byteSize),
                    DataTier.DATA_WARM,
                    createStats(Set.of(warmIndex1, warmIndex2), 0, 2, docCount, byteSize),
                    DataTier.DATA_COLD,
                    createStats(Set.of(coldIndex1), 1, 1, docCount, byteSize)
                )
            ),
            new NodeDataTiersUsage(
                dataNode2,
                Map.of(
                    DataTier.DATA_HOT,
                    createStats(Set.of(hotIndex), 1, 2, docCount, byteSize),
                    DataTier.DATA_WARM,
                    createStats(Set.of(warmIndex1), 1, 1, docCount, byteSize),
                    DataTier.DATA_COLD,
                    createStats(Set.of(coldIndex2), 1, 1, docCount, byteSize)
                )
            ),
            new NodeDataTiersUsage(
                dataNode3,
                Map.of(
                    DataTier.DATA_HOT,
                    createStats(Set.of(hotIndex), 1, 2, docCount, byteSize),
                    DataTier.DATA_WARM,
                    createStats(Set.of(warmIndex2), 1, 1, docCount, byteSize),
                    DataTier.DATA_COLD,
                    createStats(Set.of(coldIndex3), 1, 1, docCount, byteSize)
                )
            )
        );
        // Calculate usage
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeDataTiersUsages
        );

        // Verify - Index stats exist for the tiers, but no tiered nodes are found
        assertThat(tierSpecificStats.size(), is(3));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(0));
        assertThat(hotStats.indexCount, is(1));
        assertThat(hotStats.totalShardCount, is(6));
        assertThat(hotStats.docCount, is(6 * docCount));
        assertThat(hotStats.totalByteCount, is(6 * byteSize));
        assertThat(hotStats.primaryShardCount, is(3));
        assertThat(hotStats.primaryByteCount, is(3 * byteSize));
        assertThat(hotStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(0));
        assertThat(warmStats.indexCount, is(2));
        assertThat(warmStats.totalShardCount, is(4));
        assertThat(warmStats.docCount, is(4 * docCount));
        assertThat(warmStats.totalByteCount, is(4 * byteSize));
        assertThat(warmStats.primaryShardCount, is(2));
        assertThat(warmStats.primaryByteCount, is(2 * byteSize));
        assertThat(warmStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats coldStats = tierSpecificStats.get(DataTier.DATA_COLD);
        assertThat(coldStats, is(notNullValue()));
        assertThat(coldStats.nodeCount, is(0));
        assertThat(coldStats.indexCount, is(3));
        assertThat(coldStats.totalShardCount, is(3));
        assertThat(coldStats.docCount, is(3 * docCount));
        assertThat(coldStats.totalByteCount, is(3 * byteSize));
        assertThat(coldStats.primaryShardCount, is(3));
        assertThat(coldStats.primaryByteCount, is(3 * byteSize));
        assertThat(coldStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(coldStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    public void testCalculateStatsReasonableCase() {
        // Nodes: 3 Hot, 5 Warm, 1 Cold
        int nodeId = 0;
        DiscoveryNode leader = newNode(nodeId++, DiscoveryNodeRole.MASTER_ROLE);
        DiscoveryNode hotNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        DiscoveryNode hotNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        DiscoveryNode hotNode3 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        DiscoveryNode warmNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        DiscoveryNode warmNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        DiscoveryNode warmNode3 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        DiscoveryNode warmNode4 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        DiscoveryNode warmNode5 = newNode(nodeId++, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        DiscoveryNode coldNode1 = newNode(nodeId, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);

        String hotIndex1 = "hot_index_1";
        String warmIndex1 = "warm_index_1";
        String warmIndex2 = "warm_index_2";
        String coldIndex1 = "cold_index_1";
        String coldIndex2 = "cold_index_2";
        String coldIndex3 = "cold_index_3";

        List<NodeDataTiersUsage> nodeDataTiersUsages = List.of(
            new NodeDataTiersUsage(leader, Map.of()),
            new NodeDataTiersUsage(hotNode1, Map.of(DataTier.DATA_HOT, createStats(Set.of(hotIndex1), 1, 2, docCount, byteSize))),
            new NodeDataTiersUsage(hotNode2, Map.of(DataTier.DATA_HOT, createStats(Set.of(hotIndex1), 1, 2, docCount, byteSize))),
            new NodeDataTiersUsage(hotNode3, Map.of(DataTier.DATA_HOT, createStats(Set.of(hotIndex1), 1, 2, docCount, byteSize))),
            new NodeDataTiersUsage(warmNode1, Map.of(DataTier.DATA_WARM, createStats(Set.of(warmIndex1), 1, 1, docCount, byteSize))),
            new NodeDataTiersUsage(warmNode2, Map.of(DataTier.DATA_WARM, createStats(Set.of(warmIndex1), 0, 1, docCount, byteSize))),
            new NodeDataTiersUsage(warmNode3, Map.of(DataTier.DATA_WARM, createStats(Set.of(warmIndex2), 1, 1, docCount, byteSize))),
            new NodeDataTiersUsage(warmNode4, Map.of(DataTier.DATA_WARM, createStats(Set.of(warmIndex2), 0, 1, docCount, byteSize))),
            new NodeDataTiersUsage(warmNode5, Map.of()),
            new NodeDataTiersUsage(
                coldNode1,
                Map.of(DataTier.DATA_COLD, createStats(Set.of(coldIndex1, coldIndex2, coldIndex3), 3, 3, docCount, byteSize))
            )

        );
        // Calculate usage
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeDataTiersUsages
        );

        // Verify - Node and Index stats are both collected
        assertThat(tierSpecificStats.size(), is(3));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(3));
        assertThat(hotStats.indexCount, is(1));
        assertThat(hotStats.totalShardCount, is(6));
        assertThat(hotStats.docCount, is(6 * docCount));
        assertThat(hotStats.totalByteCount, is(6 * byteSize));
        assertThat(hotStats.primaryShardCount, is(3));
        assertThat(hotStats.primaryByteCount, is(3 * byteSize));
        assertThat(hotStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(5));
        assertThat(warmStats.indexCount, is(2));
        assertThat(warmStats.totalShardCount, is(4));
        assertThat(warmStats.docCount, is(4 * docCount));
        assertThat(warmStats.totalByteCount, is(4 * byteSize));
        assertThat(warmStats.primaryShardCount, is(2));
        assertThat(warmStats.primaryByteCount, is(2 * byteSize));
        assertThat(warmStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats coldStats = tierSpecificStats.get(DataTier.DATA_COLD);
        assertThat(coldStats, is(notNullValue()));
        assertThat(coldStats.nodeCount, is(1));
        assertThat(coldStats.indexCount, is(3));
        assertThat(coldStats.totalShardCount, is(3));
        assertThat(coldStats.docCount, is(3 * docCount));
        assertThat(coldStats.totalByteCount, is(3 * byteSize));
        assertThat(coldStats.primaryShardCount, is(3));
        assertThat(coldStats.primaryByteCount, is(3 * byteSize));
        assertThat(coldStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(coldStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    public void testCalculateStatsMixedTiers() {
        // Nodes: 3 Hot+Warm - Nodes that are marked as part of multiple tiers
        int nodeId = 0;
        DiscoveryNode leader = newNode(nodeId++, DiscoveryNodeRole.MASTER_ROLE);

        DiscoveryNode mixedNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        DiscoveryNode mixedNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        DiscoveryNode mixedNode3 = newNode(nodeId, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);

        String hotIndex1 = "hot_index_1";
        String warmIndex1 = "warm_index_1";
        String warmIndex2 = "warm_index_2";

        List<NodeDataTiersUsage> nodeDataTiersUsages = List.of(
            new NodeDataTiersUsage(leader, Map.of()),
            new NodeDataTiersUsage(
                mixedNode1,
                Map.of(
                    DataTier.DATA_HOT,
                    createStats(Set.of(hotIndex1), 1, 2, docCount, byteSize),
                    DataTier.DATA_WARM,
                    createStats(Set.of(warmIndex1, warmIndex2), 1, 2, docCount, byteSize)
                )
            ),
            new NodeDataTiersUsage(
                mixedNode2,
                Map.of(
                    DataTier.DATA_HOT,
                    createStats(Set.of(hotIndex1), 1, 2, docCount, byteSize),
                    DataTier.DATA_WARM,
                    createStats(Set.of(warmIndex1), 0, 1, docCount, byteSize)
                )
            ),
            new NodeDataTiersUsage(
                mixedNode3,
                Map.of(
                    DataTier.DATA_HOT,
                    createStats(Set.of(hotIndex1), 1, 2, docCount, byteSize),
                    DataTier.DATA_WARM,
                    createStats(Set.of(warmIndex2), 1, 1, docCount, byteSize)
                )
            )
        );

        // Calculate usage
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeDataTiersUsages
        );

        // Verify - Index stats are separated by their preferred tier, instead of counted
        // toward multiple tiers based on their current routing. Nodes are counted for each tier they are in.
        assertThat(tierSpecificStats.size(), is(2));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(3));
        assertThat(hotStats.indexCount, is(1));
        assertThat(hotStats.totalShardCount, is(6));
        assertThat(hotStats.docCount, is(6 * docCount));
        assertThat(hotStats.totalByteCount, is(6 * byteSize));
        assertThat(hotStats.primaryShardCount, is(3));
        assertThat(hotStats.primaryByteCount, is(3 * byteSize));
        assertThat(hotStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(3));
        assertThat(warmStats.indexCount, is(2));
        assertThat(warmStats.totalShardCount, is(4));
        assertThat(warmStats.docCount, is(4 * docCount));
        assertThat(warmStats.totalByteCount, is(4 * byteSize));
        assertThat(warmStats.primaryShardCount, is(2));
        assertThat(warmStats.primaryByteCount, is(2 * byteSize));
        assertThat(warmStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    public void testCalculateStatsStuckInWrongTier() {
        // Nodes: 3 Hot, 0 Warm - Emulating indices stuck on non-preferred tiers
        int nodeId = 0;
        DiscoveryNode leader = newNode(nodeId++, DiscoveryNodeRole.MASTER_ROLE);
        DiscoveryNode hotNode1 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        DiscoveryNode hotNode2 = newNode(nodeId++, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        DiscoveryNode hotNode3 = newNode(nodeId, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);

        String hotIndex1 = "hot_index_1";
        String warmIndex1 = "warm_index_1";

        List<NodeDataTiersUsage> nodeDataTiersUsages = List.of(
            new NodeDataTiersUsage(leader, Map.of()),
            new NodeDataTiersUsage(
                hotNode1,
                Map.of(
                    DataTier.DATA_HOT,
                    createStats(Set.of(hotIndex1), 1, 2, docCount, byteSize),
                    DataTier.DATA_WARM,
                    createStats(Set.of(warmIndex1), 1, 1, docCount, byteSize)
                )
            ),
            new NodeDataTiersUsage(
                hotNode2,
                Map.of(
                    DataTier.DATA_HOT,
                    createStats(Set.of(hotIndex1), 1, 2, docCount, byteSize),
                    DataTier.DATA_WARM,
                    createStats(Set.of(warmIndex1), 0, 1, docCount, byteSize)
                )
            ),
            new NodeDataTiersUsage(hotNode3, Map.of(DataTier.DATA_HOT, createStats(Set.of(hotIndex1), 1, 2, docCount, byteSize)))
        );

        // Calculate usage
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = DataTiersUsageTransportAction.calculateStats(
            nodeDataTiersUsages
        );

        // Verify - Warm indices are still calculated separately from Hot ones, despite Warm nodes missing
        assertThat(tierSpecificStats.size(), is(2));

        DataTiersFeatureSetUsage.TierSpecificStats hotStats = tierSpecificStats.get(DataTier.DATA_HOT);
        assertThat(hotStats, is(notNullValue()));
        assertThat(hotStats.nodeCount, is(3));
        assertThat(hotStats.indexCount, is(1));
        assertThat(hotStats.totalShardCount, is(6));
        assertThat(hotStats.docCount, is(6 * docCount));
        assertThat(hotStats.totalByteCount, is(6 * byteSize));
        assertThat(hotStats.primaryShardCount, is(3));
        assertThat(hotStats.primaryByteCount, is(3 * byteSize));
        assertThat(hotStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(hotStats.primaryShardBytesMAD, is(0L)); // All same size

        DataTiersFeatureSetUsage.TierSpecificStats warmStats = tierSpecificStats.get(DataTier.DATA_WARM);
        assertThat(warmStats, is(notNullValue()));
        assertThat(warmStats.nodeCount, is(0));
        assertThat(warmStats.indexCount, is(1));
        assertThat(warmStats.totalShardCount, is(2));
        assertThat(warmStats.docCount, is(2 * docCount));
        assertThat(warmStats.totalByteCount, is(2 * byteSize));
        assertThat(warmStats.primaryShardCount, is(1));
        assertThat(warmStats.primaryByteCount, is(byteSize));
        assertThat(warmStats.primaryByteCountMedian, is(byteSize)); // All same size
        assertThat(warmStats.primaryShardBytesMAD, is(0L)); // All same size
    }

    private static DiscoveryNode newNode(int nodeId, DiscoveryNodeRole... roles) {
        return DiscoveryNodeUtils.builder("node_" + nodeId).roles(Set.of(roles)).build();
    }

    private NodeDataTiersUsage.UsageStats createStats(
        Set<String> indices,
        int primaryShardCount,
        int totalNumberOfShards,
        long docCount,
        long byteSize
    ) {
        return new NodeDataTiersUsage.UsageStats(
            indices,
            primaryShardCount > 0 ? IntStream.range(0, primaryShardCount).mapToObj(i -> byteSize).toList() : List.of(),
            totalNumberOfShards,
            totalNumberOfShards * docCount,
            totalNumberOfShards * byteSize
        );
    }
}
