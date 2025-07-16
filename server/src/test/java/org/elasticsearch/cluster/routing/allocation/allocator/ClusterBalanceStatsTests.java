/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.allocator.ClusterBalanceStats.MetricStats;
import org.elasticsearch.cluster.routing.allocation.allocator.ClusterBalanceStats.NodeBalanceStats;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_WARM_NODE_ROLE;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;

public class ClusterBalanceStatsTests extends ESAllocationTestCase {

    private static final DiscoveryNode NODE1 = newNode("node-1", "node-1", Set.of(DATA_CONTENT_NODE_ROLE));
    private static final DiscoveryNode NODE2 = newNode("node-2", "node-2", Set.of(DATA_CONTENT_NODE_ROLE));
    private static final DiscoveryNode NODE3 = newNode("node-3", "node-3", Set.of(DATA_CONTENT_NODE_ROLE));

    public void testStatsForSingleTierClusterWithNoForecasts() {
        var clusterState = createClusterState(
            List.of(NODE1, NODE2, NODE3),
            List.of(
                startedIndex("index-1", null, null, "node-1", "node-2"),
                startedIndex("index-2", null, null, "node-2", "node-3"),
                startedIndex("index-3", null, null, "node-3", "node-1")
            )
        );

        var clusterInfo = createClusterInfo(
            List.of(indexSizes("index-1", 1L, 1L), indexSizes("index-2", 2L, 2L), indexSizes("index-3", 3L, 3L))
        );

        double nodeWeight = randomDoubleBetween(-1, 1, true);
        var stats = ClusterBalanceStats.createFrom(
            clusterState,
            createDesiredBalance(clusterState, nodeWeight),
            clusterInfo,
            TEST_WRITE_LOAD_FORECASTER
        );

        assertThat(
            stats,
            equalTo(
                new ClusterBalanceStats(
                    6,
                    0,
                    Map.of(
                        DATA_CONTENT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new MetricStats(6.0, 2.0, 2.0, 2.0, 0.0),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(12.0, 3.0, 5.0, 4.0, stdDev(3.0, 5.0, 4.0)),
                            new MetricStats(12.0, 3.0, 5.0, 4.0, stdDev(3.0, 5.0, 4.0))
                        )
                    ),
                    Map.ofEntries(
                        Map.entry(
                            "node-1",
                            new NodeBalanceStats("node-1", List.of(DATA_CONTENT_NODE_ROLE.roleName()), 2, 0, 0.0, 4L, 4L, nodeWeight)
                        ),
                        Map.entry(
                            "node-2",
                            new NodeBalanceStats("node-2", List.of(DATA_CONTENT_NODE_ROLE.roleName()), 2, 0, 0.0, 3L, 3L, nodeWeight)
                        ),
                        Map.entry(
                            "node-3",
                            new NodeBalanceStats("node-3", List.of(DATA_CONTENT_NODE_ROLE.roleName()), 2, 0, 0.0, 5L, 5L, nodeWeight)
                        )
                    )
                )
            )
        );
    }

    public void testStatsForSingleTierClusterWithForecasts() {
        var clusterState = createClusterState(
            List.of(NODE1, NODE2, NODE3),
            List.of(
                startedIndex("index-1", 1.5, 8L, "node-1", "node-2"),
                startedIndex("index-2", 2.5, 4L, "node-2", "node-3"),
                startedIndex("index-3", 2.0, 6L, "node-3", "node-1")
            )
        );

        // intentionally different from forecast
        var clusterInfo = createClusterInfo(
            List.of(indexSizes("index-1", 1L, 1L), indexSizes("index-2", 2L, 2L), indexSizes("index-3", 3L, 3L))
        );

        double nodeWeight = randomDoubleBetween(-1, 1, true);
        var stats = ClusterBalanceStats.createFrom(
            clusterState,
            createDesiredBalance(clusterState, nodeWeight),
            clusterInfo,
            TEST_WRITE_LOAD_FORECASTER
        );

        assertThat(
            stats,
            equalTo(
                new ClusterBalanceStats(
                    6,
                    0,
                    Map.of(
                        DATA_CONTENT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new MetricStats(6.0, 2.0, 2.0, 2.0, 0.0),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(12.0, 3.5, 4.5, 4.0, stdDev(3.5, 4.0, 4.5)),
                            new MetricStats(36.0, 10.0, 14.0, 12.0, stdDev(10.0, 12.0, 14.0)),
                            new MetricStats(12.0, 3.0, 5.0, 4.0, stdDev(3.0, 5.0, 4.0))
                        )
                    ),
                    Map.ofEntries(
                        Map.entry(
                            "node-1",
                            new NodeBalanceStats("node-1", List.of(DATA_CONTENT_NODE_ROLE.roleName()), 2, 0, 3.5, 14L, 4L, nodeWeight)
                        ),
                        Map.entry(
                            "node-2",
                            new NodeBalanceStats("node-2", List.of(DATA_CONTENT_NODE_ROLE.roleName()), 2, 0, 4.0, 12L, 3L, nodeWeight)
                        ),
                        Map.entry(
                            "node-3",
                            new NodeBalanceStats("node-3", List.of(DATA_CONTENT_NODE_ROLE.roleName()), 2, 0, 4.5, 10L, 5L, nodeWeight)
                        )
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

        // intentionally different from forecast
        var clusterInfo = createClusterInfo(
            List.of(
                indexSizes("index-hot-1", 4L, 4L, 4L),
                indexSizes("index-hot-2", 5L, 5L),
                indexSizes("index-hot-3", 6L, 6L),
                indexSizes("index-warm-1", 12L, 12L),
                indexSizes("index-warm-2", 18L)
            )
        );

        double nodeWeight = randomDoubleBetween(-1, 1, true);
        var stats = ClusterBalanceStats.createFrom(
            clusterState,
            createDesiredBalance(clusterState, nodeWeight),
            clusterInfo,
            TEST_WRITE_LOAD_FORECASTER
        );

        var hotRoleNames = List.of(DATA_CONTENT_NODE_ROLE.roleName(), DATA_HOT_NODE_ROLE.roleName());
        var warmRoleNames = List.of(DATA_WARM_NODE_ROLE.roleName());
        assertThat(
            stats,
            equalTo(
                new ClusterBalanceStats(
                    10,
                    0,
                    Map.of(
                        DATA_CONTENT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new MetricStats(7.0, 2.0, 3.0, 7.0 / 3, stdDev(3.0, 2.0, 2.0)),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(21.0, 6.0, 8.5, 7.0, stdDev(6.0, 8.5, 6.5)),
                            new MetricStats(36.0, 10.0, 16.0, 12.0, stdDev(10.0, 10.0, 16.0)),
                            new MetricStats(34.0, 9.0, 15.0, 34.0 / 3, stdDev(9.0, 10.0, 15.0))
                        ),
                        DATA_HOT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new MetricStats(7.0, 2.0, 3.0, 7.0 / 3, stdDev(3.0, 2.0, 2.0)),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(21.0, 6.0, 8.5, 7.0, stdDev(6.0, 8.5, 6.5)),
                            new MetricStats(36.0, 10.0, 16.0, 12.0, stdDev(10.0, 10.0, 16.0)),
                            new MetricStats(34.0, 9.0, 15.0, 34.0 / 3, stdDev(9.0, 10.0, 15.0))
                        ),
                        DATA_WARM_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new MetricStats(3.0, 1.0, 1.0, 1.0, 0.0),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(42.0, 12.0, 18.0, 14.0, stdDev(12.0, 12.0, 18.0)),
                            new MetricStats(42.0, 12.0, 18.0, 14.0, stdDev(12.0, 12.0, 18.0))
                        )
                    ),
                    Map.ofEntries(
                        Map.entry("node-hot-1", new NodeBalanceStats("node-hot-1", hotRoleNames, 3, 0, 8.5, 16L, 15L, nodeWeight)),
                        Map.entry("node-hot-2", new NodeBalanceStats("node-hot-2", hotRoleNames, 2, 0, 6.0, 10L, 9L, nodeWeight)),
                        Map.entry("node-hot-3", new NodeBalanceStats("node-hot-3", hotRoleNames, 2, 0, 6.5, 10L, 10L, nodeWeight)),
                        Map.entry("node-warm-1", new NodeBalanceStats("node-warm-1", warmRoleNames, 1, 0, 0.0, 12L, 12L, nodeWeight)),
                        Map.entry("node-warm-2", new NodeBalanceStats("node-warm-2", warmRoleNames, 1, 0, 0.0, 12L, 12L, nodeWeight)),
                        Map.entry("node-warm-3", new NodeBalanceStats("node-warm-3", warmRoleNames, 1, 0, 0.0, 18L, 18L, nodeWeight))
                    )
                )
            )
        );
    }

    public void testStatsForNoIndicesInTier() {
        var clusterState = createClusterState(List.of(NODE1, NODE2, NODE3), List.of());
        var clusterInfo = createClusterInfo(List.of());

        var stats = ClusterBalanceStats.createFrom(clusterState, null, clusterInfo, TEST_WRITE_LOAD_FORECASTER);

        assertThat(
            stats,
            equalTo(
                new ClusterBalanceStats(
                    0,
                    0,
                    Map.of(
                        DATA_CONTENT_NODE_ROLE.roleName(),
                        new ClusterBalanceStats.TierBalanceStats(
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                            new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0)
                        )
                    ),
                    Map.ofEntries(
                        Map.entry(
                            "node-1",
                            new NodeBalanceStats("node-1", List.of(DATA_CONTENT_NODE_ROLE.roleName()), 0, 0, 0.0, 0L, 0L, null)
                        ),
                        Map.entry(
                            "node-2",
                            new NodeBalanceStats("node-2", List.of(DATA_CONTENT_NODE_ROLE.roleName()), 0, 0, 0.0, 0L, 0L, null)
                        ),
                        Map.entry(
                            "node-3",
                            new NodeBalanceStats("node-3", List.of(DATA_CONTENT_NODE_ROLE.roleName()), 0, 0, 0.0, 0L, 0L, null)
                        )
                    )
                )
            )
        );
    }

    // NodeBalanceStats specific tests //

    private static final String UNKNOWN = "UNKNOWN";
    private static final int UNDESIRED_SHARD_ALLOCATION_DEFAULT_VALUE = -1;

    public void testReadFrom() throws IOException {
        String nodeId = "node-1";
        List<String> roles = randomSubset(List.of("ingest", "data", "master", "ml"), 3);
        int shards = randomInt();
        int undesiredWriteAllocation = randomInt();
        double forecastWriteLoad = randomDouble();
        long forecastShardSize = randomLong();
        long actualShardSize = randomLong();
        Double nodeWeights = randomOptionalDouble();

        // Simulate a version before 8.8.0
        TransportVersion version = TransportVersions.V_8_7_0;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            out.writeInt(shards);
            out.writeDouble(forecastWriteLoad);
            out.writeLong(forecastShardSize);
            out.writeLong(actualShardSize);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                NodeBalanceStats nodeBalanceStats = NodeBalanceStats.readFrom(in);
                assertEquals(UNKNOWN, nodeBalanceStats.nodeId());
                assertEquals(List.of(), nodeBalanceStats.roles());
                assertEquals(shards, nodeBalanceStats.shards());
                assertEquals(UNDESIRED_SHARD_ALLOCATION_DEFAULT_VALUE, nodeBalanceStats.undesiredShardAllocations());
                assertEquals(forecastWriteLoad, nodeBalanceStats.forecastWriteLoad(), 0.0001);
                assertEquals(forecastShardSize, nodeBalanceStats.forecastShardSize());
                assertEquals(actualShardSize, nodeBalanceStats.actualShardSize());
                assertNull(nodeBalanceStats.nodeWeight());
            }
        }

        // Simulate a version >= 8.8.0 but < 8.12.0
        version = TransportVersions.V_8_8_0;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            out.writeString(nodeId);
            out.writeStringCollection(roles);
            out.writeInt(shards);
            out.writeDouble(forecastWriteLoad);
            out.writeLong(forecastShardSize);
            out.writeLong(actualShardSize);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                NodeBalanceStats nodeBalanceStats = NodeBalanceStats.readFrom(in);
                assertEquals(nodeId, nodeBalanceStats.nodeId());
                assertEquals(roles, nodeBalanceStats.roles());
                assertEquals(shards, nodeBalanceStats.shards());
                assertEquals(UNDESIRED_SHARD_ALLOCATION_DEFAULT_VALUE, nodeBalanceStats.undesiredShardAllocations());
                assertEquals(forecastWriteLoad, nodeBalanceStats.forecastWriteLoad(), 0.0001);
                assertEquals(forecastShardSize, nodeBalanceStats.forecastShardSize());
                assertEquals(actualShardSize, nodeBalanceStats.actualShardSize());
                assertNull(nodeBalanceStats.nodeWeight());
            }
        }

        // Simulate a version >= 8.12.0 but < NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS
        version = TransportVersions.V_8_12_0;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            out.writeString(nodeId);
            out.writeStringCollection(roles);
            out.writeInt(shards);
            out.writeVInt(undesiredWriteAllocation);
            out.writeDouble(forecastWriteLoad);
            out.writeLong(forecastShardSize);
            out.writeLong(actualShardSize);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                NodeBalanceStats nodeBalanceStats = NodeBalanceStats.readFrom(in);
                assertEquals(nodeId, nodeBalanceStats.nodeId());
                assertEquals(roles, nodeBalanceStats.roles());
                assertEquals(shards, nodeBalanceStats.shards());
                assertEquals(undesiredWriteAllocation, nodeBalanceStats.undesiredShardAllocations());
                assertEquals(forecastWriteLoad, nodeBalanceStats.forecastWriteLoad(), 0.0001);
                assertEquals(forecastShardSize, nodeBalanceStats.forecastShardSize());
                assertEquals(actualShardSize, nodeBalanceStats.actualShardSize());
                assertNull(nodeBalanceStats.nodeWeight());
            }
        }

        // Simulate a version >= NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS
        version = TransportVersions.NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            out.writeString(nodeId);
            out.writeStringCollection(roles);
            out.writeInt(shards);
            out.writeVInt(undesiredWriteAllocation);
            out.writeDouble(forecastWriteLoad);
            out.writeLong(forecastShardSize);
            out.writeLong(actualShardSize);
            out.writeOptionalDouble(nodeWeights);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                NodeBalanceStats nodeBalanceStats = NodeBalanceStats.readFrom(in);
                assertEquals(nodeId, nodeBalanceStats.nodeId());
                assertEquals(roles, nodeBalanceStats.roles());
                assertEquals(shards, nodeBalanceStats.shards());
                assertEquals(undesiredWriteAllocation, nodeBalanceStats.undesiredShardAllocations());
                assertEquals(forecastWriteLoad, nodeBalanceStats.forecastWriteLoad(), 0.0001);
                assertEquals(forecastShardSize, nodeBalanceStats.forecastShardSize());
                assertEquals(actualShardSize, nodeBalanceStats.actualShardSize());
                assertEquals(nodeWeights, nodeBalanceStats.nodeWeight());
            }
        }
    }

    public void testWriteTo() throws IOException {
        String nodeId = "node-1";
        List<String> roles = randomSubset(List.of("ingest", "data", "master", "ml"), 2);
        int shards = 5;
        int undesiredShardAllocations = 2;
        double forecastWriteLoad = 1.23;
        long forecastShardSize = 12345L;
        long actualShardSize = 54321L;
        Double nodeWeight = 0.99;

        NodeBalanceStats nodeBalanceStats = new NodeBalanceStats(
            nodeId,
            roles,
            shards,
            undesiredShardAllocations,
            forecastWriteLoad,
            forecastShardSize,
            actualShardSize,
            nodeWeight
        );

        // Test for TransportVersion V_8_8_0 (should write nodeId, roles, shards, forecastWriteLoad, forecastShardSize, actualShardSize)
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersions.V_8_8_0);
            nodeBalanceStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersions.V_8_8_0);
                assertEquals(nodeId, in.readString());
                assertEquals(roles, in.readStringCollectionAsList());
                assertEquals(shards, in.readInt());
                // V_8_8_0 does not write undesiredShardAllocations
                assertEquals(forecastWriteLoad, in.readDouble(), 0.0001);
                assertEquals(forecastShardSize, in.readLong());
                assertEquals(actualShardSize, in.readLong());
                // NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS not yet present
            }
        }

        // Test for TransportVersion V_8_12_0 (should also write undesiredShardAllocations)
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersions.V_8_12_0);
            nodeBalanceStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersions.V_8_12_0);
                assertEquals(nodeId, in.readString());
                assertEquals(roles, in.readStringCollectionAsList());
                assertEquals(shards, in.readInt());
                assertEquals(undesiredShardAllocations, in.readVInt());
                assertEquals(forecastWriteLoad, in.readDouble(), 0.0001);
                assertEquals(forecastShardSize, in.readLong());
                assertEquals(actualShardSize, in.readLong());
                // NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS not yet present
            }
        }

        // Test for TransportVersion.NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS (should also write nodeWeight)
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersions.NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS);
            nodeBalanceStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersions.NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS);
                assertEquals(nodeId, in.readString());
                assertEquals(roles, in.readStringCollectionAsList());
                assertEquals(shards, in.readInt());
                assertEquals(undesiredShardAllocations, in.readVInt());
                assertEquals(forecastWriteLoad, in.readDouble(), 0.0001);
                assertEquals(forecastShardSize, in.readLong());
                assertEquals(actualShardSize, in.readLong());
                assertEquals(nodeWeight, in.readOptionalDouble());
            }
        }
    }

    public void testToXContentWithoutHumanReadableNames() throws IOException {
        String nodeId = "node-1";
        List<String> roles = randomSubset(List.of("ingest", "data", "master", "ml"), 2);
        int shards = 5;
        int undesiredShardAllocations = 2;
        double forecastWriteLoad = 1.23;
        long forecastShardSize = 12345L;
        long actualShardSize = 54321L;
        Double nodeWeight = 0.99;

        NodeBalanceStats nodeBalanceStats = new NodeBalanceStats(
            nodeId,
            roles,
            shards,
            undesiredShardAllocations,
            forecastWriteLoad,
            forecastShardSize,
            actualShardSize,
            nodeWeight
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = nodeBalanceStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        // Convert to map for easy assertions
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        // Assert fields are present
        assertEquals(nodeId, map.get("node_id"));
        assertEquals(roles, map.get("roles"));
        assertEquals(shards, map.get("shard_count"));
        assertEquals(undesiredShardAllocations, map.get("undesired_shard_allocation_count"));
        assertEquals(forecastWriteLoad, (Double) map.get("forecast_write_load"), 0.0001);
        assertEquals(nodeWeight, map.get("node_weight"));

        // Check non human-readable fields are present
        assertEquals(forecastShardSize, ((Number) map.get("forecast_disk_usage_bytes")).longValue());
        assertEquals(actualShardSize, ((Number) map.get("actual_disk_usage_bytes")).longValue());

        // Check human-readable fields are not present
        assertFalse(map.containsKey("forecast_disk_usage"));
        assertFalse(map.containsKey("actual_disk_usage"));
    }

    public void testToXContentWithHumanReadableNames() throws IOException {
        String nodeId = "node-1";
        List<String> roles = randomSubset(List.of("ingest", "data", "master", "ml"), 2);
        int shards = 5;
        int undesiredShardAllocations = 2;
        double forecastWriteLoad = 1.23;
        long forecastShardSize = 12345L;
        long actualShardSize = 54321L;
        Double nodeWeight = 0.99;

        NodeBalanceStats nodeBalanceStats = new NodeBalanceStats(
            nodeId,
            roles,
            shards,
            undesiredShardAllocations,
            forecastWriteLoad,
            forecastShardSize,
            actualShardSize,
            nodeWeight
        );

        XContentBuilder builder = XContentFactory.jsonBuilder().humanReadable(true);
        builder = nodeBalanceStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        // Assert fields are present
        assertEquals(nodeId, map.get("node_id"));
        assertEquals(roles, map.get("roles"));
        assertEquals(shards, map.get("shard_count"));
        assertEquals(undesiredShardAllocations, map.get("undesired_shard_allocation_count"));
        assertEquals(forecastWriteLoad, (Double) map.get("forecast_write_load"), 0.0001);
        assertEquals(nodeWeight, map.get("node_weight"));

        // Check human-readable fields are present
        assertEquals("12kb", map.get("forecast_disk_usage"));
        assertEquals("53kb", map.get("actual_disk_usage"));

        // Check non human-readable fields are also present
        assertTrue(map.containsKey("forecast_disk_usage_bytes"));
        assertTrue(map.containsKey("actual_disk_usage_bytes"));
    }

    public void testToXContentWithUnknownNodeId() throws IOException {
        List<String> roles = randomSubset(List.of("ingest", "data", "master", "ml"), 2);
        int shards = 5;
        int undesiredShardAllocations = 2;
        double forecastWriteLoad = 1.23;
        long forecastShardSize = 12345L;
        long actualShardSize = 54321L;
        Double nodeWeight = 0.99;

        NodeBalanceStats nodeBalanceStats = new NodeBalanceStats(
            UNKNOWN,
            roles,
            shards,
            undesiredShardAllocations,
            forecastWriteLoad,
            forecastShardSize,
            actualShardSize,
            nodeWeight
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = nodeBalanceStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        // Assert node_id is not present
        assertFalse(map.containsKey("node_id"));

        // Assert the rest of the fields are present
        assertEquals(roles, map.get("roles"));
        assertEquals(shards, map.get("shard_count"));
        assertEquals(undesiredShardAllocations, map.get("undesired_shard_allocation_count"));
        assertEquals(forecastWriteLoad, (Double) map.get("forecast_write_load"), 0.0001);
        assertEquals(nodeWeight, map.get("node_weight"));

        // Check non human-readable fields are present
        assertEquals(forecastShardSize, ((Number) map.get("forecast_disk_usage_bytes")).longValue());
        assertEquals(actualShardSize, ((Number) map.get("actual_disk_usage_bytes")).longValue());
    }

    public void testToXContentWithNullNodeWeight() throws IOException {
        String nodeId = "node-id";
        List<String> roles = randomSubset(List.of("ingest", "data", "master", "ml"), 2);
        int shards = 5;
        int undesiredShardAllocations = 2;
        double forecastWriteLoad = 1.23;
        long forecastShardSize = 12345L;
        long actualShardSize = 54321L;

        NodeBalanceStats nodeBalanceStats = new NodeBalanceStats(
            nodeId,
            roles,
            shards,
            undesiredShardAllocations,
            forecastWriteLoad,
            forecastShardSize,
            actualShardSize,
            null
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = nodeBalanceStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        // Assert all other fields are present
        assertTrue(map.containsKey("node_id"));
        assertEquals(roles, map.get("roles"));
        assertEquals(shards, map.get("shard_count"));
        assertEquals(undesiredShardAllocations, map.get("undesired_shard_allocation_count"));
        assertEquals(forecastWriteLoad, (Double) map.get("forecast_write_load"), 0.0001);
        assertEquals(forecastShardSize, ((Number) map.get("forecast_disk_usage_bytes")).longValue());
        assertEquals(actualShardSize, ((Number) map.get("actual_disk_usage_bytes")).longValue());

        // Assert node weight is not present
        assertFalse(map.containsKey("node_weight"));
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
                .settings(settings(IndexVersion.current()))
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

    private static DesiredBalance createDesiredBalance(ClusterState state, Double nodeWeight) {
        var assignments = new HashMap<ShardId, ShardAssignment>();
        Map<String, Long> shardCounts = new HashMap<>();
        for (var indexRoutingTable : state.getRoutingTable()) {
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                var indexShardRoutingTable = indexRoutingTable.shard(i);
                final String nodeId = indexShardRoutingTable.primaryShard().currentNodeId();
                assignments.put(indexShardRoutingTable.shardId(), new ShardAssignment(Set.of(nodeId), 1, 0, 0));
                shardCounts.compute(nodeId, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        final var nodeWeights = state.nodes()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    node -> new DesiredBalanceMetrics.NodeWeightStats(
                        shardCounts.getOrDefault(node.getId(), 0L),
                        randomDouble(),
                        randomDouble(),
                        nodeWeight
                    )
                )
            );

        return new DesiredBalance(1, assignments, nodeWeights, DesiredBalance.ComputationFinishReason.CONVERGED);
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

    private ClusterInfo createClusterInfo(List<Tuple<String, long[]>> shardSizes) {
        return ClusterInfo.builder()
            .shardSizes(
                shardSizes.stream()
                    .flatMap(
                        entry -> IntStream.range(0, entry.v2().length)
                            .mapToObj(
                                index -> Map.entry(
                                    ClusterInfo.shardIdentifierFromRouting(new ShardId(entry.v1(), "_na_", index), true),
                                    entry.v2()[index]
                                )
                            )
                    )
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            )
            .build();
    }

    private static Tuple<String, long[]> indexSizes(String name, long... sizes) {
        return Tuple.tuple(name, sizes);
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
