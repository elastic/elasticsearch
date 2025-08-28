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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class NodeBalanceStatsTests extends AbstractWireSerializingTestCase<ClusterBalanceStats.NodeBalanceStats> {
    private static final String UNKNOWN = "UNKNOWN";
    private static final int UNDESIRED_SHARD_ALLOCATION_DEFAULT_VALUE = -1;
    private static final TransportVersion NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS = TransportVersion.fromName(
        "node_weights_added_to_node_balance_stats"
    );

    @Override
    protected Writeable.Reader<ClusterBalanceStats.NodeBalanceStats> instanceReader() {
        return ClusterBalanceStats.NodeBalanceStats::readFrom;
    }

    @Override
    protected ClusterBalanceStats.NodeBalanceStats createTestInstance() {
        return createRandomNodeBalanceStats();
    }

    private ClusterBalanceStats.NodeBalanceStats createRandomNodeBalanceStats() {
        return new ClusterBalanceStats.NodeBalanceStats(
            randomIdentifier(),
            randomNonEmptySubsetOf(List.of("ingest", "data", "master", "ml")),
            randomInt(),
            randomInt(),
            randomDouble(),
            randomLong(),
            randomLong(),
            randomOptionalDouble()
        );
    }

    @Override
    protected ClusterBalanceStats.NodeBalanceStats mutateInstance(ClusterBalanceStats.NodeBalanceStats instance) throws IOException {
        return createTestInstance();
    }

    public void testSerializationWithTransportVersionV_8_12_0() throws IOException {
        ClusterBalanceStats.NodeBalanceStats instance = createTestInstance();
        // Serialization changes based on this version
        final var oldVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_8_12_0,
            TransportVersionUtils.getPreviousVersion(NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS)
        );
        ClusterBalanceStats.NodeBalanceStats deserialized = copyInstance(instance, oldVersion);

        // Assert the values are as expected
        assertEquals(instance.nodeId(), deserialized.nodeId());
        assertEquals(instance.roles(), deserialized.roles());
        assertEquals(instance.undesiredShardAllocations(), deserialized.undesiredShardAllocations());

        // Assert the default values are as expected
        assertNull(deserialized.nodeWeight());
    }

    public void testSerializationWithTransportVersionNodeWeightsAddedToNodeBalanceStats() throws IOException {
        ClusterBalanceStats.NodeBalanceStats instance = createTestInstance();
        // Serialization changes based on this version
        final var oldVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            NODE_WEIGHTS_ADDED_TO_NODE_BALANCE_STATS,
            TransportVersion.current()
        );
        ClusterBalanceStats.NodeBalanceStats deserialized = copyInstance(instance, oldVersion);

        // Assert the values are as expected
        assertEquals(instance.nodeId(), deserialized.nodeId());
        assertEquals(instance.roles(), deserialized.roles());
        assertEquals(instance.undesiredShardAllocations(), deserialized.undesiredShardAllocations());
        assertEquals(instance.nodeWeight(), deserialized.nodeWeight());
    }

    public void testToXContentWithoutHumanReadableNames() throws IOException {
        String nodeId = "node-1";
        List<String> roles = randomNonEmptySubsetOf(List.of("ingest", "data", "master", "ml"));
        int shards = 5;
        int undesiredShardAllocations = 2;
        double forecastWriteLoad = 1.23;
        long forecastShardSize = 12345L;
        long actualShardSize = 54321L;
        Double nodeWeight = 0.99;

        ClusterBalanceStats.NodeBalanceStats nodeBalanceStats = new ClusterBalanceStats.NodeBalanceStats(
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
        List<String> roles = randomNonEmptySubsetOf(List.of("ingest", "data", "master", "ml"));
        int shards = 5;
        int undesiredShardAllocations = 2;
        double forecastWriteLoad = 1.23;
        long forecastShardSize = 12345L;
        long actualShardSize = 54321L;
        Double nodeWeight = 0.99;

        ClusterBalanceStats.NodeBalanceStats nodeBalanceStats = new ClusterBalanceStats.NodeBalanceStats(
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
        List<String> roles = randomNonEmptySubsetOf(List.of("ingest", "data", "master", "ml"));
        int shards = 5;
        int undesiredShardAllocations = 2;
        double forecastWriteLoad = 1.23;
        long forecastShardSize = 12345L;
        long actualShardSize = 54321L;
        Double nodeWeight = 0.99;

        ClusterBalanceStats.NodeBalanceStats nodeBalanceStats = new ClusterBalanceStats.NodeBalanceStats(
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
        List<String> roles = randomNonEmptySubsetOf(List.of("ingest", "data", "master", "ml"));
        int shards = 5;
        int undesiredShardAllocations = 2;
        double forecastWriteLoad = 1.23;
        long forecastShardSize = 12345L;
        long actualShardSize = 54321L;

        ClusterBalanceStats.NodeBalanceStats nodeBalanceStats = new ClusterBalanceStats.NodeBalanceStats(
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
}
