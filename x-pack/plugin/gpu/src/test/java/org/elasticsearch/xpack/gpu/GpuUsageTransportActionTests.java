/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.gpu.GpuVectorIndexingFeatureSetUsage;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/** Unit test for how the action aggregates GPU stats responses from multiple nodes into a single cluster-wide usage report. */
public class GpuUsageTransportActionTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");

    public void testAggregation_mixedNodes() throws Exception {
        var nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), true, true, 5, 24_000_000_000L, "NVIDIA L4"),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node2"), true, false, 0, 24_000_000_000L, "NVIDIA L4"),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node3"), false, false, 0, -1L, null),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node4"), true, true, 3, 80_000_000_000L, "NVIDIA A100")
        );

        var usage = GpuUsageTransportAction.buildUsage(new GpuStatsResponse(CLUSTER_NAME, nodeResponses, Collections.emptyList()), true);

        assertThat(usage.available(), equalTo(true));
        assertThat(usage.enabled(), equalTo(true));
        Map<String, Object> xContent = toMap(usage);
        assertThat(((Number) xContent.get("index_build_count")).longValue(), equalTo(8L));
        assertThat(xContent.get("nodes_with_gpu"), equalTo(3));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) xContent.get("nodes");
        assertThat(nodes, hasSize(3));

        // Verify per-node details (node3 excluded - no GPU)
        assertThat(nodes.get(0).get("type"), equalTo("NVIDIA L4"));
        assertThat(((Number) nodes.get(0).get("memory_in_bytes")).longValue(), equalTo(24_000_000_000L));
        assertThat(nodes.get(0).get("enabled"), equalTo(true));
        assertThat(((Number) nodes.get(0).get("index_build_count")).longValue(), equalTo(5L));

        assertThat(nodes.get(1).get("type"), equalTo("NVIDIA L4"));
        assertThat(nodes.get(1).get("enabled"), equalTo(false));

        assertThat(nodes.get(2).get("type"), equalTo("NVIDIA A100"));
        assertThat(((Number) nodes.get(2).get("memory_in_bytes")).longValue(), equalTo(80_000_000_000L));
        assertThat(((Number) nodes.get(2).get("index_build_count")).longValue(), equalTo(3L));
    }

    public void testAggregation_noGpuNodes() throws Exception {
        var nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), false, false, 0, -1L, null),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node2"), false, false, 0, -1L, null)
        );

        var usage = GpuUsageTransportAction.buildUsage(new GpuStatsResponse(CLUSTER_NAME, nodeResponses, Collections.emptyList()), false);

        assertThat(usage.available(), equalTo(false));
        assertThat(usage.enabled(), equalTo(false));
        Map<String, Object> xContent = toMap(usage);
        assertThat(((Number) xContent.get("index_build_count")).longValue(), equalTo(0L));
        assertThat(xContent.get("nodes_with_gpu"), equalTo(0));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) xContent.get("nodes");
        assertThat(nodes, hasSize(0));
    }

    public void testAggregation_emptyClusterResponse() throws Exception {
        var usage = GpuUsageTransportAction.buildUsage(new GpuStatsResponse(CLUSTER_NAME, List.of(), Collections.emptyList()), true);

        assertThat(usage.available(), equalTo(true));
        assertThat(usage.enabled(), equalTo(false));
        Map<String, Object> xContent = toMap(usage);
        assertThat(((Number) xContent.get("index_build_count")).longValue(), equalTo(0L));
        assertThat(xContent.get("nodes_with_gpu"), equalTo(0));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) xContent.get("nodes");
        assertThat(nodes, hasSize(0));
    }

    public void testAggregation_licenseNotAvailable() throws Exception {
        // When license is not available, GPU format is not used, so index_build_count must be 0.
        // However, GPU hardware can still be detected (enabled=true, name/memory reported).
        var nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), true, true, 0, 24_000_000_000L, "NVIDIA L4")
        );

        var usage = GpuUsageTransportAction.buildUsage(new GpuStatsResponse(CLUSTER_NAME, nodeResponses, Collections.emptyList()), false);

        assertThat(usage.available(), equalTo(false));
        assertThat(usage.enabled(), equalTo(true));
        Map<String, Object> xContent = toMap(usage);
        assertThat(((Number) xContent.get("index_build_count")).longValue(), equalTo(0L));
        assertThat(xContent.get("nodes_with_gpu"), equalTo(1));
    }

    public void testAggregation_enabledBasedOnSetting() throws Exception {
        // All nodes have GPU but all disabled via setting -> enabled=false
        var nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), true, false, 0, 24_000_000_000L, "NVIDIA L4"),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node2"), true, false, 0, 24_000_000_000L, "NVIDIA L4")
        );

        var usage = GpuUsageTransportAction.buildUsage(new GpuStatsResponse(CLUSTER_NAME, nodeResponses, Collections.emptyList()), true);
        assertThat(usage.enabled(), equalTo(false));

        // GPU nodes are still reported even when setting is disabled
        Map<String, Object> xContent = toMap(usage);
        assertThat(xContent.get("nodes_with_gpu"), equalTo(2));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) xContent.get("nodes");
        assertThat(nodes.get(0).get("type"), equalTo("NVIDIA L4"));
        assertThat(nodes.get(1).get("type"), equalTo("NVIDIA L4"));

        // One node has GPU enabled via setting -> enabled=true
        nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), true, true, 0, 24_000_000_000L, "NVIDIA L4"),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node2"), true, false, 0, 24_000_000_000L, "NVIDIA L4")
        );

        usage = GpuUsageTransportAction.buildUsage(new GpuStatsResponse(CLUSTER_NAME, nodeResponses, Collections.emptyList()), true);
        assertThat(usage.enabled(), equalTo(true));
    }

    private static Map<String, Object> toMap(GpuVectorIndexingFeatureSetUsage usage) throws Exception {
        try (var builder = XContentFactory.jsonBuilder()) {
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(builder), false);
        }
    }
}
