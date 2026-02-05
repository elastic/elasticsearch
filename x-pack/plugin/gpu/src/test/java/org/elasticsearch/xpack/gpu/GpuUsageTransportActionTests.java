/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.gpu.GpuVectorIndexingFeatureSetUsage;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GpuUsageTransportActionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testAggregation_mixedNodes() throws Exception {
        List<NodeGpuStatsResponse> nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), true, true, 5, 24_000_000_000L, "NVIDIA L4"),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node2"), true, true, 0, 24_000_000_000L, "NVIDIA L4"),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node3"), false, false, 0, -1L, null),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node4"), true, true, 3, 80_000_000_000L, "NVIDIA A100")
        );

        GpuVectorIndexingFeatureSetUsage usage = executeUsageAction(nodeResponses, true);

        assertThat(usage.available(), equalTo(true));
        assertThat(usage.enabled(), equalTo(true));
        Map<String, Object> xContent = toMap(usage);
        assertThat(((Number) xContent.get("index_build_count")).longValue(), equalTo(8L));
        assertThat(xContent.get("nodes_with_gpu"), equalTo(3));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) xContent.get("nodes");
        assertThat(nodes, hasSize(3));
    }

    @SuppressWarnings("unchecked")
    public void testAggregation_noGpuNodes() throws Exception {
        List<NodeGpuStatsResponse> nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), false, false, 0, -1L, null),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node2"), false, false, 0, -1L, null)
        );

        GpuVectorIndexingFeatureSetUsage usage = executeUsageAction(nodeResponses, false);

        assertThat(usage.available(), equalTo(false));
        assertThat(usage.enabled(), equalTo(false));
        Map<String, Object> xContent = toMap(usage);
        assertThat(((Number) xContent.get("index_build_count")).longValue(), equalTo(0L));
        assertThat(xContent.get("nodes_with_gpu"), equalTo(0));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) xContent.get("nodes");
        assertThat(nodes, hasSize(0));
    }

    @SuppressWarnings("unchecked")
    public void testAggregation_nodeDetails() throws Exception {
        List<NodeGpuStatsResponse> nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), true, true, 10, 24_000_000_000L, "NVIDIA L4"),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node2"), true, false, 5, 80_000_000_000L, "NVIDIA A100")
        );

        GpuVectorIndexingFeatureSetUsage usage = executeUsageAction(nodeResponses, true);
        Map<String, Object> xContent = toMap(usage);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) xContent.get("nodes");
        assertThat(nodes, hasSize(2));

        Map<String, Object> node1 = nodes.get(0);
        assertThat(node1.get("type"), equalTo("NVIDIA L4"));
        assertThat(((Number) node1.get("memory_in_bytes")).longValue(), equalTo(24_000_000_000L));
        assertThat(node1.get("enabled"), equalTo(true));
        assertThat(((Number) node1.get("index_build_count")).longValue(), equalTo(10L));

        Map<String, Object> node2 = nodes.get(1);
        assertThat(node2.get("type"), equalTo("NVIDIA A100"));
        assertThat(((Number) node2.get("memory_in_bytes")).longValue(), equalTo(80_000_000_000L));
        assertThat(node2.get("enabled"), equalTo(false));
        assertThat(((Number) node2.get("index_build_count")).longValue(), equalTo(5L));
    }

    @SuppressWarnings("unchecked")
    public void testAggregation_enabledBasedOnSetting() throws Exception {
        // All nodes have GPU but all disabled via setting -> enabled=false
        List<NodeGpuStatsResponse> nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), true, false, 0, 24_000_000_000L, "NVIDIA L4"),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node2"), true, false, 0, 24_000_000_000L, "NVIDIA L4")
        );

        GpuVectorIndexingFeatureSetUsage usage = executeUsageAction(nodeResponses, true);
        assertThat(usage.enabled(), equalTo(false));

        // One node has GPU enabled via setting -> enabled=true
        nodeResponses = List.of(
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node1"), true, true, 0, 24_000_000_000L, "NVIDIA L4"),
            new NodeGpuStatsResponse(DiscoveryNodeUtils.create("node2"), true, false, 0, 24_000_000_000L, "NVIDIA L4")
        );

        usage = executeUsageAction(nodeResponses, true);
        assertThat(usage.enabled(), equalTo(true));
    }

    @SuppressWarnings("unchecked")
    private GpuVectorIndexingFeatureSetUsage executeUsageAction(List<NodeGpuStatsResponse> nodeResponses, boolean licenseAvailable)
        throws Exception {
        GpuStatsResponse statsResponse = new GpuStatsResponse(new ClusterName("test"), nodeResponses, Collections.emptyList());

        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(GPUPlugin.GPU_INDEXING_FEATURE)).thenReturn(licenseAvailable);

        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode mockNode = DiscoveryNodeUtils.create("local");
        when(clusterService.localNode()).thenReturn(mockNode);

        var client = mock(org.elasticsearch.client.internal.Client.class);
        doAnswer(invocation -> {
            ActionListener<GpuStatsResponse> listener = (ActionListener<GpuStatsResponse>) invocation.getArguments()[2];
            listener.onResponse(statsResponse);
            return null;
        }).when(client).execute(eq(GpuStatsAction.INSTANCE), any(), any());

        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);

        GpuUsageTransportAction action = new GpuUsageTransportAction(
            transportService,
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            client,
            licenseState
        );

        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        action.localClusterStateOperation(mock(Task.class), null, null, future);
        return (GpuVectorIndexingFeatureSetUsage) future.get().getUsage();
    }

    private Map<String, Object> toMap(GpuVectorIndexingFeatureSetUsage usage) throws Exception {
        try (var builder = XContentFactory.jsonBuilder()) {
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(builder), false);
        }
    }
}
