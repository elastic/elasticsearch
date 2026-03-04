/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Fans out to all data nodes to collect local GPU information (availability,
 * memory, device name, usage count) from {@link GPUSupport}. Called internally
 * by {@link GpuUsageTransportAction} to gather per-node stats for {@code _xpack/usage}.
 */
public class TransportGpuStatsAction extends TransportNodesAction<
    GpuStatsRequest,
    GpuStatsResponse,
    TransportGpuStatsAction.NodeRequest,
    NodeGpuStatsResponse,
    Void> {

    private final boolean gpuSettingEnabled;

    @Inject
    public TransportGpuStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings
    ) {
        super(
            GpuStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        GPUPlugin.GpuMode gpuMode = GPUPlugin.VECTORS_INDEXING_USE_GPU_NODE_SETTING.get(settings);
        this.gpuSettingEnabled = gpuMode != GPUPlugin.GpuMode.FALSE && GPUSupport.isSupported();
    }

    @Override
    protected GpuStatsResponse newResponse(
        GpuStatsRequest request,
        List<NodeGpuStatsResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new GpuStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(GpuStatsRequest request) {
        return new NodeRequest();
    }

    @Override
    protected NodeGpuStatsResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeGpuStatsResponse(in);
    }

    @Override
    protected NodeGpuStatsResponse nodeOperation(NodeRequest nodeRequest, Task task) {
        return new NodeGpuStatsResponse(
            clusterService.localNode(),
            GPUSupport.isSupported(),
            gpuSettingEnabled,
            GPUSupport.getUsageCount(),
            GPUSupport.getTotalGpuMemory(),
            GPUSupport.getGpuName()
        );
    }

    static class NodeRequest extends AbstractTransportRequest {

        NodeRequest() {}

        NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
