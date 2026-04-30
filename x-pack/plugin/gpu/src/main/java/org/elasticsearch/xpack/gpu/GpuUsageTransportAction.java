/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.gpu.GpuNodeStats;
import org.elasticsearch.xpack.core.gpu.GpuVectorIndexingFeatureSetUsage;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles the {@code _xpack/usage} request for the GPU vector indexing feature.
 * Dispatches a {@link GpuStatsAction} to collect per-node GPU stats via
 * {@link TransportGpuStatsAction}, then aggregates the results into a
 * cluster-level {@link GpuVectorIndexingFeatureSetUsage}.
 */
public class GpuUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;
    private final XPackLicenseState licenseState;

    @Inject
    public GpuUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        XPackLicenseState licenseState
    ) {
        super(XPackUsageFeatureAction.GPU_VECTOR_INDEXING.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = client;
        this.licenseState = licenseState;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        new ParentTaskAssigningClient(client, clusterService.localNode(), task).execute(
            GpuStatsAction.INSTANCE,
            new GpuStatsRequest(),
            listener.delegateFailureAndWrap((delegate, response) -> {
                boolean available = GPUPlugin.GPU_INDEXING_FEATURE.checkWithoutTracking(licenseState);
                delegate.onResponse(new XPackUsageFeatureResponse(buildUsage(response, available)));
            })
        );
    }

    // package-private for testing
    static GpuVectorIndexingFeatureSetUsage buildUsage(GpuStatsResponse response, boolean available) {
        int nodesWithGpu = 0;
        long indexBuildCount = 0;
        boolean enabled = false;
        List<GpuNodeStats> nodes = new ArrayList<>();

        for (NodeGpuStatsResponse nodeResponse : response.getNodes()) {
            if (nodeResponse.isGpuSupported()) {
                nodesWithGpu++;
                nodes.add(
                    new GpuNodeStats(
                        nodeResponse.getGpuName(),
                        nodeResponse.getTotalGpuMemoryInBytes(),
                        nodeResponse.isGpuSettingEnabled(),
                        nodeResponse.getGpuUsageCount()
                    )
                );
            }
            if (nodeResponse.isGpuSettingEnabled()) {
                enabled = true;
            }
            indexBuildCount += nodeResponse.getGpuUsageCount();
        }

        return new GpuVectorIndexingFeatureSetUsage(available, enabled, indexBuildCount, nodesWithGpu, nodes);
    }
}
