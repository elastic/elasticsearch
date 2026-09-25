/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.monitor.metrics.IndexModeStatsActionType;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.application.ColumnarFeatureSetUsage;

/**
 * Collects usage statistics for the {@code columnar} index mode ({@code index.mode: columnar}).
 */
public class ColumnarUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final ClusterService clusterService;
    private final Client client;
    private final ProjectResolver projectResolver;

    @Inject
    public ColumnarUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        ProjectResolver projectResolver
    ) {
        super(XPackUsageFeatureAction.COLUMNAR.name(), transportService, clusterService, threadPool, actionFilters);
        this.clusterService = clusterService;
        this.client = client;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        final ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(state);
        final var counts = LogsDBColumnarUsageTransportAction.computeIndexModeStats(
            projectMetadata,
            clusterService.getClusterSettings(),
            IndexMode.COLUMNAR
        );

        final DiscoveryNode[] nodes = state.nodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        final var statsRequest = new IndexModeStatsActionType.StatsRequest(nodes);
        client.execute(IndexModeStatsActionType.TYPE, statsRequest, listener.map(statsResponse -> {
            final var indexStats = statsResponse.stats().get(counts.indexMode());
            return new XPackUsageFeatureResponse(
                new ColumnarFeatureSetUsage(
                    true,
                    counts.enabled(),
                    counts.numIndices(),
                    counts.numIndicesWithSyntheticSources(),
                    indexStats.numDocs(),
                    indexStats.numBytes(),
                    counts.dataStreamsCount(),
                    counts.dataStreamsManagedByIlm(),
                    counts.dataStreamsManagedByDlm()
                )
            );
        }));
    }
}
