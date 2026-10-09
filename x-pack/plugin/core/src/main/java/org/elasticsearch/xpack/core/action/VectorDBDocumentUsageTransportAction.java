/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.monitor.metrics.IndexModeStatsActionType;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.application.VectorDBDocumentFeatureSetUsage;

public class VectorDBDocumentUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final Client client;

    @Inject
    public VectorDBDocumentUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client
    ) {
        super(XPackUsageFeatureAction.VECTORDB_DOCUMENT.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = client;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        final DiscoveryNode[] nodes = state.nodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        final var statsRequest = new IndexModeStatsActionType.StatsRequest(nodes);
        client.execute(IndexModeStatsActionType.TYPE, statsRequest, listener.map(statsResponse -> {
            final var indexStats = statsResponse.stats().get(IndexMode.VECTORDB_DOCUMENT);
            return new XPackUsageFeatureResponse(
                new VectorDBDocumentFeatureSetUsage(true, true, indexStats.numIndices(), indexStats.numDocs())
            );
        }));
    }
}
