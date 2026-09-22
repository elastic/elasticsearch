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
import org.elasticsearch.xpack.core.application.VectorDBColumnarFeatureSetUsage;

public class VectorDBColumnarUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final Client client;

    @Inject
    public VectorDBColumnarUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client
    ) {
        super(XPackUsageFeatureAction.VECTORDB_COLUMNAR.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = client;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        if (IndexMode.VECTORDB_COLUMNAR_FEATURE_FLAG.isEnabled() == false) {
            // Unusable in this build: IndexMode#availableModes omits the mode, so StatsResponse#stats holds no entry for it
            // and the fan-out below would have nothing to read. Report the section anyway - disabled, but available, since
            // no license restricts the mode - so the shape of the usage response does not depend on the build.
            listener.onResponse(new XPackUsageFeatureResponse(new VectorDBColumnarFeatureSetUsage(true, false, 0, 0)));
            return;
        }
        final DiscoveryNode[] nodes = state.nodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        final var statsRequest = new IndexModeStatsActionType.StatsRequest(nodes);
        client.execute(IndexModeStatsActionType.TYPE, statsRequest, listener.map(statsResponse -> {
            final var indexStats = statsResponse.stats().get(IndexMode.VECTORDB_COLUMNAR);
            return new XPackUsageFeatureResponse(
                new VectorDBColumnarFeatureSetUsage(true, true, indexStats.numIndices(), indexStats.numDocs())
            );
        }));
    }
}
