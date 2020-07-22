/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.graph;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.graph.GraphFeatureSetUsage;

public class GraphUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Settings settings;
    private final XPackLicenseState licenseState;

    @Inject
    public GraphUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                     Settings settings, XPackLicenseState licenseState) {
        super(XPackUsageFeatureAction.GRAPH.name(), transportService, clusterService,
              threadPool, actionFilters, indexNameExpressionResolver);
        this.settings = settings;
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        GraphFeatureSetUsage usage =
            new GraphFeatureSetUsage(licenseState.isAllowed(Feature.GRAPH), XPackSettings.GRAPH_ENABLED.get(settings));
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
