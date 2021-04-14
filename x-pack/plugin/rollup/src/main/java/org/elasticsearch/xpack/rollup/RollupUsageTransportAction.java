/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;

public class RollupUsageTransportAction extends XPackUsageFeatureTransportAction {

    @Inject
    public RollupUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                      ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(XPackUsageFeatureAction.ROLLUP.name(), transportService, clusterService,
            threadPool, actionFilters, indexNameExpressionResolver);
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        // TODO expose the currently running rollup tasks on this node?  Unclear the best way to do that
        RollupFeatureSetUsage usage = new RollupFeatureSetUsage();
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
