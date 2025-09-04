/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.frozen.FrozenIndicesFeatureSetUsage;

@UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT) // Remove this: it is unused in v9 but needed for mixed v8/v9 clusters
public class FrozenIndicesUsageTransportAction extends XPackUsageFeatureTransportAction {

    @Inject
    public FrozenIndicesUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(XPackUsageFeatureAction.FROZEN_INDICES.name(), transportService, clusterService, threadPool, actionFilters);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        // v9 does not recognize frozen indices, so report a count of 0 while this action exists to support mixed v8/v9 clusters
        int numFrozenIndices = 0;
        listener.onResponse(new XPackUsageFeatureResponse(new FrozenIndicesFeatureSetUsage(true, true, numFrozenIndices)));
    }
}
