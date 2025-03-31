/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.slm.SLMFeatureSetUsage;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

public class SLMUsageTransportAction extends XPackUsageFeatureTransportAction {

    @Inject
    public SLMUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(XPackUsageFeatureAction.SNAPSHOT_LIFECYCLE.name(), transportService, clusterService, threadPool, actionFilters);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        final SnapshotLifecycleMetadata slmMeta = state.metadata().getProject().custom(SnapshotLifecycleMetadata.TYPE);
        final SLMFeatureSetUsage usage = new SLMFeatureSetUsage(slmMeta == null ? null : slmMeta.getStats());
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
