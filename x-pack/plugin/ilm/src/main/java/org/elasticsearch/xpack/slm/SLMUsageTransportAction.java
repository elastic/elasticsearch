/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
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
    private final XPackLicenseState licenseState;

    @Inject
    public SLMUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                   XPackLicenseState licenseState) {
        super(XPackUsageFeatureAction.SNAPSHOT_LIFECYCLE.name(), transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver);
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        boolean available = licenseState.isAllowed(XPackLicenseState.Feature.ILM);
        final SnapshotLifecycleMetadata slmMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        final SLMFeatureSetUsage usage = new SLMFeatureSetUsage(available, slmMeta == null ? null : slmMeta.getStats());
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
