/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.FrozenEngine;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.frozen.FrozenIndicesFeatureSetUsage;

public class FrozenIndicesUsageTransportAction extends XPackUsageFeatureTransportAction {

    @Inject
    public FrozenIndicesUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                             ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(XPackUsageFeatureAction.FROZEN_INDICES.name(), transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver);
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        int numFrozenIndices = 0;
        for (IndexMetaData indexMetaData : state.metaData()) {
            if (FrozenEngine.INDEX_FROZEN.get(indexMetaData.getSettings())) {
                numFrozenIndices++;
            }
        }
        listener.onResponse(new XPackUsageFeatureResponse(new FrozenIndicesFeatureSetUsage(true, true, numFrozenIndices)));
    }
}
