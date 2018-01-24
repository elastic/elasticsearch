/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction.Request;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction.Response;

public class TransportGetLifecycleAction extends TransportMasterNodeAction<Request, Response> {

    @Inject
    public TransportGetLifecycleAction(Settings settings, TransportService transportService, ClusterService clusterService,
            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, GetLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                Request::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response newResponse() {
        return new Response();
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        IndexLifecycleMetadata metadata = clusterService.state().metaData().custom(IndexLifecycleMetadata.TYPE);
        if (metadata == null) {
            listener.onFailure(new ResourceNotFoundException("Lifecycle policy not found: {}", request.getPolicyName()));
        } else {
            LifecyclePolicy policy = metadata.getPolicies().get(request.getPolicyName());
            if (policy == null) {
                listener.onFailure(new ResourceNotFoundException("Lifecycle policy not found: {}", request.getPolicyName()));
            } else {
                listener.onResponse(new Response(policy));
            }
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}