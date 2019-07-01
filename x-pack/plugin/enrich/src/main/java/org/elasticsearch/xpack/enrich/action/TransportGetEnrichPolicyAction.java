/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichStore;

import java.io.IOException;

public class TransportGetEnrichPolicyAction extends TransportMasterNodeReadAction<GetEnrichPolicyAction.Request,
    GetEnrichPolicyAction.Response> {

    @Inject
    public TransportGetEnrichPolicyAction(TransportService transportService,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetEnrichPolicyAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetEnrichPolicyAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetEnrichPolicyAction.Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected GetEnrichPolicyAction.Response read(StreamInput in) throws IOException {
        return new GetEnrichPolicyAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, GetEnrichPolicyAction.Request request,
                                   ClusterState state,
                                   ActionListener<GetEnrichPolicyAction.Response> listener) throws Exception {
        final EnrichPolicy policy = EnrichStore.getPolicy(request.getName(), state);
        if (policy == null) {
            throw new ResourceNotFoundException("Policy [{}] was not found", request.getName());
        }
        listener.onResponse(new GetEnrichPolicyAction.Response(policy));
    }

    @Override
    protected ClusterBlockException checkBlock(GetEnrichPolicyAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

}
