/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.alias.exists;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportAliasesExistAction extends TransportMasterNodeReadAction<GetAliasesRequest, AliasesExistResponse> {

    @Inject
    public TransportAliasesExistAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            AliasesExistAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAliasesRequest::new,
            indexNameExpressionResolver,
            AliasesExistResponse::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetAliasesRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(GetAliasesRequest request, ClusterState state, ActionListener<AliasesExistResponse> listener) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        boolean result = state.metadata().hasAliases(request.aliases(), concreteIndices);
        listener.onResponse(new AliasesExistResponse(result));
    }

}
