/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.exists.types;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Types exists transport action.
 */
public class TransportTypesExistsAction extends TransportMasterNodeReadAction<TypesExistsRequest, TypesExistsResponse> {

    @Inject
    public TransportTypesExistsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TypesExistsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            TypesExistsRequest::new,
            indexNameExpressionResolver,
            TypesExistsResponse::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected ClusterBlockException checkBlock(TypesExistsRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(
        final TypesExistsRequest request,
        final ClusterState state,
        final ActionListener<TypesExistsResponse> listener
    ) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request.indicesOptions(), request.indices());
        if (concreteIndices.length == 0) {
            listener.onResponse(new TypesExistsResponse(false));
            return;
        }

        for (String concreteIndex : concreteIndices) {
            if (state.metadata().hasIndexAbstraction(concreteIndex) == false) {
                listener.onResponse(new TypesExistsResponse(false));
                return;
            }

            MappingMetadata mapping = state.metadata().getIndices().get(concreteIndex).mapping();
            if (mapping == null) {
                listener.onResponse(new TypesExistsResponse(false));
                return;
            }

            for (String type : request.types()) {
                if (mapping.type().equals(type) == false) {
                    listener.onResponse(new TypesExistsResponse(false));
                    return;
                }
            }
        }

        listener.onResponse(new TypesExistsResponse(true));
    }
}
