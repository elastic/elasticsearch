/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.exists.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Indices exists action.
 */
public class TransportIndicesExistsAction extends TransportMasterNodeReadAction<IndicesExistsRequest, IndicesExistsResponse> {

    @Inject
    public TransportIndicesExistsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            IndicesExistsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            IndicesExistsRequest::new,
            indexNameExpressionResolver,
            IndicesExistsResponse::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesExistsRequest request, ClusterState state) {
        // make sure through indices options that the concrete indices call never throws IndexMissingException
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
            true,
            true,
            request.indicesOptions().expandWildcardsOpen(),
            request.indicesOptions().expandWildcardsClosed()
        );
        return state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request.indices())
            );
    }

    @Override
    protected void masterOperation(
        final IndicesExistsRequest request,
        final ClusterState state,
        final ActionListener<IndicesExistsResponse> listener
    ) {
        boolean exists;
        try {
            // Similar as the previous behaviour, but now also aliases and wildcards are supported.
            indexNameExpressionResolver.concreteIndexNames(state, request);
            exists = true;
        } catch (IndexNotFoundException e) {
            exists = false;
        }
        listener.onResponse(new IndicesExistsResponse(exists));
    }
}
