/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.stream.Collectors;

public class TransportSnapshottableFeaturesAction extends TransportMasterNodeAction<
    GetSnapshottableFeaturesRequest,
    GetSnapshottableFeaturesResponse> {

    private final SystemIndices systemIndices;

    @Inject
    public TransportSnapshottableFeaturesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            SnapshottableFeaturesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSnapshottableFeaturesRequest::new,
            indexNameExpressionResolver,
            GetSnapshottableFeaturesResponse::new,
            ThreadPool.Names.SAME
        );
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetSnapshottableFeaturesRequest request,
        ClusterState state,
        ActionListener<GetSnapshottableFeaturesResponse> listener
    ) throws Exception {
        listener.onResponse(
            new GetSnapshottableFeaturesResponse(
                systemIndices.getFeatures()
                    .entrySet()
                    .stream()
                    .map(
                        featureEntry -> new GetSnapshottableFeaturesResponse.SnapshottableFeature(
                            featureEntry.getKey(),
                            featureEntry.getValue().getDescription()
                        )
                    )
                    .collect(Collectors.toList())
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshottableFeaturesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
