/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.AbstractTransportSetResetModeAction;
import org.elasticsearch.xpack.core.action.SetResetModeActionRequest;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.SetResetModeAction;


public class TransportSetTransformResetModeAction extends AbstractTransportSetResetModeAction {

    @Inject
    public TransportSetTransformResetModeAction(TransportService transportService, ThreadPool threadPool, ClusterService clusterService,
                                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(SetResetModeAction.NAME, transportService, threadPool, clusterService, actionFilters, indexNameExpressionResolver);
    }

    @Override
    protected boolean isResetMode(ClusterState clusterState) {
        return TransformMetadata.getTransformMetadata(clusterState).isResetMode();
    }

    @Override
    protected String featureName() {
        return "transform";
    }

    @Override
    protected ClusterState setState(ClusterState oldState, SetResetModeActionRequest request) {
        ClusterState.Builder newState = ClusterState.builder(oldState);
        if (request.shouldDeleteMetadata()) {
            assert request.isEnabled() == false; // SetResetModeActionRequest should have enforced this
            newState.metadata(Metadata.builder(oldState.getMetadata())
                .removeCustom(TransformMetadata.TYPE)
                .build());
        } else {
            TransformMetadata.Builder builder = TransformMetadata.Builder
                .from(oldState.metadata().custom(TransformMetadata.TYPE))
                .isResetMode(request.isEnabled());
            newState.metadata(Metadata.builder(oldState.getMetadata())
                .putCustom(TransformMetadata.TYPE, builder.build())
                .build());
        }
        return newState.build();
    }
}
