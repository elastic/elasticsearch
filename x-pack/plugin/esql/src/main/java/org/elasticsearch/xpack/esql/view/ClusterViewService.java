/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of {@link ViewService} that keeps the views in the cluster state.
 */
public class ClusterViewService extends ViewService {
    private final ClusterService clusterService;

    public ClusterViewService(EsqlFunctionRegistry functionRegistry, ClusterService clusterService) {
        super(functionRegistry);
        this.clusterService = clusterService;
    }

    @Override
    protected ViewMetadata getMetadata() {
        return clusterService.state().metadata().custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
    }

    @Override
    protected void updateViewMetadata(ActionListener<Void> callback, Function<ViewMetadata, Map<String, View>> function) {
        submitUnbatchedTask("update-esql-view-metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                var views = currentState.metadata().custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
                Map<String, View> policies = function.apply(views);
                Metadata metadata = Metadata.builder(currentState.metadata())
                    .putCustom(ViewMetadata.TYPE, new ViewMetadata(policies))
                    .build();
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                callback.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                callback.onFailure(e);
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected void assertMasterNode() {
        assert clusterService.localNode().isMasterNode();
    }
}
