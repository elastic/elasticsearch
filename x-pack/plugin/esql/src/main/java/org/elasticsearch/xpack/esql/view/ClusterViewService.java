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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;

import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of {@link ViewService} that keeps the views in the cluster state.
 */
public class ClusterViewService extends ViewService {
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final ProjectResolver projectResolver;

    public ClusterViewService(
        EsqlFunctionRegistry functionRegistry,
        ClusterService clusterService,
        FeatureService featureService,
        ProjectResolver projectResolver,
        ViewServiceConfig config
    ) {
        super(functionRegistry, config);
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.projectResolver = projectResolver;
    }

    public ProjectId getProjectId() {
        return projectResolver.getProjectId();
    }

    @Override
    protected ViewMetadata getMetadata() {
        return getMetadata(getProjectId());
    }

    @Override
    protected ViewMetadata getMetadata(ProjectId projectId) {
        return getMetadata(clusterService.state().metadata().getProject(projectId));
    }

    protected ViewMetadata getMetadata(ProjectMetadata projectMetadata) {
        return projectMetadata.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
    }

    protected ProjectMetadata getProjectMetadata(ProjectId projectId) {
        return clusterService.state().metadata().getProject(projectId);
    }

    @Override
    protected void updateViewMetadata(
        ProjectId projectId,
        ActionListener<Void> callback,
        Function<ViewMetadata, Map<String, View>> function
    ) {
        submitUnbatchedTask("update-esql-view-metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                var project = getProjectMetadata(projectId);
                var views = project.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
                Map<String, View> policies = function.apply(views);
                var metadata = ProjectMetadata.builder(project).putCustom(ViewMetadata.TYPE, new ViewMetadata(policies));
                return ClusterState.builder(currentState).putProjectMetadata(metadata).build();
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

    @Override
    protected boolean viewsFeatureEnabled() {
        return featureService.clusterHasFeature(clusterService.state(), EsqlFeatures.ESQL_VIEWS);
    }
}
