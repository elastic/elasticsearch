/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SequentialAckingBatchedTaskExecutor;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of {@link ViewService} that keeps the views in the cluster state.
 */
public class ClusterViewService extends ViewService {
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final ProjectResolver projectResolver;
    private final MasterServiceTaskQueue<ViewMetadataUpdateTask> taskQueue;

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
        this.taskQueue = clusterService.createTaskQueue(
            "update-esql-view-metadata",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
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
        String verb,
        ProjectId projectId,
        AcknowledgedRequest<?> request,
        ActionListener<? extends AcknowledgedResponse> callback,
        Function<ViewMetadata, Map<String, View>> function
    ) {
        ViewMetadataUpdateTask updateTask = new ViewMetadataUpdateTask(request, callback, projectId, function);
        String taskName = String.format(Locale.ROOT, "update-esql-view-metadata-[%s]", verb.toLowerCase(Locale.ROOT));
        taskQueue.submitTask(taskName, updateTask, updateTask.timeout());
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
        return EsqlFeatures.ESQL_VIEWS_FEATURE_FLAG.isEnabled();
    }

    class ViewMetadataUpdateTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final Function<ViewMetadata, Map<String, View>> updateFunction;

        ViewMetadataUpdateTask(
            AcknowledgedRequest<?> request,
            ActionListener<? extends AcknowledgedResponse> listener,
            ProjectId projectId,
            Function<ViewMetadata, Map<String, View>> updateFunction
        ) {
            super(request, listener);
            this.projectId = projectId;
            this.updateFunction = updateFunction;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            var project = getProjectMetadata(projectId);
            var views = project.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
            Map<String, View> policies = updateFunction.apply(views);
            var metadata = ProjectMetadata.builder(project).putCustom(ViewMetadata.TYPE, new ViewMetadata(policies));
            return ClusterState.builder(currentState).putProjectMetadata(metadata).build();
        }
    }
}
