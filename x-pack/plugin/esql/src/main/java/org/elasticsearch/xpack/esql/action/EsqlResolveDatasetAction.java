/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasources.DatasetResolutionService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Resolves and authorizes {@code FROM <dataset>} targets. Mirror of {@link EsqlResolveViewAction}: the
 * {@code indices:data/read/...} action name plus an {@link IndicesRequest.Replaceable} request carrying
 * {@code resolveDatasets(true)} index options makes the security action filter authorize the dataset names
 * for {@code read} and replace the request's expressions with the authorized set — so the dataset name is
 * read-checked the same way an index pattern is, with no bespoke authorization predicate.
 */
public class EsqlResolveDatasetAction extends TransportLocalProjectMetadataAction<
    EsqlResolveDatasetAction.Request,
    EsqlResolveDatasetAction.Response> {
    public static final String NAME = "indices:data/read/esql/resolve_datasets";
    public static final ActionType<EsqlResolveDatasetAction.Response> TYPE = new ActionType<>(NAME);

    private final DatasetResolutionService datasetResolutionService;

    @Inject
    public EsqlResolveDatasetAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, actionFilters, transportService.getTaskManager(), clusterService, EsExecutors.DIRECT_EXECUTOR_SERVICE, projectResolver);
        this.datasetResolutionService = new DatasetResolutionService(indexNameExpressionResolver);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(Task task, Request request, ProjectState project, ActionListener<Response> listener) {
        var result = datasetResolutionService.resolveDatasets(
            project,
            request.indices(),
            request.indicesOptions(),
            request.getResolvedIndexExpressions()
        );
        listener.onResponse(new Response(result.datasetNames(), result.resolvedIndexExpressions()));
    }

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

        private static final IndicesOptions DATASET_INDICES_OPTIONS = IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(true))
            .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).build())
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .build();

        private static final IndicesOptions CPS_DATASET_INDICES_OPTIONS = IndicesOptions.builder(DATASET_INDICES_OPTIONS)
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();

        private final IndicesOptions indicesOptions;
        private String[] indices = new String[0];
        @Nullable
        private String projectRouting;
        @Nullable
        private TargetProjects resolvedTargetProjects;
        private ResolvedIndexExpressions resolvedIndexExpressions;

        public Request(TimeValue masterTimeout, boolean cpsEnabled) {
            super(masterTimeout);
            this.indicesOptions = cpsEnabled ? CPS_DATASET_INDICES_OPTIONS : DATASET_INDICES_OPTIONS;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        @Override
        public boolean allowsCrossProject() {
            return true;
        }

        @Override
        public boolean allowsRemoteIndices() {
            return true;
        }

        public void setProjectRouting(@Nullable String projectRouting) {
            this.projectRouting = projectRouting;
        }

        @Override
        public String getProjectRouting() {
            return projectRouting;
        }

        @Override
        public void setResolvedTargetProjects(TargetProjects resolvedTargetProjects) {
            this.resolvedTargetProjects = resolvedTargetProjects;
        }

        @Override
        public TargetProjects getResolvedTargetProjects() {
            return resolvedTargetProjects;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "EsqlResolveDatasetAction.Request={indices:" + Arrays.toString(indices) + "}";
        }

        @Override
        public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
            this.resolvedIndexExpressions = expressions;
        }

        @Override
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return this.resolvedIndexExpressions;
        }
    }

    public static class Response extends ActionResponse {
        private final List<String> datasetNames;
        private final ResolvedIndexExpressions resolvedIndexExpressions;

        public Response(List<String> datasetNames, ResolvedIndexExpressions resolvedIndexExpressions) {
            this.datasetNames = datasetNames;
            this.resolvedIndexExpressions = resolvedIndexExpressions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        public List<String> datasetNames() {
            return datasetNames;
        }

        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }
    }
}
