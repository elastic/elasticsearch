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
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.view.ViewResolutionService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class EsqlResolveViewAction extends TransportLocalProjectMetadataAction<
    EsqlResolveViewAction.Request,
    EsqlResolveViewAction.Response> {
    public static final String NAME = "indices:data/read/esql/resolve_views";
    public static final ActionType<EsqlResolveViewAction.Response> TYPE = new ActionType<>(NAME);
    private static final Logger LOGGER = LogManager.getLogger(EsqlResolveViewAction.class);

    private final ViewResolutionService viewResolutionService;

    @Inject
    public EsqlResolveViewAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, actionFilters, transportService.getTaskManager(), clusterService, EsExecutors.DIRECT_EXECUTOR_SERVICE, projectResolver);
        this.viewResolutionService = new ViewResolutionService(indexNameExpressionResolver);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(Task task, Request request, ProjectState project, ActionListener<Response> listener) {
        var result = viewResolutionService.resolveViews(
            project,
            request.indices(),
            request.indicesOptions(),
            request.getResolvedIndexExpressions()
        );
        listener.onResponse(new EsqlResolveViewAction.Response(result.views(), result.resolvedIndexExpressions()));
    }

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

        private String[] indices = new String[0];
        private ResolvedIndexExpressions resolvedIndexExpressions;
        private static final IndicesOptions VIEW_INDICES_OPTIONS = IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().resolveViews(true).allowEmptyExpressions(true))
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .build();

        public Request(TimeValue masterTimeout) {
            super(masterTimeout);
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
            return VIEW_INDICES_OPTIONS;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "EsqlResolveViewAction.Request={indices:" + Arrays.toString(indices) + "}";
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
        private final View[] views;
        private final ResolvedIndexExpressions resolvedIndexExpressions;

        public Response(View[] views, ResolvedIndexExpressions resolvedIndexExpressions) {
            this.views = views;
            this.resolvedIndexExpressions = resolvedIndexExpressions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        public View[] views() {
            return views;
        }

        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }
    }
}
