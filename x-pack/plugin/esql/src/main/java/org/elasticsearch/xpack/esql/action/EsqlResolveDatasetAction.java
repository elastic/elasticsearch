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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * Read-authorization gate for {@code FROM <dataset>}: narrows the dataset names a query would read to the subset
 * the caller may read. Mirrors {@link EsqlResolveViewAction} — the {@link Request} is an
 * {@link IndicesRequest.Replaceable} with {@code resolveDatasets(true)}, so the security filter drops unauthorized
 * names (hiding their existence) and the DLS/FLS interceptor rejects restricted datasets. Read access is governed by
 * the index {@code read} privilege on the dataset name, exactly as for indices and views; the parent datasource's
 * credentials are an admin concern settled when the dataset is created (PUT), not re-checked per query.
 */
public class EsqlResolveDatasetAction extends TransportLocalProjectMetadataAction<
    EsqlResolveDatasetAction.Request,
    EsqlResolveDatasetAction.Response> {
    public static final String NAME = EsqlDatasetActionNames.ESQL_RESOLVE_DATASET_ACTION_NAME;
    public static final ActionType<EsqlResolveDatasetAction.Response> TYPE = new ActionType<>(NAME);

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public EsqlResolveDatasetAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(NAME, actionFilters, transportService.getTaskManager(), clusterService, EsExecutors.DIRECT_EXECUTOR_SERVICE, projectResolver);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(Task task, Request request, ProjectState project, ActionListener<Response> listener) {
        // On a security-enabled cluster the filter has already replaced the request indices with the resolved,
        // authorized names; re-resolving here is then a pass-through. Without security this is the sole resolution.
        var datasets = indexNameExpressionResolver.datasets(project.metadata(), request.indicesOptions(), request);
        listener.onResponse(new Response(Set.copyOf(datasets)));
    }

    /**
     * Unlike the view sibling, deliberately carries no remote/CPS plumbing ({@code allowsRemoteIndices},
     * project routing): datasets are local-only and remote-prefixed relations never reach this action.
     */
    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

        private String[] indices;
        private ResolvedIndexExpressions resolvedIndexExpressions;

        /** @param indices the concrete dataset names the query would read if fully authorized */
        public Request(TimeValue masterTimeout, String[] indices) {
            super(masterTimeout);
            this.indices = indices;
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
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return DatasetRewriter.RESOLVER_OPTIONS;
        }

        @Override
        public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
            this.resolvedIndexExpressions = expressions;
        }

        @Override
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "EsqlResolveDatasetAction.Request{indices:" + Arrays.toString(indices) + "}";
        }
    }

    public static class Response extends ActionResponse {
        private final Set<String> datasets;

        public Response(Set<String> datasets) {
            this.datasets = datasets;
        }

        /** Dataset names the caller is authorized to read, post pattern expansion. */
        public Set<String> datasets() {
            return datasets;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}
