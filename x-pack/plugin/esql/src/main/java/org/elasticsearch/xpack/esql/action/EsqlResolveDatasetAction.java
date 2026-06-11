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
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Read-authorization gate for {@code FROM <dataset>}: narrows the concrete dataset names a query would read
 * to the subset the caller is authorized to read. Mirrors {@link EsqlResolveViewAction} for views — the
 * {@link Request} is an {@link IndicesRequest.Replaceable} with {@code resolveDatasets(true)}, so the security
 * filter checks each name against the caller's permissions (a read on the dataset name; unauthorized names are
 * filtered like unauthorized indices and views, hiding their existence) and the view/dataset DLS/FLS interceptor
 * rejects DLS/FLS-restricted datasets. The request also implements {@link DataSourceRequestInfo} so security's
 * dataset-datasource interceptor can additionally enforce {@code global.data_source: read} on the parent
 * datasource of every dataset that survives the filter — the same dual-axis model PUT dataset enforces on create.
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
    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable, DataSourceRequestInfo {

        private String[] indices;
        private final Map<String, String> datasetToDataSource;
        private ResolvedIndexExpressions resolvedIndexExpressions;

        /**
         * @param indices             the concrete dataset names the query would read if fully authorized
         * @param datasetToDataSource registered dataset name → parent datasource name, captured from the coordinator's
         *                            cluster state. {@link #dataSourceNames()} derives from it for whatever names are
         *                            in {@link #indices()} at evaluation time, so after the security filter replaces
         *                            the indices with the authorized resolution it reflects exactly the surviving datasets.
         */
        public Request(TimeValue masterTimeout, String[] indices, Map<String, String> datasetToDataSource) {
            super(masterTimeout);
            this.indices = indices;
            this.datasetToDataSource = Map.copyOf(datasetToDataSource);
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
        public String[] dataSourceNames() {
            // The constructor always sets indices and the security filter replaces with a non-null array.
            // Returning empty on null would silently skip the datasource check downstream — fail instead:
            // AssertionError under tests, NPE from the stream below in production. Never silently empty.
            assert indices != null;
            return Arrays.stream(indices).map(datasetToDataSource::get).filter(Objects::nonNull).distinct().toArray(String[]::new);
        }

        @Override
        public String dataSourceClusterActionName() {
            return EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME;
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
