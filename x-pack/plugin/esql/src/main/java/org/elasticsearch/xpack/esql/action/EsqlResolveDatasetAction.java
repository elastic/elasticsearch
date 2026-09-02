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
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter.DatasetResolution;

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
 *
 * <p>The request carries one relation's <em>raw</em> FROM patterns (split on comma, not pre-expanded). Wildcard
 * expansion against the authorized abstractions happens in the authorization engine (the security filter replaces
 * {@link Request#indices()} in flight with the authorized concrete names) rather than client-side. The original raw
 * patterns are preserved separately ({@link Request#rawPatterns()}) so the action body can still classify whether the
 * relation also targets non-dataset abstractions — see {@link #localClusterStateOperation}.
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
        // Two-part engine-side resolution (see DatasetRewriter#resolve):
        // (a) the authorized concrete dataset names — from request.indices(), which on a security-enabled cluster the
        // filter has already narrowed (lenient options, so an unauthorized concrete name is dropped here, not 403'd);
        // without security this is the sole resolution.
        // (b) the concrete non-dataset names resolved from the same pattern (drives heterogeneous-FROM {@code UnionAll}), plus
        // the explicitly-named-but-unauthorized datasets — resolved from the ORIGINAL raw patterns under an open
        // predicate. The Unknown-index (400) for an explicit unauthorized dataset is surfaced downstream by
        // DatasetRewriter#rewriteOne.
        DatasetResolution resolution = DatasetRewriter.resolve(
            request.indices(),
            request.rawPatterns(),
            project.metadata(),
            indexNameExpressionResolver
        );
        listener.onResponse(
            new Response(resolution.resolvedExternalDatasets(), resolution.nonDatasetNames(), resolution.explicitUnauthorized())
        );
    }

    /**
     * Unlike the view sibling, deliberately carries no remote/CPS plumbing ({@code allowsRemoteIndices},
     * project routing): datasets are local-only and remote-prefixed relations never reach this action.
     */
    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

        private String[] indices;
        // The ORIGINAL raw FROM patterns for this relation, kept separate from the Replaceable indices: the security
        // filter narrows indices() in flight to the authorized concrete names, but the action body still needs the
        // un-narrowed patterns to classify whether the relation also targets non-dataset abstractions.
        private final String[] rawPatterns;
        private ResolvedIndexExpressions resolvedIndexExpressions;

        /** @param rawPatterns one relation's raw FROM patterns (split on comma, not pre-expanded) */
        public Request(TimeValue masterTimeout, String[] rawPatterns) {
            super(masterTimeout);
            this.indices = rawPatterns;
            this.rawPatterns = rawPatterns;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        /** The original raw FROM patterns, unaffected by the security filter's in-flight narrowing of {@link #indices()}. */
        public String[] rawPatterns() {
            return rawPatterns;
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
        private final Set<String> nonDatasetNames;
        private final Set<String> explicitUnauthorized;

        public Response(Set<String> datasets, Set<String> nonDatasetNames, Set<String> explicitUnauthorized) {
            this.datasets = datasets;
            this.nonDatasetNames = nonDatasetNames;
            this.explicitUnauthorized = explicitUnauthorized;
        }

        /** Dataset names the caller is authorized to read, post pattern expansion. */
        public Set<String> datasets() {
            return datasets;
        }

        /**
         * Concrete non-dataset names (indices, aliases, data streams) resolved from the same pattern. Non-empty when
         * the relation targets a mix of datasets and non-datasets; drives heterogeneous-FROM UnionAll building
         * in {@link DatasetRewriter}.
         */
        public Set<String> nonDatasetNames() {
            return nonDatasetNames;
        }

        /** Explicitly-named datasets absent from the authorized set — surfaced as {@code Unknown index} by the rewrite. */
        public Set<String> explicitUnauthorized() {
            return explicitUnauthorized;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}
