/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.view.ViewResolutionService;

import java.io.IOException;
import java.util.Arrays;

/**
 * Fetches view definitions from a remote cluster or project.
 * <p>
 * This action runs on the node that receives the request (the remote cluster coordinator) and
 * looks up views matching the requested index patterns in its local cluster state. Each view is
 * returned as a {@link View} carrying the view name and its query body, which the originating
 * coordinator uses to expand the view into a sub-plan.
 * <p>
 * The originating coordinator issues one request per remote cluster alias. The cluster alias is
 * derived from the qualified view names collected by
 * {@link org.elasticsearch.xpack.esql.session.EsqlCCSUtils#checkForRemoteResourceErrors} during
 * field-caps (e.g. {@code "remote1:my-view"} → send to {@code "remote1"} asking for
 * {@code "my-view"}).
 * <p>
 * For CCS the coordinator looks up the transport connection for each cluster alias via
 * {@link org.elasticsearch.transport.RemoteClusterService}. For CPS the same mechanism applies
 * because linked projects are registered as remote cluster aliases by the time this action runs.
 */
public class EsqlFetchRemoteViewsAction extends HandledTransportAction<
    EsqlFetchRemoteViewsAction.Request,
    EsqlFetchRemoteViewsAction.Response> {

    public static final String NAME = "indices:data/read/esql/fetch_remote_views";
    public static final ActionType<Response> TYPE = new ActionType<>(NAME);
    /**
     * Used by the coordinating node to send the request to a remote cluster over the existing
     * cross-cluster transport channel (same pattern as
     * {@link EsqlResolveFieldsAction#RESOLVE_REMOTE_TYPE}).
     */
    public static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(NAME, Response::new);

    private final ViewResolutionService viewResolutionService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;

    @Inject
    public EsqlFetchRemoteViewsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectResolver projectResolver
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.viewResolutionService = new ViewResolutionService(indexNameExpressionResolver);
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        var projectState = projectResolver.getProjectState(clusterService.state());
        var result = viewResolutionService.resolveViews(
            projectState,
            request.indices(),
            request.indicesOptions(),
            null  // resolve fresh — no pre-computed ResolvedIndexExpressions on the remote
        );
        listener.onResponse(new Response(result.views()));
    }

    /**
     * Request carrying the (unqualified) view name patterns to look up on the remote cluster.
     * The cluster alias is not included here; the coordinating node strips it before sending.
     */
    public static class Request extends ActionRequest implements IndicesRequest {

        /**
         * Same options used by {@link EsqlResolveViewAction.Request}: wildcard expansion enabled,
         * views resolved, missing targets allowed (lenient — the remote may not have all views).
         */
        private static final IndicesOptions VIEW_INDICES_OPTIONS = IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(true))
            .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveViews(true).build())
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .build();

        private final String[] indices;

        public Request(String[] indices) {
            this.indices = indices;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
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
            return "EsqlFetchRemoteViewsAction.Request{indices=" + Arrays.toString(indices) + "}";
        }
    }

    /**
     * Response carrying the view definitions found on the remote cluster that matched the
     * requested patterns. An empty array means no matching views exist on that cluster.
     */
    public static class Response extends ActionResponse {

        private final View[] views;

        public Response(View[] views) {
            this.views = views;
        }

        public Response(StreamInput in) throws IOException {
            this.views = in.readArray(View::new, View[]::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(views);
        }

        public View[] views() {
            return views;
        }
    }
}
