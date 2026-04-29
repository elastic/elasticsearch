/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.crossproject.ProjectRoutingResolver;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authz.AuthorizedProjectsResolver;

import java.io.IOException;

public class EsqlResolveLocalViewAction extends TransportLocalProjectMetadataAction<
    EsqlResolveLocalViewAction.Request,
    EsqlResolveLocalViewAction.Response> {

    public static final String NAME = "indices:data/read/esql/resolve_local_views";
    public static final ActionType<EsqlResolveLocalViewAction.Response> TYPE = new ActionType<>(NAME);

    private final ProjectRoutingResolver projectRoutingResolver;
    private final AuthorizedProjectsResolver authorizedProjectsResolver;

    @Inject
    public EsqlResolveLocalViewAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        ProjectRoutingResolver projectRoutingResolver,
        AuthorizedProjectsResolver authorizedProjectsResolver
    ) {
        super(NAME, actionFilters, transportService.getTaskManager(), clusterService, EsExecutors.DIRECT_EXECUTOR_SERVICE, projectResolver);
        this.projectRoutingResolver = projectRoutingResolver;
        this.authorizedProjectsResolver = authorizedProjectsResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(Task task, Request request, ProjectState project, ActionListener<Response> listener) {
        authorizedProjectsResolver.resolveAuthorizedProjects(listener.delegateFailureAndWrap((l, targetProjects) -> {
            targetProjects = projectRoutingResolver.resolve(request.projectRouting(), project.metadata(), targetProjects);
            l.onResponse(new Response(targetProjects.originProject() != null));
        }));
    }

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

        private static final String[] INDICES = new String[0];

        private final String projectRouting;

        public Request(TimeValue masterTimeout, String projectRouting) {
            super(masterTimeout);
            this.projectRouting = projectRouting;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            return null;
        }

        @Override
        public String[] indices() {
            return INDICES;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.DEFAULT;
        }

        public String projectRouting() {
            return projectRouting;
        }

        @Override
        public String toString() {
            return "EsqlResolveLocalViewAction.Request={projectRouting:" + projectRouting + "}";
        }
    }

    public static class Response extends ActionResponse {
        private final boolean resolveLocalViews;

        public Response(boolean resolveLocalViews) {
            this.resolveLocalViews = resolveLocalViews;
        }

        public boolean resolveLocalViews() {
            return resolveLocalViews;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(resolveLocalViews);
        }
    }
}
