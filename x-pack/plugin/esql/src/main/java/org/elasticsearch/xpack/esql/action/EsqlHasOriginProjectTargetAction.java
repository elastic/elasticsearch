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
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
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
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class EsqlHasOriginProjectTargetAction extends TransportLocalProjectMetadataAction<
    EsqlHasOriginProjectTargetAction.Request,
    EsqlHasOriginProjectTargetAction.Response> {

    public static final String NAME = "indices:data/read/esql/has_origin_project_target";
    public static final ActionType<EsqlHasOriginProjectTargetAction.Response> TYPE = new ActionType<>(NAME);

    @Inject
    public EsqlHasOriginProjectTargetAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(NAME, actionFilters, transportService.getTaskManager(), clusterService, EsExecutors.DIRECT_EXECUTOR_SERVICE, projectResolver);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(Task task, Request request, ProjectState project, ActionListener<Response> listener) {
        if (request.getResolvedTargetProjects() == null) {
            assert false : "request.getResolvedTargetProjects() must be not null";
            listener.onResponse(new Response(true));
        } else {
            listener.onResponse(new Response(request.getResolvedTargetProjects().originProject() != null));
        }
    }

    public static class Request extends LocalClusterStateRequest implements CompositeIndicesRequest, IndicesRequest.CrossProjectCandidate {

        private final String projectRouting;
        private TargetProjects resolvedTargetProjects;

        public Request(TimeValue masterTimeout, String projectRouting) {
            super(masterTimeout);
            this.projectRouting = projectRouting;
        }

        @Override
        public boolean allowsCrossProject() {
            return true;
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
            TransportAction.localOnly();
        }
    }
}
