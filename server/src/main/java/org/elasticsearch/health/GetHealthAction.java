/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.health.components.controller.ClusterCoordination;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GetHealthAction extends ActionType<GetHealthAction.Response> {

    public static final GetHealthAction INSTANCE = new GetHealthAction();
    public static final String NAME = "cluster:monitor/health_api";

    private GetHealthAction() {
        super(NAME, GetHealthAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final ClusterName clusterName;
        private final HealthStatus status;
        private final List<HealthComponentResult> components;

        public Response(StreamInput in) {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        public Response(final ClusterName clusterName, final List<HealthComponentResult> components) {
            this.clusterName = clusterName;
            this.components = components;
            this.status = HealthStatus.merge(components.stream().map(HealthComponentResult::status));
        }

        public ClusterName getClusterName() {
            return clusterName;
        }

        public HealthStatus getStatus() {
            return status;
        }

        public List<HealthComponentResult> getComponents() {
            return components;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("status", status);
            builder.field("cluster_name", clusterName.value());
            builder.array("impacts");
            builder.startObject("components");
            for (HealthComponentResult component : components) {
                builder.field(component.name(), component, params);
            }
            builder.endObject();
            return builder.endObject();
        }
    }

    public static class Request extends ActionRequest {

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TransportAction extends org.elasticsearch.action.support.TransportAction<Request, Response> {

        private final ClusterService clusterService;

        @Inject
        public TransportAction(
            final ActionFilters actionFilters,
            final TransportService transportService,
            final ClusterService clusterService
        ) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final ClusterState clusterState = clusterService.state();
            final HealthComponentResult controller = ClusterCoordination.createClusterCoordinationComponent(
                clusterService.localNode(),
                clusterState
            );
            final HealthComponentResult snapshots = new HealthComponentResult("snapshots", HealthStatus.GREEN, Collections.emptyMap());
            final ClusterName clusterName = clusterService.getClusterName();
            listener.onResponse(new Response(clusterName, Arrays.asList(controller, snapshots)));
        }
    }
}
